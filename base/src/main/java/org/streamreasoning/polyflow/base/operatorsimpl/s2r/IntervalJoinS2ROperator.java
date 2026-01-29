package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.apache.log4j.Logger;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.WindowImpl;
import org.streamreasoning.polyflow.api.sds.timevarying.TimeVarying;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeInstant;
import org.streamreasoning.polyflow.base.sds.TimeVaryingObject;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Interval Join S2R Operator
 * <p>
 * Implements interval joins between two streams. For each event from Stream A,
 * this operator creates an interval window [ts + lowerBound, ts + upperBound]
 * and matches with events from Stream B that fall within that interval.
 * <p>
 * The interval bounds are relative to the event's timestamp:
 * - lowerBound: typically negative (e.g., -5000 means 5 seconds before)
 * - upperBound: typically positive (e.g., +5000 means 5 seconds after)
 * <p>
 * Example: For an order at t=100, with lowerBound=-10 and upperBound=+50,
 * matching shipments must have timestamps in [90, 150].
 *
 * @param <I> Input element type (must contain timestamp information)
 * @param <W> Window element type
 * @param <R> Relation type (the coalesced result type)
 */
public class IntervalJoinS2ROperator<I, W, R extends Iterable<?>> implements StreamToRelationOperator<I, W, R> {

    private static final Logger log = Logger.getLogger(IntervalJoinS2ROperator.class);

    private final Time time;
    private final String name;
    private final ContentFactory<I, W, R> cf;
    private final Report report;
    private final Tick tick;

    // Interval bounds (relative to event timestamp)
    private final long lowerBound;
    private final long upperBound;

    // Buffer for events from "this" stream (the probe stream)
    private final TreeMap<Long, List<TimestampedElement<I>>> probeBuffer = new TreeMap<>();

    // Shared buffer for events from the "other" stream (the build stream)
    private final TreeMap<Long, List<TimestampedElement<I>>> buildBuffer;

    // Function to extract timestamp from element
    private final Function<I, Long> timestampExtractor;

    // Optional join predicate (beyond temporal condition)
    private final BiPredicate<I, I> joinPredicate;

    // Factory to create joined result elements
    private final JoinResultFactory<I> joinResultFactory;

    // Active interval windows with their content
    private final Map<Window, Content<I, W, R>> activeWindows = new LinkedHashMap<>();

    // Windows that have been reported
    private final List<Window> reportedWindows = new ArrayList<>();

    // State retention time (how long to keep events in buffer for late arrivals)
    private final long stateRetentionTime;

    /**
     * Wrapper class to store elements with their timestamps
     */
    public static class TimestampedElement<T> {
        public final T element;
        public final long timestamp;

        public TimestampedElement(T element, long timestamp) {
            this.element = element;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "(" + element + "@" + timestamp + ")";
        }
    }

    /**
     * Factory interface to create joined result elements
     */
    public interface JoinResultFactory<I> {
        /**
         * Create a joined element from probe and build elements
         */
        I createJoinedElement(I probeElement, long probeTs, I buildElement, long buildTs);
    }

    /**
     * Creates an IntervalJoinS2ROperator
     *
     * @param tick               The tick type (TIME_DRIVEN, TUPLE_DRIVEN, etc.)
     * @param time               The shared Time object
     * @param name               Name of this operator
     * @param cf                 Content factory for creating window content
     * @param report             Report strategy
     * @param lowerBound         Lower bound of interval (relative to event timestamp, typically negative)
     * @param upperBound         Upper bound of interval (relative to event timestamp, typically positive)
     * @param buildBuffer        Shared buffer containing events from the other stream
     * @param timestampExtractor Function to extract timestamp from elements
     * @param joinPredicate      Optional additional predicate for joining (e.g., key equality)
     * @param joinResultFactory  Factory to create joined result elements
     * @param stateRetentionTime How long to retain state for late events
     */
    public IntervalJoinS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long lowerBound,
            long upperBound,
            TreeMap<Long, List<TimestampedElement<I>>> buildBuffer,
            Function<I, Long> timestampExtractor,
            BiPredicate<I, I> joinPredicate,
            JoinResultFactory<I> joinResultFactory,
            long stateRetentionTime) {

        this.tick = tick;
        this.time = time;
        this.name = name;
        this.cf = cf;
        this.report = report;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.buildBuffer = buildBuffer;
        this.timestampExtractor = timestampExtractor;
        this.joinPredicate = joinPredicate;
        this.joinResultFactory = joinResultFactory;
        this.stateRetentionTime = stateRetentionTime;
    }

    /**
     * Simplified constructor with default join result factory that returns probe element
     */
    public IntervalJoinS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long lowerBound,
            long upperBound,
            TreeMap<Long, List<TimestampedElement<I>>> buildBuffer,
            Function<I, Long> timestampExtractor,
            long stateRetentionTime) {

        this(tick, time, name, cf, report, lowerBound, upperBound, buildBuffer,
                timestampExtractor, null, null, stateRetentionTime);
    }

    @Override
    public void compute(I element, long ts) {
        log.debug("Received element (" + element + "," + ts + ") on operator " + name);

        // 1. Add element to probe buffer
        probeBuffer.computeIfAbsent(ts, k -> new ArrayList<>())
                .add(new TimestampedElement<>(element, ts));

        // 2. Calculate interval bounds for this event
        long intervalStart = ts + lowerBound;
        long intervalEnd = ts + upperBound;

        log.debug("Creating interval window [" + intervalStart + ", " + intervalEnd + "] for event at " + ts);

        // 3. Create interval window
        Window intervalWindow = new WindowImpl(intervalStart, intervalEnd);
        Content<I, W, R> content = activeWindows.computeIfAbsent(intervalWindow, w -> cf.create());

        // 4. Add the probe element itself to the content
        content.add(element);

        // 5. Find and add matching elements from build buffer within the interval
        if (buildBuffer != null && !buildBuffer.isEmpty()) {
            NavigableMap<Long, List<TimestampedElement<I>>> matchingEvents =
                    buildBuffer.subMap(intervalStart, true, intervalEnd, true);

            for (Map.Entry<Long, List<TimestampedElement<I>>> entry : matchingEvents.entrySet()) {
                for (TimestampedElement<I> buildElement : entry.getValue()) {
                    // Apply additional join predicate if specified
                    if (joinPredicate == null || joinPredicate.test(element, buildElement.element)) {
                        // Create joined result if factory is provided
                        if (joinResultFactory != null) {
                            I joinedElement = joinResultFactory.createJoinedElement(
                                    element, ts, buildElement.element, buildElement.timestamp);
                            content.add(joinedElement);
                        } else {
                            // Default: add the build element directly
                            content.add(buildElement.element);
                        }
                        log.debug("Matched: " + element + "@" + ts + " with " + buildElement);
                    }
                }
            }
        }

        // 6. Check if we should report
        if (report.report(intervalWindow, content, ts, System.currentTimeMillis())) {
            reportedWindows.add(intervalWindow);
            time.addEvaluationTimeInstants(new TimeInstant(ts));
            log.debug("Window reported: " + intervalWindow);
        }

        // 7. Update application time
        time.setAppTime(ts);

        // 8. Evict old state
        evictOldState(ts);
    }

    /**
     * Add an element to the build buffer (called by the other stream's operator)
     */
    public void addToBuildBuffer(I element, long ts) {
        buildBuffer.computeIfAbsent(ts, k -> new ArrayList<>())
                .add(new TimestampedElement<>(element, ts));
    }

    /**
     * Get the probe buffer (for sharing with another IntervalJoinS2ROperator)
     */
    public TreeMap<Long, List<TimestampedElement<I>>> getProbeBuffer() {
        return probeBuffer;
    }

    private void evictOldState(long currentTime) {
        long evictionThreshold = currentTime - stateRetentionTime;

        // Remove old entries from probe buffer
        probeBuffer.headMap(evictionThreshold, true).clear();

        // Note: build buffer is shared and managed externally or by the paired operator

        // Remove expired windows (windows whose end time has passed the retention period)
        activeWindows.entrySet().removeIf(entry ->
                entry.getKey().getC() < evictionThreshold);

        // Clear reported windows
        reportedWindows.removeIf(w -> w.getC() < evictionThreshold);
    }

    @Override
    public Content<I, W, R> content(long t_e) {
        // If some windows were reported, return the most recent one
        if (!reportedWindows.isEmpty()) {
            return reportedWindows.stream()
                    .max(Comparator.comparingLong(Window::getC))
                    .map(activeWindows::get)
                    .orElse(cf.createEmpty());
        }

        // Otherwise, find windows that contain the given timestamp
        Optional<Window> matchingWindow = activeWindows.keySet().stream()
                .filter(w -> w.getO() <= t_e && t_e <= w.getC())
                .max(Comparator.comparingLong(Window::getC));

        return matchingWindow.map(activeWindows::get).orElse(cf.createEmpty());
    }

    @Override
    public List<Content<I, W, R>> getContents(long t_e) {
        if (!reportedWindows.isEmpty()) {
            return reportedWindows.stream()
                    .map(activeWindows::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        return activeWindows.entrySet().stream()
                .filter(e -> e.getKey().getO() <= t_e && t_e <= e.getKey().getC())
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public TimeVarying<R> get() {
        return new TimeVaryingObject<>(this, name);
    }

    @Override
    public Report report() {
        return report;
    }

    @Override
    public Tick tick() {
        return tick;
    }

    @Override
    public Time time() {
        return time;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void evict() {
        // Clear reported windows after processing
        reportedWindows.clear();
    }

    @Override
    public void evict(long ts) {
        evictOldState(ts);
        reportedWindows.clear();
    }

    /**
     * Get the number of active windows
     */
    public int getActiveWindowCount() {
        return activeWindows.size();
    }

    /**
     * Get the number of elements in the probe buffer
     */
    public int getProbeBufferSize() {
        return probeBuffer.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Get the number of elements in the build buffer
     */
    public int getBuildBufferSize() {
        if (buildBuffer == null) return 0;
        return buildBuffer.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Get all active windows for debugging/testing
     */
    public Set<Window> getActiveWindows() {
        return Collections.unmodifiableSet(activeWindows.keySet());
    }

    @Override
    public String toString() {
        return "IntervalJoinS2ROperator{" +
                "name='" + name + '\'' +
                ", lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                ", activeWindows=" + activeWindows.size() +
                ", probeBufferSize=" + getProbeBufferSize() +
                ", buildBufferSize=" + getBuildBufferSize() +
                '}';
    }
}
