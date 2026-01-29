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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Keyed Interval Join S2R Operator
 * <p>
 * Performs interval joins with efficient key-based lookups, similar to SQL:
 * <pre>
 * SELECT * FROM orders o 
 * JOIN shipments s ON o.order_id = s.order_id 
 * AND s.timestamp BETWEEN o.timestamp + lowerBound AND o.timestamp + upperBound
 * </pre>
 * <p>
 * Key Features:
 * - Primary key / Foreign key matching (like SQL joins)
 * - Time-interval based correlation
 * - Efficient O(log n) lookups using key-indexed TreeMaps
 * - Configurable state retention for memory management
 * <p>
 * Example: Match orders with shipments where:
 * - shipment.orderId = order.orderId (key match)
 * - shipment.timestamp is within [order.timestamp - 10min, order.timestamp + 1hour]
 *
 * @param <I> Input element type
 * @param <K> Key type for joining (e.g., String for orderId)
 * @param <W> Window element type
 * @param <R> Relation type (the coalesced result type)
 */
public class KeyedIntervalJoinS2ROperator<I, K, W, R extends Iterable<?>> 
        implements StreamToRelationOperator<I, W, R> {

    private static final Logger log = Logger.getLogger(KeyedIntervalJoinS2ROperator.class);

    private final Tick tick;
    private final Time time;
    private final String name;
    private final ContentFactory<I, W, R> cf;
    private final Report report;

    // Interval bounds (relative to probe event's timestamp)
    private final long lowerBound;
    private final long upperBound;

    // Key extractor: extracts the join key from an element
    private final Function<I, K> keyExtractor;

    // Timestamp extractor: extracts timestamp from an element
    private final Function<I, Long> timestampExtractor;

    // Build buffer indexed by KEY, then by TIMESTAMP
    // Structure: Key -> (Timestamp -> List<Elements>)
    // This allows efficient lookup: first find by key, then by time range
    private final Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buildBufferByKey;

    // Probe buffer for this stream's events (also keyed for bidirectional joins)
    private final Map<K, TreeMap<Long, List<TimestampedElement<I>>>> probeBufferByKey = new HashMap<>();

    // Active windows with their content
    private final Map<Window, Content<I, W, R>> activeWindows = new LinkedHashMap<>();

    // Windows that have been reported
    private final List<Window> reportedWindows = new ArrayList<>();

    // State retention time
    private final long stateRetentionTime;

    // Optional: Factory to create joined result elements
    private final JoinResultFactory<I, K> joinResultFactory;

    /**
     * Wrapper class to store elements with their timestamps and keys
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
            return "TS{" + element + "@" + timestamp + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimestampedElement<?> that = (TimestampedElement<?>) o;
            return timestamp == that.timestamp && Objects.equals(element, that.element);
        }

        @Override
        public int hashCode() {
            return Objects.hash(element, timestamp);
        }
    }

    /**
     * Factory interface to create joined result elements
     */
    @FunctionalInterface
    public interface JoinResultFactory<I, K> {
        /**
         * Create a joined element from probe and build elements
         *
         * @param probeElement The element from the probe stream
         * @param probeKey     The key of the probe element
         * @param probeTs      The timestamp of the probe element
         * @param buildElement The matching element from the build stream
         * @param buildTs      The timestamp of the build element
         * @return A new element representing the joined result
         */
        I createJoinedElement(I probeElement, K probeKey, long probeTs, 
                              I buildElement, long buildTs);
    }

    /**
     * Full constructor with all options
     *
     * @param tick               The tick type (TIME_DRIVEN, TUPLE_DRIVEN, etc.)
     * @param time               The shared Time object
     * @param name               Name of this operator
     * @param cf                 Content factory for creating window content
     * @param report             Report strategy
     * @param lowerBound         Lower bound of interval (relative to event timestamp, typically negative)
     * @param upperBound         Upper bound of interval (relative to event timestamp, typically positive)
     * @param buildBufferByKey   Shared buffer containing events from the other stream, indexed by key
     * @param keyExtractor       Function to extract the join key from elements
     * @param timestampExtractor Function to extract timestamp from elements
     * @param joinResultFactory  Optional factory to create joined result elements (can be null)
     * @param stateRetentionTime How long to retain state for late events
     */
    public KeyedIntervalJoinS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long lowerBound,
            long upperBound,
            Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buildBufferByKey,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            JoinResultFactory<I, K> joinResultFactory,
            long stateRetentionTime) {

        this.tick = tick;
        this.time = time;
        this.name = name;
        this.cf = cf;
        this.report = report;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.buildBufferByKey = buildBufferByKey;
        this.keyExtractor = keyExtractor;
        this.timestampExtractor = timestampExtractor;
        this.joinResultFactory = joinResultFactory;
        this.stateRetentionTime = stateRetentionTime;
    }

    /**
     * Simplified constructor without join result factory
     */
    public KeyedIntervalJoinS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long lowerBound,
            long upperBound,
            Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buildBufferByKey,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            long stateRetentionTime) {

        this(tick, time, name, cf, report, lowerBound, upperBound, buildBufferByKey,
                keyExtractor, timestampExtractor, null, stateRetentionTime);
    }

    @Override
    public void compute(I element, long ts) {
        // Step 1: Extract the join key from the probe element
        K key = keyExtractor.apply(element);
        
        log.debug("Received element with key=" + key + " at ts=" + ts);

        // Step 2: Add element to probe buffer (keyed)
        probeBufferByKey
                .computeIfAbsent(key, k -> new TreeMap<>())
                .computeIfAbsent(ts, t -> new ArrayList<>())
                .add(new TimestampedElement<>(element, ts));

        // Step 3: Calculate interval window boundaries
        long windowStart = ts + lowerBound;
        long windowEnd = ts + upperBound;
        Window intervalWindow = new WindowImpl(windowStart, windowEnd);

        log.debug("Created interval window [" + windowStart + ", " + windowEnd + "] for key=" + key);

        // Step 4: Create content for this window
        Content<I, W, R> content = activeWindows.computeIfAbsent(intervalWindow, w -> cf.create());

        // Step 5: Add the probe element itself to the content
        content.add(element);

        // Step 6: KEY-BASED LOOKUP - Only look at events with the SAME KEY
        TreeMap<Long, List<TimestampedElement<I>>> eventsForKey = buildBufferByKey.get(key);

        if (eventsForKey != null && !eventsForKey.isEmpty()) {
            // Get events within the time interval for this specific key
            NavigableMap<Long, List<TimestampedElement<I>>> matchingEvents =
                    eventsForKey.subMap(windowStart, true, windowEnd, true);

            int matchCount = 0;
            for (Map.Entry<Long, List<TimestampedElement<I>>> entry : matchingEvents.entrySet()) {
                for (TimestampedElement<I> candidate : entry.getValue()) {
                    // Create joined result if factory is provided
                    if (joinResultFactory != null) {
                        I joinedElement = joinResultFactory.createJoinedElement(
                                element, key, ts, candidate.element, candidate.timestamp);
                        content.add(joinedElement);
                    } else {
                        // Default: add the build element directly
                        content.add(candidate.element);
                    }
                    matchCount++;
                    log.debug("Matched: " + element + " with " + candidate.element + " (key=" + key + ")");
                }
            }
            log.debug("Found " + matchCount + " matches for key=" + key);
        } else {
            log.debug("No events found for key=" + key + " in build buffer");
        }

        // Step 7: Check if we should report
        if (report.report(intervalWindow, content, ts, System.currentTimeMillis())) {
            reportedWindows.add(intervalWindow);
            time.addEvaluationTimeInstants(new TimeInstant(ts));
        }

        // Step 8: Update application time
        time.setAppTime(ts);

        // Step 9: Evict old state
        evictOldState(ts);
    }

    /**
     * Add an element to the build buffer (called from outside to populate the other stream's data)
     *
     * @param buffer    The build buffer to add to
     * @param element   The element to add
     * @param key       The join key of the element
     * @param timestamp The timestamp of the element
     */
    public static <I, K> void addToBuildBuffer(
            Map<K, TreeMap<Long, List<TimestampedElement<I>>>> buffer,
            I element,
            K key,
            long timestamp) {

        buffer.computeIfAbsent(key, k -> new TreeMap<>())
                .computeIfAbsent(timestamp, t -> new ArrayList<>())
                .add(new TimestampedElement<>(element, timestamp));
    }

    /**
     * Get the probe buffer (for sharing with another KeyedIntervalJoinS2ROperator for bidirectional joins)
     */
    public Map<K, TreeMap<Long, List<TimestampedElement<I>>>> getProbeBufferByKey() {
        return probeBufferByKey;
    }

    private void evictOldState(long currentTime) {
        long evictionThreshold = currentTime - stateRetentionTime;

        // Evict from probe buffer
        for (TreeMap<Long, List<TimestampedElement<I>>> keyBuffer : probeBufferByKey.values()) {
            keyBuffer.headMap(evictionThreshold, true).clear();
        }
        // Remove empty key entries from probe buffer
        probeBufferByKey.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        // Evict from build buffer (shared, so be careful)
        for (TreeMap<Long, List<TimestampedElement<I>>> keyBuffer : buildBufferByKey.values()) {
            keyBuffer.headMap(evictionThreshold, true).clear();
        }
        // Remove empty key entries from build buffer
        buildBufferByKey.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        // Evict old windows
        activeWindows.entrySet().removeIf(
                entry -> entry.getKey().getC() < evictionThreshold
        );

        // Clear old reported windows
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
        reportedWindows.clear();
    }

    @Override
    public void evict(long ts) {
        evictOldState(ts);
        reportedWindows.clear();
    }

    // ========== Utility methods for testing/debugging ==========

    /**
     * Get the number of active windows
     */
    public int getActiveWindowCount() {
        return activeWindows.size();
    }

    /**
     * Get the number of distinct keys in the probe buffer
     */
    public int getProbeKeyCount() {
        return probeBufferByKey.size();
    }

    /**
     * Get the total number of elements in the probe buffer
     */
    public int getProbeBufferSize() {
        return probeBufferByKey.values().stream()
                .flatMap(tm -> tm.values().stream())
                .mapToInt(List::size)
                .sum();
    }

    /**
     * Get the number of distinct keys in the build buffer
     */
    public int getBuildKeyCount() {
        return buildBufferByKey.size();
    }

    /**
     * Get the total number of elements in the build buffer
     */
    public int getBuildBufferSize() {
        return buildBufferByKey.values().stream()
                .flatMap(tm -> tm.values().stream())
                .mapToInt(List::size)
                .sum();
    }

    /**
     * Get all active windows for debugging
     */
    public Set<Window> getActiveWindows() {
        return Collections.unmodifiableSet(activeWindows.keySet());
    }

    /**
     * Get all keys currently in the build buffer
     */
    public Set<K> getBuildBufferKeys() {
        return Collections.unmodifiableSet(buildBufferByKey.keySet());
    }

    /**
     * Get all keys currently in the probe buffer
     */
    public Set<K> getProbeBufferKeys() {
        return Collections.unmodifiableSet(probeBufferByKey.keySet());
    }

    @Override
    public String toString() {
        return "KeyedIntervalJoinS2ROperator{" +
                "name='" + name + '\'' +
                ", lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                ", activeWindows=" + activeWindows.size() +
                ", probeKeys=" + probeBufferByKey.size() +
                ", buildKeys=" + buildBufferByKey.size() +
                '}';
    }
}
