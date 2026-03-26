package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.apache.log4j.Logger;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin.*;

import java.util.*;
import java.util.function.Function;

/**
 * Sliding-Window Join Operator for two keyed streams.
 *
 * <h3>Semantics</h3>
 * Events from both streams are assigned to <b>time-based sliding windows</b>
 * defined by a {@code windowSize} and {@code slideSize}.  A window
 * {@code [wStart, wStart + windowSize)} is created for every multiple of
 * {@code slideSize}.  When the watermark passes the window end, the window
 * is evaluated: for every key, all left elements × all right elements in
 * that window are joined via the user-supplied {@link ProcessJoinFunction}.
 *
 * <p>If {@code slideSize == windowSize} the operator behaves as a
 * <b>tumbling-window</b> join.  If {@code slideSize < windowSize} each event
 * may belong to multiple overlapping windows (classic sliding window).
 *
 * <h3>Output timestamp</h3>
 * Each joined pair gets timestamp {@code max(tL, tR)} (same as interval join).
 *
 * <h3>Late data</h3>
 * Events whose timestamp is strictly below the current watermark minus an
 * optional {@code allowedLateness} are emitted to a configurable side output.
 *
 * <h3>API</h3>
 * Follows the same {@code processLeft / processRight / drainOutputs /
 * advanceWatermark} contract as {@link FlinkIntervalJoinOperator} so the
 * benchmark can drive it uniformly.
 *
 * @param <K>   key type
 * @param <L>   left stream element type
 * @param <R>   right stream element type
 * @param <OUT> output element type
 */
public class SlidingWindowJoinOperator<K, L, R, OUT> {

    private static final Logger log = Logger.getLogger(SlidingWindowJoinOperator.class);

    // ────────── configuration ──────────
    private final long windowSize;
    private final long slideSize;
    private final long allowedLateness;

    private final Function<L, K> leftKeyExtractor;
    private final Function<R, K> rightKeyExtractor;
    private final ProcessJoinFunction<L, R, OUT> joinFunction;

    private final String leftLateDataOutputTag;
    private final String rightLateDataOutputTag;

    // ────────── window state ──────────
    // windowStart → per-key left / right buffers
    private final TreeMap<Long, Map<K, List<TimestampedElement<L>>>> leftWindows = new TreeMap<>();
    private final TreeMap<Long, Map<K, List<TimestampedElement<R>>>> rightWindows = new TreeMap<>();

    // ────────── watermark ──────────
    private long currentWatermark = Long.MIN_VALUE;

    // ────────── output accumulation ──────────
    private final List<TimestampedOutput<OUT>> outputRecords = new ArrayList<>();
    private final List<SideOutputRecord<?>> sideOutputRecords = new ArrayList<>();

    // ────────── statistics ──────────
    private long windowsFired = 0;

    // ═══════════════════════════════════════════════════════
    //  NESTED TYPES
    // ═══════════════════════════════════════════════════════

    /** A timestamped element inside a window buffer. */
    private static final class TimestampedElement<T> {
        final T element;
        final long timestamp;

        TimestampedElement(T element, long timestamp) {
            this.element = element;
            this.timestamp = timestamp;
        }
    }

    /** A main output record together with its event-time. */
    public static final class TimestampedOutput<T> {
        private final T record;
        private final long timestamp;

        public TimestampedOutput(T record, long timestamp) {
            this.record = record;
            this.timestamp = timestamp;
        }

        public T getRecord()       { return record; }
        public long getTimestamp()  { return timestamp; }

        @Override public String toString() {
            return "Output{" + record + " @" + timestamp + '}';
        }
    }

    /** A side-output record (late data). */
    public static final class SideOutputRecord<T> {
        private final String tag;
        private final T value;

        public SideOutputRecord(String tag, T value) {
            this.tag = tag;
            this.value = value;
        }

        public String getTag()  { return tag; }
        public T getValue()     { return value; }

        @Override public String toString() {
            return "SideOutput{tag=" + tag + ", val=" + value + '}';
        }
    }

    // ═══════════════════════════════════════════════════════
    //  BUILDER
    // ═══════════════════════════════════════════════════════

    public static <K, L, R, OUT> Builder<K, L, R, OUT> builder() {
        return new Builder<>();
    }

    public static final class Builder<K, L, R, OUT> {
        private long windowSize;
        private long slideSize = -1;  // default: tumbling (= windowSize)
        private long allowedLateness = 0;
        private String leftLateTag  = "left-late";
        private String rightLateTag = "right-late";
        private Function<L, K> leftKeyExtractor;
        private Function<R, K> rightKeyExtractor;
        private ProcessJoinFunction<L, R, OUT> joinFunction;

        /** Window size (duration of each window). */
        public Builder<K, L, R, OUT> windowSize(long v) { this.windowSize = v; return this; }

        /** Slide interval.  Defaults to windowSize (tumbling) if not set. */
        public Builder<K, L, R, OUT> slideSize(long v)   { this.slideSize = v; return this; }

        /** Maximum allowed lateness before events are side-outputted. */
        public Builder<K, L, R, OUT> allowedLateness(long v) { this.allowedLateness = v; return this; }

        public Builder<K, L, R, OUT> leftLateDataOutputTag(String t)  { this.leftLateTag = t; return this; }
        public Builder<K, L, R, OUT> rightLateDataOutputTag(String t) { this.rightLateTag = t; return this; }
        public Builder<K, L, R, OUT> leftKeyExtractor(Function<L, K> fn)  { this.leftKeyExtractor = fn; return this; }
        public Builder<K, L, R, OUT> rightKeyExtractor(Function<R, K> fn) { this.rightKeyExtractor = fn; return this; }
        public Builder<K, L, R, OUT> processJoinFunction(ProcessJoinFunction<L, R, OUT> fn) { this.joinFunction = fn; return this; }

        public SlidingWindowJoinOperator<K, L, R, OUT> build() {
            Objects.requireNonNull(leftKeyExtractor,  "leftKeyExtractor");
            Objects.requireNonNull(rightKeyExtractor, "rightKeyExtractor");
            Objects.requireNonNull(joinFunction,      "processJoinFunction");
            if (windowSize <= 0) throw new IllegalArgumentException("windowSize must be > 0, got " + windowSize);
            if (slideSize < 0)   slideSize = windowSize; // default to tumbling
            if (slideSize <= 0)  throw new IllegalArgumentException("slideSize must be > 0, got " + slideSize);

            return new SlidingWindowJoinOperator<>(
                    windowSize, slideSize, allowedLateness,
                    leftLateTag, rightLateTag,
                    leftKeyExtractor, rightKeyExtractor, joinFunction);
        }
    }

    // ────────── constructor ──────────

    private SlidingWindowJoinOperator(
            long windowSize, long slideSize, long allowedLateness,
            String leftLateDataOutputTag, String rightLateDataOutputTag,
            Function<L, K> leftKeyExtractor, Function<R, K> rightKeyExtractor,
            ProcessJoinFunction<L, R, OUT> joinFunction) {
        this.windowSize = windowSize;
        this.slideSize  = slideSize;
        this.allowedLateness = allowedLateness;
        this.leftLateDataOutputTag  = leftLateDataOutputTag;
        this.rightLateDataOutputTag = rightLateDataOutputTag;
        this.leftKeyExtractor  = leftKeyExtractor;
        this.rightKeyExtractor = rightKeyExtractor;
        this.joinFunction = joinFunction;
    }

    // ═══════════════════════════════════════════════════════
    //  WINDOW ASSIGNMENT
    // ═══════════════════════════════════════════════════════

    /**
     * Returns the set of window-start times that a given event-time belongs to.
     * For a sliding window with size W and slide S, an event at time t belongs to
     * all windows [wStart, wStart + W) where:
     *   wStart = floor((t - W + S) / S) * S  ...  floor(t / S) * S
     * Each wStart must satisfy wStart <= t < wStart + W.
     */
    private List<Long> windowsFor(long timestamp) {
        List<Long> windows = new ArrayList<>();
        // Earliest possible window start that could contain this timestamp
        long firstStart = timestamp - windowSize + slideSize;
        // Align to slide boundary
        firstStart = (firstStart >= 0)
                ? (firstStart / slideSize) * slideSize
                : ((firstStart - slideSize + 1) / slideSize) * slideSize;

        for (long wStart = firstStart; wStart <= timestamp; wStart += slideSize) {
            if (wStart + windowSize > timestamp) {
                windows.add(wStart);
            }
        }
        return windows;
    }

    /**
     * Returns the window-end time for a window starting at wStart.
     */
    private long windowEnd(long wStart) {
        return wStart + windowSize;
    }

    // ═══════════════════════════════════════════════════════
    //  PROCESS LEFT
    // ═══════════════════════════════════════════════════════

    public void processLeft(L element, long tL) {
        if (tL == Long.MIN_VALUE) {
            throw new IllegalArgumentException("Timestamp must not be Long.MIN_VALUE.");
        }

        K key = leftKeyExtractor.apply(element);

        // Late-data check
        if (tL < currentWatermark - allowedLateness) {
            sideOutputRecords.add(new SideOutputRecord<>(leftLateDataOutputTag, element));
            return;
        }

        // Assign to all relevant windows
        for (long wStart : windowsFor(tL)) {
            // Don't add to windows that have already been fired
            if (windowEnd(wStart) <= currentWatermark) continue;

            leftWindows.computeIfAbsent(wStart, k -> new HashMap<>())
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(new TimestampedElement<>(element, tL));
        }
    }

    // ═══════════════════════════════════════════════════════
    //  PROCESS RIGHT
    // ═══════════════════════════════════════════════════════

    public void processRight(R element, long tR) {
        if (tR == Long.MIN_VALUE) {
            throw new IllegalArgumentException("Timestamp must not be Long.MIN_VALUE.");
        }

        K key = rightKeyExtractor.apply(element);

        // Late-data check
        if (tR < currentWatermark - allowedLateness) {
            sideOutputRecords.add(new SideOutputRecord<>(rightLateDataOutputTag, element));
            return;
        }

        // Assign to all relevant windows
        for (long wStart : windowsFor(tR)) {
            if (windowEnd(wStart) <= currentWatermark) continue;

            rightWindows.computeIfAbsent(wStart, k -> new HashMap<>())
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(new TimestampedElement<>(element, tR));
        }
    }

    // ═══════════════════════════════════════════════════════
    //  WATERMARK + WINDOW FIRING
    // ═══════════════════════════════════════════════════════

    /**
     * Advance the watermark.  All windows whose end ≤ newWatermark are evaluated
     * (cross-join per key) and then evicted.
     */
    public void advanceWatermark(long newWatermark) {
        if (newWatermark <= currentWatermark) return;
        currentWatermark = newWatermark;

        // Fire all windows whose end time ≤ watermark
        // Collect window starts to fire
        List<Long> toFire = new ArrayList<>();
        for (Long wStart : leftWindows.keySet()) {
            if (windowEnd(wStart) <= currentWatermark) toFire.add(wStart);
        }
        for (Long wStart : rightWindows.keySet()) {
            if (windowEnd(wStart) <= currentWatermark && !toFire.contains(wStart)) {
                toFire.add(wStart);
            }
        }
        Collections.sort(toFire);

        for (long wStart : toFire) {
            fireWindow(wStart);
        }
    }

    /**
     * Evaluate a window: for every key present on both sides, emit the
     * cross product of left × right elements.
     */
    private void fireWindow(long wStart) {
        Map<K, List<TimestampedElement<L>>> leftByKey =
                leftWindows.getOrDefault(wStart, Collections.emptyMap());
        Map<K, List<TimestampedElement<R>>> rightByKey =
                rightWindows.getOrDefault(wStart, Collections.emptyMap());

        // Join on keys present in both sides
        for (Map.Entry<K, List<TimestampedElement<L>>> leftEntry : leftByKey.entrySet()) {
            K key = leftEntry.getKey();
            List<TimestampedElement<R>> rightList = rightByKey.get(key);
            if (rightList == null || rightList.isEmpty()) continue;

            for (TimestampedElement<L> le : leftEntry.getValue()) {
                for (TimestampedElement<R> re : rightList) {
                    emitJoin(le.element, le.timestamp, re.element, re.timestamp);
                }
            }
        }

        // Evict window state
        leftWindows.remove(wStart);
        rightWindows.remove(wStart);
        windowsFired++;
    }

    // ═══════════════════════════════════════════════════════
    //  JOIN EMIT
    // ═══════════════════════════════════════════════════════

    private void emitJoin(L left, long tL, R right, long tR) {
        long outTs = Math.max(tL, tR);

        JoinContext ctx = new JoinContext() {
            @Override public long getLeftTimestamp()  { return tL; }
            @Override public long getRightTimestamp() { return tR; }
            @Override public long getTimestamp()      { return outTs; }
            @Override public <X> void output(String tag, X value) {
                sideOutputRecords.add(new SideOutputRecord<>(tag, value));
            }
        };

        JoinCollector<OUT> collector = new JoinCollector<>() {
            @Override public void collect(OUT record, long timestamp) {
                outputRecords.add(new TimestampedOutput<>(record, timestamp));
            }
            @Override public <X> void sideOutput(String tag, X value) {
                sideOutputRecords.add(new SideOutputRecord<>(tag, value));
            }
        };

        joinFunction.processElement(left, right, ctx, collector);
    }

    // ═══════════════════════════════════════════════════════
    //  OUTPUT ACCESS
    // ═══════════════════════════════════════════════════════

    /** Drain and return all main output records since last drain. */
    public List<TimestampedOutput<OUT>> drainOutputs() {
        List<TimestampedOutput<OUT>> r = new ArrayList<>(outputRecords);
        outputRecords.clear();
        return r;
    }

    /** Drain and return all side-output records since last drain. */
    public List<SideOutputRecord<?>> drainSideOutputs() {
        List<SideOutputRecord<?>> r = new ArrayList<>(sideOutputRecords);
        sideOutputRecords.clear();
        return r;
    }

    /** Peek (non-destructive) at accumulated main outputs. */
    public List<TimestampedOutput<OUT>> peekOutputs() {
        return Collections.unmodifiableList(outputRecords);
    }

    /** Peek (non-destructive) at accumulated side outputs. */
    public List<SideOutputRecord<?>> peekSideOutputs() {
        return Collections.unmodifiableList(sideOutputRecords);
    }

    // ═══════════════════════════════════════════════════════
    //  INTROSPECTION
    // ═══════════════════════════════════════════════════════

    public long getWindowSize()       { return windowSize; }
    public long getSlideSize()        { return slideSize; }
    public long getAllowedLateness()   { return allowedLateness; }
    public long getCurrentWatermark() { return currentWatermark; }
    public long getWindowsFired()     { return windowsFired; }

    /** Number of active (not yet fired) windows on the left side. */
    public int getActiveLeftWindowCount()  { return leftWindows.size(); }

    /** Number of active (not yet fired) windows on the right side. */
    public int getActiveRightWindowCount() { return rightWindows.size(); }

    /** Total buffered left elements across all windows. */
    public int getLeftBufferSize() {
        return leftWindows.values().stream()
                .flatMap(m -> m.values().stream())
                .mapToInt(List::size).sum();
    }

    /** Total buffered right elements across all windows. */
    public int getRightBufferSize() {
        return rightWindows.values().stream()
                .flatMap(m -> m.values().stream())
                .mapToInt(List::size).sum();
    }

    @Override
    public String toString() {
        return "SlidingWindowJoinOperator{" +
                "windowSize=" + windowSize +
                ", slideSize=" + slideSize +
                ", allowedLateness=" + allowedLateness +
                ", activeWindows=" + Math.max(getActiveLeftWindowCount(), getActiveRightWindowCount()) +
                ", windowsFired=" + windowsFired +
                '}';
    }
}
