package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.apache.log4j.Logger;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin.*;

import java.util.*;
import java.util.function.Function;

public class FlinkIntervalJoinOperator<K, L, R, OUT> {

    private static final Logger log = Logger.getLogger(FlinkIntervalJoinOperator.class);

    // ────────── configuration ──────────
    private final long lowerBound;
    private final long upperBound;

    private final String leftLateDataOutputTag;
    private final String rightLateDataOutputTag;

    private final Function<L, K> leftKeyExtractor;
    private final Function<R, K> rightKeyExtractor;

    private final ProcessJoinFunction<L, R, OUT> joinFunction;

    // ────────── keyed state buffers ──────────
    // key → (eventTimestamp → list of buffer entries)
    private final Map<K, TreeMap<Long, List<BufferEntry<L>>>> leftBuffer = new HashMap<>();
    private final Map<K, TreeMap<Long, List<BufferEntry<R>>>> rightBuffer = new HashMap<>();

    // ────────── event-time timer service ──────────
    // We maintain two separate timer sets (left/right namespaces)
    // key → sorted set of cleanup timestamps
    private final Map<K, TreeSet<Long>> leftCleanupTimers = new HashMap<>();
    private final Map<K, TreeSet<Long>> rightCleanupTimers = new HashMap<>();

    // ────────── watermark ──────────
    private long currentWatermark = Long.MIN_VALUE;

    // ────────── output collections (populated during processing) ──────────
    private final List<TimestampedOutput<OUT>> outputRecords = new ArrayList<>();
    private final List<SideOutputRecord<?>> sideOutputRecords = new ArrayList<>();

    // ────────── nested output types ──────────

    /**
     * A main output record together with its event-time.
     */
    public static final class TimestampedOutput<T> {
        private final T record;
        private final long timestamp;

        public TimestampedOutput(T record, long timestamp) {
            this.record = record;
            this.timestamp = timestamp;
        }

        public T getRecord() { return record; }
        public long getTimestamp() { return timestamp; }

        @Override
        public String toString() {
            return "Output{" + record + " @" + timestamp + '}';
        }
    }

    /**
     * A side-output record (late data).
     */
    public static final class SideOutputRecord<T> {
        private final String tag;
        private final T value;

        public SideOutputRecord(String tag, T value) {
            this.tag = tag;
            this.value = value;
        }

        public String getTag() { return tag; }
        public T getValue() { return value; }

        @Override
        public String toString() {
            return "SideOutput{tag=" + tag + ", val=" + value + '}';
        }
    }

    // ────────── builder ──────────

    public static <K, L, R, OUT> Builder<K, L, R, OUT> builder() {
        return new Builder<>();
    }

    public static final class Builder<K, L, R, OUT> {
        private long lowerBound;
        private long upperBound;
        private boolean lowerInclusive = true;
        private boolean upperInclusive = true;
        private String leftLateTag = "left-late";
        private String rightLateTag = "right-late";
        private Function<L, K> leftKeyExtractor;
        private Function<R, K> rightKeyExtractor;
        private ProcessJoinFunction<L, R, OUT> joinFunction;

        public Builder<K, L, R, OUT> lowerBound(long bound) { this.lowerBound = bound; return this; }
        public Builder<K, L, R, OUT> upperBound(long bound) { this.upperBound = bound; return this; }
        public Builder<K, L, R, OUT> lowerInclusive(boolean v) { this.lowerInclusive = v; return this; }
        public Builder<K, L, R, OUT> upperInclusive(boolean v) { this.upperInclusive = v; return this; }
        public Builder<K, L, R, OUT> leftLateDataOutputTag(String t) { this.leftLateTag = t; return this; }
        public Builder<K, L, R, OUT> rightLateDataOutputTag(String t) { this.rightLateTag = t; return this; }
        public Builder<K, L, R, OUT> leftKeyExtractor(Function<L, K> fn) { this.leftKeyExtractor = fn; return this; }
        public Builder<K, L, R, OUT> rightKeyExtractor(Function<R, K> fn) { this.rightKeyExtractor = fn; return this; }
        public Builder<K, L, R, OUT> processJoinFunction(ProcessJoinFunction<L, R, OUT> fn) { this.joinFunction = fn; return this; }

        public FlinkIntervalJoinOperator<K, L, R, OUT> build() {
            Objects.requireNonNull(leftKeyExtractor, "leftKeyExtractor");
            Objects.requireNonNull(rightKeyExtractor, "rightKeyExtractor");
            Objects.requireNonNull(joinFunction, "processJoinFunction");

            // Translate exclusive bounds to inclusive by shifting ±1, like Flink
            long effectiveLower = lowerInclusive ? lowerBound : lowerBound + 1;
            long effectiveUpper = upperInclusive ? upperBound : upperBound - 1;

            if (effectiveLower > effectiveUpper) {
                throw new IllegalArgumentException(
                        "After bound adjustment the lower bound (" + effectiveLower +
                                ") is greater than the upper bound (" + effectiveUpper + ")");
            }

            return new FlinkIntervalJoinOperator<>(
                    effectiveLower, effectiveUpper,
                    leftLateTag, rightLateTag,
                    leftKeyExtractor, rightKeyExtractor,
                    joinFunction);
        }
    }

    // ────────── constructor ──────────

    private FlinkIntervalJoinOperator(
            long lowerBound, long upperBound,
            String leftLateDataOutputTag, String rightLateDataOutputTag,
            Function<L, K> leftKeyExtractor, Function<R, K> rightKeyExtractor,
            ProcessJoinFunction<L, R, OUT> joinFunction) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.leftLateDataOutputTag = leftLateDataOutputTag;
        this.rightLateDataOutputTag = rightLateDataOutputTag;
        this.leftKeyExtractor = leftKeyExtractor;
        this.rightKeyExtractor = rightKeyExtractor;
        this.joinFunction = joinFunction;
    }

    // ═══════════════════════════════════════════
    //  PUBLIC API
    // ═══════════════════════════════════════════

    /**
     * Advance the watermark.  Should be called whenever the watermark
     * changes (e.g. periodically, or after ingesting a batch of records).
     * Fires all cleanup timers whose time ≤ {@code newWatermark}.
     */
    public void advanceWatermark(long newWatermark) {
        if (newWatermark <= currentWatermark) {
            return;
        }
        currentWatermark = newWatermark;

        // Fire LEFT cleanup timers
        for (Map.Entry<K, TreeSet<Long>> entry : new ArrayList<>(leftCleanupTimers.entrySet())) {
            K key = entry.getKey();
            TreeSet<Long> timers = entry.getValue();
            NavigableSet<Long> fired = timers.headSet(newWatermark, true);
            for (Long timerTs : new ArrayList<>(fired)) {
                onLeftCleanupTimer(key, timerTs);
            }
            fired.clear();
            if (timers.isEmpty()) {
                leftCleanupTimers.remove(key);
            }
        }

        // Fire RIGHT cleanup timers
        for (Map.Entry<K, TreeSet<Long>> entry : new ArrayList<>(rightCleanupTimers.entrySet())) {
            K key = entry.getKey();
            TreeSet<Long> timers = entry.getValue();
            NavigableSet<Long> fired = timers.headSet(newWatermark, true);
            for (Long timerTs : new ArrayList<>(fired)) {
                onRightCleanupTimer(key, timerTs);
            }
            fired.clear();
            if (timers.isEmpty()) {
                rightCleanupTimers.remove(key);
            }
        }
    }

    /**
     * Process a record arriving on the <b>left</b> input.
     *
     * @param element the left element
     * @param tL      its event-time timestamp
     * @throws IllegalArgumentException if {@code tL == Long.MIN_VALUE}
     */
    public void processLeft(L element, long tL) {
        if (tL == Long.MIN_VALUE) {
            throw new IllegalArgumentException("Timestamp must not be Long.MIN_VALUE (missing event-time).");
        }

        K key = leftKeyExtractor.apply(element);

        // Late-data check
        if (tL < currentWatermark) {
            sideOutputRecords.add(new SideOutputRecord<>(leftLateDataOutputTag, element));
            log.debug("LEFT late: " + element + " tL=" + tL + " wm=" + currentWatermark);
            return;
        }

        // 1. Buffer the left record
        addToBuffer(leftBuffer, key, tL, new BufferEntry<>(element, false));

        // 2. Scan right buffer for matches: tR ∈ [tL + lowerBound, tL + upperBound]
        TreeMap<Long, List<BufferEntry<R>>> rightForKey = rightBuffer.get(key);
        if (rightForKey != null) {
            long scanLow = tL + lowerBound;
            long scanHigh = tL + upperBound;
            NavigableMap<Long, List<BufferEntry<R>>> candidates =
                    rightForKey.subMap(scanLow, true, scanHigh, true);

            for (Map.Entry<Long, List<BufferEntry<R>>> tsEntry : candidates.entrySet()) {
                long tR = tsEntry.getKey();
                for (BufferEntry<R> rightEntry : tsEntry.getValue()) {
                    emitJoin(element, tL, rightEntry.getElement(), tR);
                    rightEntry.markJoined();
                }
            }
        }

        // 3. Register LEFT cleanup timer
        //    Left record at tL can be matched by a right record at tR up to tL + upperBound.
        //    The right record arrives at most at watermark = tR, so the left record can be
        //    safely cleaned when watermark reaches tL + upperBound (+ 1 for safety like Flink).
        long leftCleanupTime = (upperBound <= 0) ? tL : tL + upperBound;
        registerLeftTimer(key, leftCleanupTime);
    }

    /**
     * Process a record arriving on the <b>right</b> input.
     *
     * @param element the right element
     * @param tR      its event-time timestamp
     * @throws IllegalArgumentException if {@code tR == Long.MIN_VALUE}
     */
    public void processRight(R element, long tR) {
        if (tR == Long.MIN_VALUE) {
            throw new IllegalArgumentException("Timestamp must not be Long.MIN_VALUE (missing event-time).");
        }

        K key = rightKeyExtractor.apply(element);

        // Late-data check
        if (tR < currentWatermark) {
            sideOutputRecords.add(new SideOutputRecord<>(rightLateDataOutputTag, element));
            log.debug("RIGHT late: " + element + " tR=" + tR + " wm=" + currentWatermark);
            return;
        }

        // 1. Buffer the right record
        addToBuffer(rightBuffer, key, tR, new BufferEntry<>(element, false));

        // 2. Scan left buffer for matches.
        //    We need: tR ∈ [tL + lowerBound, tL + upperBound]
        //    ⟺ tL ∈ [tR - upperBound, tR - lowerBound]
        TreeMap<Long, List<BufferEntry<L>>> leftForKey = leftBuffer.get(key);
        if (leftForKey != null) {
            long scanLow = tR - upperBound;
            long scanHigh = tR - lowerBound;
            NavigableMap<Long, List<BufferEntry<L>>> candidates =
                    leftForKey.subMap(scanLow, true, scanHigh, true);

            for (Map.Entry<Long, List<BufferEntry<L>>> tsEntry : candidates.entrySet()) {
                long tL = tsEntry.getKey();
                for (BufferEntry<L> leftEntry : tsEntry.getValue()) {
                    emitJoin(leftEntry.getElement(), tL, element, tR);
                    leftEntry.markJoined();
                }
            }
        }

        // 3. Register RIGHT cleanup timer
        //    Right record at tR can be matched by a left record at tL up to tR - lowerBound.
        //    Cleanup when watermark reaches tR - lowerBound (or tR if lowerBound ≥ 0).
        long rightCleanupTime = (lowerBound <= 0) ? tR + Math.abs(lowerBound) : tR;
        registerRightTimer(key, rightCleanupTime);
    }

    // ═══════════════════════════════════════════
    //  OUTPUT ACCESS
    // ═══════════════════════════════════════════

    /**
     * Drain and return all main output records produced since the last drain.
     */
    public List<TimestampedOutput<OUT>> drainOutputs() {
        List<TimestampedOutput<OUT>> result = new ArrayList<>(outputRecords);
        outputRecords.clear();
        return result;
    }

    /**
     * Drain and return all side-output records produced since the last drain.
     */
    public List<SideOutputRecord<?>> drainSideOutputs() {
        List<SideOutputRecord<?>> result = new ArrayList<>(sideOutputRecords);
        sideOutputRecords.clear();
        return result;
    }

    /** Peek (non-destructive) at accumulated main outputs. */
    public List<TimestampedOutput<OUT>> peekOutputs() {
        return Collections.unmodifiableList(outputRecords);
    }

    /** Peek (non-destructive) at accumulated side-outputs. */
    public List<SideOutputRecord<?>> peekSideOutputs() {
        return Collections.unmodifiableList(sideOutputRecords);
    }

    // ═══════════════════════════════════════════
    //  INTROSPECTION (for testing / debugging)
    // ═══════════════════════════════════════════

    public long getCurrentWatermark() { return currentWatermark; }
    public long getLowerBound() { return lowerBound; }
    public long getUpperBound() { return upperBound; }

    /** Total number of entries across all keys in the left buffer. */
    public int getLeftBufferSize() {
        return leftBuffer.values().stream()
                .flatMap(tm -> tm.values().stream())
                .mapToInt(List::size).sum();
    }

    /** Total number of entries across all keys in the right buffer. */
    public int getRightBufferSize() {
        return rightBuffer.values().stream()
                .flatMap(tm -> tm.values().stream())
                .mapToInt(List::size).sum();
    }

    /** Number of distinct keys in the left buffer. */
    public int getLeftKeyCount() { return leftBuffer.size(); }

    /** Number of distinct keys in the right buffer. */
    public int getRightKeyCount() { return rightBuffer.size(); }

    /** Number of pending left cleanup timers across all keys. */
    public int getLeftTimerCount() {
        return leftCleanupTimers.values().stream().mapToInt(Set::size).sum();
    }

    /** Number of pending right cleanup timers across all keys. */
    public int getRightTimerCount() {
        return rightCleanupTimers.values().stream().mapToInt(Set::size).sum();
    }

    // ═══════════════════════════════════════════
    //  INTERNAL HELPERS
    // ═══════════════════════════════════════════

    private <T> void addToBuffer(Map<K, TreeMap<Long, List<BufferEntry<T>>>> buffer,
                                 K key, long ts, BufferEntry<T> entry) {
        buffer.computeIfAbsent(key, k -> new TreeMap<>())
                .computeIfAbsent(ts, t -> new ArrayList<>())
                .add(entry);
    }

    private void emitJoin(L left, long tL, R right, long tR) {
        long outTs = Math.max(tL, tR);

        JoinContext ctx = new JoinContext() {
            @Override public long getLeftTimestamp() { return tL; }
            @Override public long getRightTimestamp() { return tR; }
            @Override public long getTimestamp() { return outTs; }
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

    // ────────── timer registration ──────────

    private void registerLeftTimer(K key, long time) {
        leftCleanupTimers.computeIfAbsent(key, k -> new TreeSet<>()).add(time);
    }

    private void registerRightTimer(K key, long time) {
        rightCleanupTimers.computeIfAbsent(key, k -> new TreeSet<>()).add(time);
    }

    // ────────── cleanup callbacks (mirror Flink logic) ──────────

    /**
     * LEFT cleanup timer fires at {@code timerTs}.
     * Remove left buffer entries at:
     * <pre>
     *   tsToRemove = (upperBound <= 0) ? timerTs : timerTs - upperBound
     * </pre>
     */
    private void onLeftCleanupTimer(K key, long timerTs) {
        long tsToRemove = (upperBound <= 0) ? timerTs : timerTs - upperBound;
        TreeMap<Long, List<BufferEntry<L>>> map = leftBuffer.get(key);
        if (map != null) {
            map.remove(tsToRemove);
            if (map.isEmpty()) {
                leftBuffer.remove(key);
            }
        }
        log.debug("LEFT cleanup key=" + key + " timerTs=" + timerTs + " removed ts=" + tsToRemove);
    }

    /**
     * RIGHT cleanup timer fires at {@code timerTs}.
     * Remove right buffer entries at:
     * <pre>
     *   tsToRemove = (lowerBound <= 0) ? timerTs + lowerBound : timerTs
     * </pre>
     */
    private void onRightCleanupTimer(K key, long timerTs) {
        long tsToRemove = (lowerBound <= 0) ? timerTs + lowerBound : timerTs;
        TreeMap<Long, List<BufferEntry<R>>> map = rightBuffer.get(key);
        if (map != null) {
            map.remove(tsToRemove);
            if (map.isEmpty()) {
                rightBuffer.remove(key);
            }
        }
        log.debug("RIGHT cleanup key=" + key + " timerTs=" + timerTs + " removed ts=" + tsToRemove);
    }
}
