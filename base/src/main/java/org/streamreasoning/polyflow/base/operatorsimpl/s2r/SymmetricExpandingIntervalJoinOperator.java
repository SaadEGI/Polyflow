package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.apache.log4j.Logger;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin.*;

import java.util.*;
import java.util.function.Function;

/**
 * Symmetric Expanding Interval Join Operator for Polyflow.
 * <p>
 * A two-input, keyed, event-time inner join where the time window <b>expands
 * symmetrically</b> when an arriving record finds <b>no matching candidates</b>
 * in the opposite buffer.
 *
 * <h3>Base window for a left record L at tL</h3>
 * <pre>
 *   [tL − βK , tL + αK]
 * </pre>
 * A right record R at tR matches if:
 * <pre>
 *   key(L) == key(R)  AND  tR ∈ [tL − βK , tL + αK]
 * </pre>
 *
 * <h3>Symmetric expansion (triggered only when no matches are found)</h3>
 * Each key K maintains dynamic bounds αK (future) and βK (past), initialised
 * to α0 and β0.  When a record arrives and finds <em>zero</em> matches in the
 * opposite buffer the operator expands:
 * <pre>
 *   αK := min(αK + Δ , αmax)
 *   βK := min(βK + Δ , βmax)
 * </pre>
 * and immediately re-probes with the wider window.  When matches <em>are</em>
 * found on the first probe the bounds stay unchanged.
 *
 * <h3>Output timestamp</h3>
 * <pre>max(tL , tR)</pre>
 *
 * <h3>Late data</h3>
 * Records with event-time strictly below the current watermark are emitted to
 * configurable side outputs ({@code leftLateDataOutputTag} /
 * {@code rightLateDataOutputTag}).
 *
 * <h3>Cleanup</h3>
 * Event-time timers evict buffer entries when they can no longer be matched.
 * Timers use the <em>current</em> per-key bounds at registration time so that
 * expanded windows keep state proportionally longer.
 *
 * @param <K>   key type
 * @param <L>   left stream element type
 * @param <R>   right stream element type
 * @param <OUT> output element type
 */
public class SymmetricExpandingIntervalJoinOperator<K, L, R, OUT> {

    private static final Logger log = Logger.getLogger(SymmetricExpandingIntervalJoinOperator.class);

    // ────────── initial / max configuration ──────────
    private final long alpha0;          // initial future bound
    private final long beta0;           // initial past bound
    private final long alphaMax;        // max future bound
    private final long betaMax;         // max past bound
    private final long delta;           // expansion step

    // ────────── side-output tags ──────────
    private final String leftLateDataOutputTag;
    private final String rightLateDataOutputTag;

    // ────────── key extractors ──────────
    private final Function<L, K> leftKeyExtractor;
    private final Function<R, K> rightKeyExtractor;

    // ────────── user join function ──────────
    private final ProcessJoinFunction<L, R, OUT> joinFunction;

    // ────────── per-key dynamic bounds ──────────
    // key → [currentAlpha, currentBeta]
    private final Map<K, long[]> keyBounds = new HashMap<>();

    // ────────── keyed state buffers ──────────
    // key → (eventTimestamp → list of buffer entries)
    private final Map<K, TreeMap<Long, List<BufferEntry<L>>>> leftBuffer = new HashMap<>();
    private final Map<K, TreeMap<Long, List<BufferEntry<R>>>> rightBuffer = new HashMap<>();

    // ────────── event-time cleanup timers (separate namespaces) ──────────
    private final Map<K, TreeSet<Long>> leftCleanupTimers = new HashMap<>();
    private final Map<K, TreeSet<Long>> rightCleanupTimers = new HashMap<>();

    // ────────── watermark ──────────
    private long currentWatermark = Long.MIN_VALUE;

    // ────────── output accumulation ──────────
    private final List<TimestampedOutput<OUT>> outputRecords = new ArrayList<>();
    private final List<SideOutputRecord<?>> sideOutputRecords = new ArrayList<>();

    // ────────── expansion event log (for testing / observability) ──────────
    private final List<ExpansionEvent<K>> expansionLog = new ArrayList<>();

    // ═══════════════════════════════════════════════════════
    //  PUBLIC NESTED TYPES
    // ═══════════════════════════════════════════════════════

    /** A main output record together with its event-time. */
    public static final class TimestampedOutput<T> {
        private final T record;
        private final long timestamp;

        public TimestampedOutput(T record, long timestamp) {
            this.record = record;
            this.timestamp = timestamp;
        }

        public T getRecord()    { return record; }
        public long getTimestamp() { return timestamp; }

        @Override public String toString() {
            return "Output{" + record + " @" + timestamp + '}';
        }
    }

    /** A side-output record (late data or user-emitted). */
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

    /** Records every expansion event for observability. */
    public static final class ExpansionEvent<K> {
        private final K key;
        private final long oldAlpha;
        private final long oldBeta;
        private final long newAlpha;
        private final long newBeta;
        private final String triggerSide;   // "LEFT" or "RIGHT"
        private final long recordTimestamp;

        public ExpansionEvent(K key, long oldAlpha, long oldBeta,
                              long newAlpha, long newBeta,
                              String triggerSide, long recordTimestamp) {
            this.key = key;
            this.oldAlpha = oldAlpha;
            this.oldBeta = oldBeta;
            this.newAlpha = newAlpha;
            this.newBeta = newBeta;
            this.triggerSide = triggerSide;
            this.recordTimestamp = recordTimestamp;
        }

        public K getKey()             { return key; }
        public long getOldAlpha()     { return oldAlpha; }
        public long getOldBeta()      { return oldBeta; }
        public long getNewAlpha()     { return newAlpha; }
        public long getNewBeta()      { return newBeta; }
        public String getTriggerSide(){ return triggerSide; }
        public long getRecordTimestamp() { return recordTimestamp; }

        @Override public String toString() {
            return "Expansion{key=" + key +
                    ", α:" + oldAlpha + "→" + newAlpha +
                    ", β:" + oldBeta  + "→" + newBeta  +
                    ", side=" + triggerSide +
                    ", ts=" + recordTimestamp + '}';
        }
    }

    // ═══════════════════════════════════════════════════════
    //  BUILDER
    // ═══════════════════════════════════════════════════════

    public static <K, L, R, OUT> Builder<K, L, R, OUT> builder() {
        return new Builder<>();
    }

    public static final class Builder<K, L, R, OUT> {
        private long alpha0;
        private long beta0;
        private long alphaMax = Long.MAX_VALUE;
        private long betaMax  = Long.MAX_VALUE;
        private long delta;
        private String leftLateTag  = "left-late";
        private String rightLateTag = "right-late";
        private Function<L, K> leftKeyExtractor;
        private Function<R, K> rightKeyExtractor;
        private ProcessJoinFunction<L, R, OUT> joinFunction;

        /** Initial future bound α₀ (≥ 0). */
        public Builder<K, L, R, OUT> alpha0(long v)    { this.alpha0 = v; return this; }
        /** Initial past bound β₀ (≥ 0). */
        public Builder<K, L, R, OUT> beta0(long v)     { this.beta0 = v; return this; }
        /** Maximum future bound αmax. */
        public Builder<K, L, R, OUT> alphaMax(long v)  { this.alphaMax = v; return this; }
        /** Maximum past bound βmax. */
        public Builder<K, L, R, OUT> betaMax(long v)   { this.betaMax = v; return this; }
        /** Symmetric expansion step Δ. */
        public Builder<K, L, R, OUT> delta(long v)     { this.delta = v; return this; }
        /** Side-output tag for late left records. */
        public Builder<K, L, R, OUT> leftLateDataOutputTag(String t)  { this.leftLateTag = t; return this; }
        /** Side-output tag for late right records. */
        public Builder<K, L, R, OUT> rightLateDataOutputTag(String t) { this.rightLateTag = t; return this; }
        /** Key extractor for the left stream. */
        public Builder<K, L, R, OUT> leftKeyExtractor(Function<L, K> fn)  { this.leftKeyExtractor = fn; return this; }
        /** Key extractor for the right stream. */
        public Builder<K, L, R, OUT> rightKeyExtractor(Function<R, K> fn) { this.rightKeyExtractor = fn; return this; }
        /** The user-defined join function. */
        public Builder<K, L, R, OUT> processJoinFunction(ProcessJoinFunction<L, R, OUT> fn) { this.joinFunction = fn; return this; }

        public SymmetricExpandingIntervalJoinOperator<K, L, R, OUT> build() {
            Objects.requireNonNull(leftKeyExtractor,  "leftKeyExtractor");
            Objects.requireNonNull(rightKeyExtractor, "rightKeyExtractor");
            Objects.requireNonNull(joinFunction,      "processJoinFunction");
            if (alpha0 < 0) throw new IllegalArgumentException("alpha0 must be ≥ 0, got " + alpha0);
            if (beta0  < 0) throw new IllegalArgumentException("beta0 must be ≥ 0, got "  + beta0);
            if (delta  < 0) throw new IllegalArgumentException("delta must be ≥ 0, got "  + delta);
            if (alphaMax < alpha0) throw new IllegalArgumentException("alphaMax < alpha0");
            if (betaMax  < beta0)  throw new IllegalArgumentException("betaMax < beta0");

            return new SymmetricExpandingIntervalJoinOperator<>(
                    alpha0, beta0, alphaMax, betaMax, delta,
                    leftLateTag, rightLateTag,
                    leftKeyExtractor, rightKeyExtractor, joinFunction);
        }
    }

    // ────────── constructor ──────────

    private SymmetricExpandingIntervalJoinOperator(
            long alpha0, long beta0, long alphaMax, long betaMax, long delta,
            String leftLateDataOutputTag, String rightLateDataOutputTag,
            Function<L, K> leftKeyExtractor, Function<R, K> rightKeyExtractor,
            ProcessJoinFunction<L, R, OUT> joinFunction) {
        this.alpha0 = alpha0;
        this.beta0  = beta0;
        this.alphaMax = alphaMax;
        this.betaMax  = betaMax;
        this.delta = delta;
        this.leftLateDataOutputTag  = leftLateDataOutputTag;
        this.rightLateDataOutputTag = rightLateDataOutputTag;
        this.leftKeyExtractor  = leftKeyExtractor;
        this.rightKeyExtractor = rightKeyExtractor;
        this.joinFunction = joinFunction;
    }

    // ═══════════════════════════════════════════════════════
    //  WATERMARK
    // ═══════════════════════════════════════════════════════

    /**
     * Advance the watermark.  Fires all expired cleanup timers.
     */
    public void advanceWatermark(long newWatermark) {
        if (newWatermark <= currentWatermark) return;
        currentWatermark = newWatermark;

        fireTimers(leftCleanupTimers, newWatermark, true);
        fireTimers(rightCleanupTimers, newWatermark, false);
    }

    private void fireTimers(Map<K, TreeSet<Long>> timerMap, long wm, boolean isLeft) {
        for (Map.Entry<K, TreeSet<Long>> entry : new ArrayList<>(timerMap.entrySet())) {
            K key = entry.getKey();
            TreeSet<Long> timers = entry.getValue();
            NavigableSet<Long> fired = timers.headSet(wm, true);
            for (Long timerTs : new ArrayList<>(fired)) {
                if (isLeft) {
                    onLeftCleanupTimer(key, timerTs);
                } else {
                    onRightCleanupTimer(key, timerTs);
                }
            }
            fired.clear();
            if (timers.isEmpty()) timerMap.remove(key);
        }
    }

    // ═══════════════════════════════════════════════════════
    //  PROCESS LEFT
    // ═══════════════════════════════════════════════════════

    /**
     * Process a record arriving on the <b>left</b> input.
     *
     * @param element the left element
     * @param tL      its event-time timestamp
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

        // Ensure per-key bounds exist
        ensureBounds(key);
        long[] bounds = keyBounds.get(key);  // [alpha, beta]

        // 1. Buffer the left record
        addToBuffer(leftBuffer, key, tL, new BufferEntry<>(element, false));

        // 2. Probe right buffer with current bounds:  tR ∈ [tL − βK , tL + αK]
        int matchCount = probeRightAndEmit(element, key, tL, bounds[1], bounds[0]);

        // 3. If no matches → expand symmetrically and re-probe with expanded zone only
        if (matchCount == 0 && canExpand(bounds)) {
            long oldAlpha = bounds[0];
            long oldBeta  = bounds[1];

            expand(key, bounds);

            log.debug("LEFT expansion key=" + key + " α:" + oldAlpha + "→" + bounds[0] +
                    " β:" + oldBeta + "→" + bounds[1]);

            expansionLog.add(new ExpansionEvent<>(key, oldAlpha, oldBeta,
                    bounds[0], bounds[1], "LEFT", tL));

            // Re-probe the NEWLY OPENED zone only (avoid duplicates)
            probeRightExpandedZone(element, key, tL, oldBeta, oldAlpha, bounds[1], bounds[0]);
        }

        // 4. Register LEFT cleanup timer using current (possibly expanded) bounds
        long leftCleanupTime = tL + bounds[0]; // tL + αK
        registerTimer(leftCleanupTimers, key, leftCleanupTime);
    }

    // ═══════════════════════════════════════════════════════
    //  PROCESS RIGHT
    // ═══════════════════════════════════════════════════════

    /**
     * Process a record arriving on the <b>right</b> input.
     *
     * @param element the right element
     * @param tR      its event-time timestamp
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

        // Ensure per-key bounds exist
        ensureBounds(key);
        long[] bounds = keyBounds.get(key);  // [alpha, beta]

        // 1. Buffer the right record
        addToBuffer(rightBuffer, key, tR, new BufferEntry<>(element, false));

        // 2. Probe left buffer with current bounds:  tL ∈ [tR − αK , tR + βK]
        int matchCount = probeLeftAndEmit(element, key, tR, bounds[0], bounds[1]);

        // 3. If no matches → expand symmetrically and re-probe expanded zone only
        if (matchCount == 0 && canExpand(bounds)) {
            long oldAlpha = bounds[0];
            long oldBeta  = bounds[1];

            expand(key, bounds);

            log.debug("RIGHT expansion key=" + key + " α:" + oldAlpha + "→" + bounds[0] +
                    " β:" + oldBeta + "→" + bounds[1]);

            expansionLog.add(new ExpansionEvent<>(key, oldAlpha, oldBeta,
                    bounds[0], bounds[1], "RIGHT", tR));

            // Re-probe the NEWLY OPENED zone only (avoid duplicates)
            probeLeftExpandedZone(element, key, tR, oldAlpha, oldBeta, bounds[0], bounds[1]);
        }

        // 4. Register RIGHT cleanup timer using current (possibly expanded) bounds
        long rightCleanupTime = tR + bounds[1]; // tR + βK
        registerTimer(rightCleanupTimers, key, rightCleanupTime);
    }

    // ═══════════════════════════════════════════════════════
    //  PROBE HELPERS
    // ═══════════════════════════════════════════════════════

    /**
     * Probe right buffer for left record L at tL with bounds [tL − beta, tL + alpha].
     * Emits all matches.
     * @return number of matches found
     */
    private int probeRightAndEmit(L leftElem, K key, long tL, long beta, long alpha) {
        TreeMap<Long, List<BufferEntry<R>>> rightForKey = rightBuffer.get(key);
        if (rightForKey == null || rightForKey.isEmpty()) return 0;

        long lo = tL - beta;
        long hi = tL + alpha;
        NavigableMap<Long, List<BufferEntry<R>>> candidates =
                rightForKey.subMap(lo, true, hi, true);

        int count = 0;
        for (Map.Entry<Long, List<BufferEntry<R>>> tsEntry : candidates.entrySet()) {
            long tR = tsEntry.getKey();
            for (BufferEntry<R> re : tsEntry.getValue()) {
                emitJoin(leftElem, tL, re.getElement(), tR);
                re.markJoined();
                count++;
            }
        }
        return count;
    }

    /**
     * After expansion, probe only the newly opened zones in the right buffer
     * to avoid duplicate emissions.
     * <p>
     * Old range was [tL − oldBeta, tL + oldAlpha].
     * New range is  [tL − newBeta, tL + newAlpha].
     * We probe [tL − newBeta, tL − oldBeta) ∪ (tL + oldAlpha, tL + newAlpha].
     */
    private void probeRightExpandedZone(L leftElem, K key, long tL,
                                         long oldBeta, long oldAlpha,
                                         long newBeta, long newAlpha) {
        TreeMap<Long, List<BufferEntry<R>>> rightForKey = rightBuffer.get(key);
        if (rightForKey == null || rightForKey.isEmpty()) return;

        // Left wing: [tL − newBeta, tL − oldBeta)   (only if newBeta > oldBeta)
        if (newBeta > oldBeta) {
            long lo = tL - newBeta;
            long hi = tL - oldBeta - 1; // exclusive of old boundary
            if (lo <= hi) {
                NavigableMap<Long, List<BufferEntry<R>>> wing =
                        rightForKey.subMap(lo, true, hi, true);
                for (Map.Entry<Long, List<BufferEntry<R>>> e : wing.entrySet()) {
                    long tR = e.getKey();
                    for (BufferEntry<R> re : e.getValue()) {
                        emitJoin(leftElem, tL, re.getElement(), tR);
                        re.markJoined();
                    }
                }
            }
        }

        // Right wing: (tL + oldAlpha, tL + newAlpha]   (only if newAlpha > oldAlpha)
        if (newAlpha > oldAlpha) {
            long lo = tL + oldAlpha + 1; // exclusive of old boundary
            long hi = tL + newAlpha;
            if (lo <= hi) {
                NavigableMap<Long, List<BufferEntry<R>>> wing =
                        rightForKey.subMap(lo, true, hi, true);
                for (Map.Entry<Long, List<BufferEntry<R>>> e : wing.entrySet()) {
                    long tR = e.getKey();
                    for (BufferEntry<R> re : e.getValue()) {
                        emitJoin(leftElem, tL, re.getElement(), tR);
                        re.markJoined();
                    }
                }
            }
        }
    }

    /**
     * Probe left buffer for right record R at tR with bounds [tR − alpha, tR + beta].
     * @return number of matches found
     */
    private int probeLeftAndEmit(R rightElem, K key, long tR, long alpha, long beta) {
        TreeMap<Long, List<BufferEntry<L>>> leftForKey = leftBuffer.get(key);
        if (leftForKey == null || leftForKey.isEmpty()) return 0;

        long lo = tR - alpha;
        long hi = tR + beta;
        NavigableMap<Long, List<BufferEntry<L>>> candidates =
                leftForKey.subMap(lo, true, hi, true);

        int count = 0;
        for (Map.Entry<Long, List<BufferEntry<L>>> tsEntry : candidates.entrySet()) {
            long tL = tsEntry.getKey();
            for (BufferEntry<L> le : tsEntry.getValue()) {
                emitJoin(le.getElement(), tL, rightElem, tR);
                le.markJoined();
                count++;
            }
        }
        return count;
    }

    /**
     * After expansion, probe only the newly opened zones in the left buffer
     * to avoid duplicate emissions.
     * <p>
     * Old range: [tR − oldAlpha, tR + oldBeta].
     * New range: [tR − newAlpha, tR + newBeta].
     */
    private void probeLeftExpandedZone(R rightElem, K key, long tR,
                                        long oldAlpha, long oldBeta,
                                        long newAlpha, long newBeta) {
        TreeMap<Long, List<BufferEntry<L>>> leftForKey = leftBuffer.get(key);
        if (leftForKey == null || leftForKey.isEmpty()) return;

        // Left wing: [tR − newAlpha, tR − oldAlpha)
        if (newAlpha > oldAlpha) {
            long lo = tR - newAlpha;
            long hi = tR - oldAlpha - 1;
            if (lo <= hi) {
                NavigableMap<Long, List<BufferEntry<L>>> wing =
                        leftForKey.subMap(lo, true, hi, true);
                for (Map.Entry<Long, List<BufferEntry<L>>> e : wing.entrySet()) {
                    long tL = e.getKey();
                    for (BufferEntry<L> le : e.getValue()) {
                        emitJoin(le.getElement(), tL, rightElem, tR);
                        le.markJoined();
                    }
                }
            }
        }

        // Right wing: (tR + oldBeta, tR + newBeta]
        if (newBeta > oldBeta) {
            long lo = tR + oldBeta + 1;
            long hi = tR + newBeta;
            if (lo <= hi) {
                NavigableMap<Long, List<BufferEntry<L>>> wing =
                        leftForKey.subMap(lo, true, hi, true);
                for (Map.Entry<Long, List<BufferEntry<L>>> e : wing.entrySet()) {
                    long tL = e.getKey();
                    for (BufferEntry<L> le : e.getValue()) {
                        emitJoin(le.getElement(), tL, rightElem, tR);
                        le.markJoined();
                    }
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════
    //  EXPANSION LOGIC
    // ═══════════════════════════════════════════════════════

    private void ensureBounds(K key) {
        keyBounds.computeIfAbsent(key, k -> new long[]{ alpha0, beta0 });
    }

    /** @return true if at least one bound can still grow. */
    private boolean canExpand(long[] bounds) {
        return bounds[0] < alphaMax || bounds[1] < betaMax;
    }

    /** Perform one symmetric expansion step in-place. */
    private void expand(K key, long[] bounds) {
        bounds[0] = Math.min(bounds[0] + delta, alphaMax);
        bounds[1] = Math.min(bounds[1] + delta, betaMax);
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
    //  BUFFER / TIMER HELPERS
    // ═══════════════════════════════════════════════════════

    private <T> void addToBuffer(Map<K, TreeMap<Long, List<BufferEntry<T>>>> buffer,
                                 K key, long ts, BufferEntry<T> entry) {
        buffer.computeIfAbsent(key, k -> new TreeMap<>())
                .computeIfAbsent(ts, t -> new ArrayList<>())
                .add(entry);
    }

    private void registerTimer(Map<K, TreeSet<Long>> timerMap, K key, long time) {
        timerMap.computeIfAbsent(key, k -> new TreeSet<>()).add(time);
    }

    // ────────── cleanup callbacks ──────────

    /**
     * LEFT cleanup timer fires at {@code timerTs}.
     * The timer was registered as {@code tL + αK}, so the original
     * record timestamp is at most {@code timerTs} (when αK may be 0)
     * or {@code timerTs − αK_at_registration_time}.
     * <p>
     * Because αK can grow per-key we use a conservative strategy:
     * remove all left entries with timestamp ≤ timerTs − alpha(key)
     * at fire time.  Since α only grows, this is safe.
     */
    private void onLeftCleanupTimer(K key, long timerTs) {
        TreeMap<Long, List<BufferEntry<L>>> map = leftBuffer.get(key);
        if (map != null) {
            // Remove entries at timestamps ≤ timerTs that can no longer be matched.
            // A left entry at tL was registered with cleanup = tL + αK.
            // When timerTs fires, tL = timerTs − αK_at_registration.  We don't store
            // αK_at_registration, so we remove the exact timestamp bucket:
            // timerTs was tL + α → tL = timerTs - getCurrentAlpha(key)
            // But α might have grown since then.  Safe approach: remove head entries
            // whose cleanup time ≤ now.
            // Simple and correct: remove the bucket at timerTs − currentAlpha.
            // If α grew since registration the bucket may already have been removed.
            long[] bounds = keyBounds.getOrDefault(key, new long[]{ alpha0, beta0 });
            long tsToRemove = timerTs - bounds[0];
            map.remove(tsToRemove);
            if (map.isEmpty()) leftBuffer.remove(key);
        }
        log.debug("LEFT cleanup key=" + key + " timerTs=" + timerTs);
    }

    /**
     * RIGHT cleanup timer fires at {@code timerTs}.
     */
    private void onRightCleanupTimer(K key, long timerTs) {
        TreeMap<Long, List<BufferEntry<R>>> map = rightBuffer.get(key);
        if (map != null) {
            long[] bounds = keyBounds.getOrDefault(key, new long[]{ alpha0, beta0 });
            long tsToRemove = timerTs - bounds[1];
            map.remove(tsToRemove);
            if (map.isEmpty()) rightBuffer.remove(key);
        }
        log.debug("RIGHT cleanup key=" + key + " timerTs=" + timerTs);
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

    /** Drain the expansion event log. */
    public List<ExpansionEvent<K>> drainExpansionLog() {
        List<ExpansionEvent<K>> r = new ArrayList<>(expansionLog);
        expansionLog.clear();
        return r;
    }

    /** Peek (non-destructive) at the expansion log. */
    public List<ExpansionEvent<K>> peekExpansionLog() {
        return Collections.unmodifiableList(expansionLog);
    }

    // ═══════════════════════════════════════════════════════
    //  INTROSPECTION
    // ═══════════════════════════════════════════════════════

    public long getCurrentWatermark() { return currentWatermark; }
    public long getAlpha0()           { return alpha0; }
    public long getBeta0()            { return beta0; }
    public long getAlphaMax()         { return alphaMax; }
    public long getBetaMax()          { return betaMax; }
    public long getDelta()            { return delta; }

    /** Current αK for a given key (returns α₀ if key not yet seen). */
    public long getAlpha(K key) {
        long[] b = keyBounds.get(key);
        return b != null ? b[0] : alpha0;
    }

    /** Current βK for a given key (returns β₀ if key not yet seen). */
    public long getBeta(K key) {
        long[] b = keyBounds.get(key);
        return b != null ? b[1] : beta0;
    }

    /** Total entries in the left buffer across all keys. */
    public int getLeftBufferSize() {
        return leftBuffer.values().stream()
                .flatMap(tm -> tm.values().stream())
                .mapToInt(List::size).sum();
    }

    /** Total entries in the right buffer across all keys. */
    public int getRightBufferSize() {
        return rightBuffer.values().stream()
                .flatMap(tm -> tm.values().stream())
                .mapToInt(List::size).sum();
    }

    /** Distinct keys in the left buffer. */
    public int getLeftKeyCount() { return leftBuffer.size(); }

    /** Distinct keys in the right buffer. */
    public int getRightKeyCount() { return rightBuffer.size(); }

    /** Pending left cleanup timers across all keys. */
    public int getLeftTimerCount() {
        return leftCleanupTimers.values().stream().mapToInt(Set::size).sum();
    }

    /** Pending right cleanup timers across all keys. */
    public int getRightTimerCount() {
        return rightCleanupTimers.values().stream().mapToInt(Set::size).sum();
    }

    /** Number of keys that have been expanded at least once. */
    public long getExpandedKeyCount() {
        return keyBounds.entrySet().stream()
                .filter(e -> e.getValue()[0] > alpha0 || e.getValue()[1] > beta0)
                .count();
    }

    @Override
    public String toString() {
        return "SymmetricExpandingIntervalJoinOperator{" +
                "α₀=" + alpha0 + ", β₀=" + beta0 +
                ", αmax=" + alphaMax + ", βmax=" + betaMax +
                ", Δ=" + delta +
                ", leftBuf=" + getLeftBufferSize() +
                ", rightBuf=" + getRightBufferSize() +
                ", expandedKeys=" + getExpandedKeyCount() +
                '}';
    }
}
