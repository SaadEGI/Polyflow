package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.apache.log4j.Logger;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin.*;

import java.util.*;
import java.util.function.Function;

/**
 * Expand-then-Shrink Interval Join Operator for Polyflow.
 *
 * <h3>Policy: expand until match, then shrink to minimal window</h3>
 * <ol>
 *   <li>Start with base bounds [tL − β₀, tL + α₀].</li>
 *   <li>Probe the opposite buffer.  If <b>zero</b> candidates → expand
 *       symmetrically by Δ (up to αmax/βmax) and re-probe, repeating until
 *       at least one candidate exists or max bounds are reached.</li>
 *   <li>If <b>multiple</b> candidates are found → <b>shrink</b> both bounds
 *       by Δ per step (down to α₀/β₀) and re-count.  Stop shrinking when
 *       either exactly one candidate remains, or α₀/β₀ is hit.</li>
 *   <li>Emit all candidates that fall within the <em>final</em> (post-shrink)
 *       bounds.</li>
 * </ol>
 *
 * <h3>Intuition</h3>
 * Compared to the pure expanding policy, expand-then-shrink <b>reduces false
 * positives</b> (fewer spurious matches emitted from overly wide windows)
 * while still <b>recovering late/early events</b> that fall outside the
 * base window.  The tradeoff is that shrinking may occasionally drop a
 * correct match if it appears alongside noise in the same expanded zone.
 *
 * <h3>Window for a left record L at tL (after adapt)</h3>
 * <pre>
 *   [tL − βK_final , tL + αK_final]
 * </pre>
 *
 * <h3>Output timestamp</h3>
 * <pre>max(tL , tR)</pre>
 *
 * <h3>Late data</h3>
 * Records with event-time below the current watermark are side-outputted.
 *
 * @param <K>   key type
 * @param <L>   left stream element type
 * @param <R>   right stream element type
 * @param <OUT> output element type
 */
public class ExpandShrinkIntervalJoinOperator<K, L, R, OUT> {

    private static final Logger log = Logger.getLogger(ExpandShrinkIntervalJoinOperator.class);

    // ────────── configuration ──────────
    private final long alpha0;          // initial / minimum future bound
    private final long beta0;           // initial / minimum past bound
    private final long alphaMax;        // maximum future bound
    private final long betaMax;         // maximum past bound
    private final long delta;           // expansion / shrink step

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
    private final Map<K, TreeMap<Long, List<BufferEntry<L>>>> leftBuffer = new HashMap<>();
    private final Map<K, TreeMap<Long, List<BufferEntry<R>>>> rightBuffer = new HashMap<>();

    // ────────── event-time cleanup timers ──────────
    private final Map<K, TreeSet<Long>> leftCleanupTimers = new HashMap<>();
    private final Map<K, TreeSet<Long>> rightCleanupTimers = new HashMap<>();

    // ────────── watermark ──────────
    private long currentWatermark = Long.MIN_VALUE;

    // ────────── output accumulation ──────────
    private final List<TimestampedOutput<OUT>> outputRecords = new ArrayList<>();
    private final List<SideOutputRecord<?>> sideOutputRecords = new ArrayList<>();

    // ────────── observability logs ──────────
    private final List<AdaptationEvent<K>> adaptationLog = new ArrayList<>();

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

    /** Records every expand / shrink adaptation for observability. */
    public static final class AdaptationEvent<K> {
        private final K key;
        private final long startAlpha, startBeta;
        private final long peakAlpha, peakBeta;      // after expand phase
        private final long finalAlpha, finalBeta;     // after shrink phase
        private final int expandSteps, shrinkSteps;
        private final int candidatesAtPeak, candidatesAtFinal;
        private final String triggerSide;
        private final long recordTimestamp;

        public AdaptationEvent(K key,
                               long startAlpha, long startBeta,
                               long peakAlpha, long peakBeta,
                               long finalAlpha, long finalBeta,
                               int expandSteps, int shrinkSteps,
                               int candidatesAtPeak, int candidatesAtFinal,
                               String triggerSide, long recordTimestamp) {
            this.key = key;
            this.startAlpha = startAlpha;
            this.startBeta = startBeta;
            this.peakAlpha = peakAlpha;
            this.peakBeta = peakBeta;
            this.finalAlpha = finalAlpha;
            this.finalBeta = finalBeta;
            this.expandSteps = expandSteps;
            this.shrinkSteps = shrinkSteps;
            this.candidatesAtPeak = candidatesAtPeak;
            this.candidatesAtFinal = candidatesAtFinal;
            this.triggerSide = triggerSide;
            this.recordTimestamp = recordTimestamp;
        }

        public K getKey()                  { return key; }
        public long getStartAlpha()        { return startAlpha; }
        public long getStartBeta()         { return startBeta; }
        public long getPeakAlpha()         { return peakAlpha; }
        public long getPeakBeta()          { return peakBeta; }
        public long getFinalAlpha()        { return finalAlpha; }
        public long getFinalBeta()         { return finalBeta; }
        public int getExpandSteps()        { return expandSteps; }
        public int getShrinkSteps()        { return shrinkSteps; }
        public int getCandidatesAtPeak()   { return candidatesAtPeak; }
        public int getCandidatesAtFinal()  { return candidatesAtFinal; }
        public String getTriggerSide()     { return triggerSide; }
        public long getRecordTimestamp()   { return recordTimestamp; }

        @Override public String toString() {
            return "Adapt{key=" + key +
                    " α:" + startAlpha + "→" + peakAlpha + "→" + finalAlpha +
                    " β:" + startBeta  + "→" + peakBeta  + "→" + finalBeta +
                    " expand=" + expandSteps + " shrink=" + shrinkSteps +
                    " cands:" + candidatesAtPeak + "→" + candidatesAtFinal +
                    " side=" + triggerSide + " ts=" + recordTimestamp + '}';
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

        public Builder<K, L, R, OUT> alpha0(long v)    { this.alpha0 = v; return this; }
        public Builder<K, L, R, OUT> beta0(long v)     { this.beta0 = v; return this; }
        public Builder<K, L, R, OUT> alphaMax(long v)  { this.alphaMax = v; return this; }
        public Builder<K, L, R, OUT> betaMax(long v)   { this.betaMax = v; return this; }
        public Builder<K, L, R, OUT> delta(long v)     { this.delta = v; return this; }
        public Builder<K, L, R, OUT> leftLateDataOutputTag(String t)  { this.leftLateTag = t; return this; }
        public Builder<K, L, R, OUT> rightLateDataOutputTag(String t) { this.rightLateTag = t; return this; }
        public Builder<K, L, R, OUT> leftKeyExtractor(Function<L, K> fn)  { this.leftKeyExtractor = fn; return this; }
        public Builder<K, L, R, OUT> rightKeyExtractor(Function<R, K> fn) { this.rightKeyExtractor = fn; return this; }
        public Builder<K, L, R, OUT> processJoinFunction(ProcessJoinFunction<L, R, OUT> fn) { this.joinFunction = fn; return this; }

        public ExpandShrinkIntervalJoinOperator<K, L, R, OUT> build() {
            Objects.requireNonNull(leftKeyExtractor,  "leftKeyExtractor");
            Objects.requireNonNull(rightKeyExtractor, "rightKeyExtractor");
            Objects.requireNonNull(joinFunction,      "processJoinFunction");
            if (alpha0 < 0) throw new IllegalArgumentException("alpha0 must be ≥ 0");
            if (beta0  < 0) throw new IllegalArgumentException("beta0 must be ≥ 0");
            if (delta  < 0) throw new IllegalArgumentException("delta must be ≥ 0");
            if (alphaMax < alpha0) throw new IllegalArgumentException("alphaMax < alpha0");
            if (betaMax  < beta0)  throw new IllegalArgumentException("betaMax < beta0");

            return new ExpandShrinkIntervalJoinOperator<>(
                    alpha0, beta0, alphaMax, betaMax, delta,
                    leftLateTag, rightLateTag,
                    leftKeyExtractor, rightKeyExtractor, joinFunction);
        }
    }

    // ────────── constructor ──────────

    private ExpandShrinkIntervalJoinOperator(
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
                if (isLeft) onLeftCleanupTimer(key, timerTs);
                else        onRightCleanupTimer(key, timerTs);
            }
            fired.clear();
            if (timers.isEmpty()) timerMap.remove(key);
        }
    }

    // ═══════════════════════════════════════════════════════
    //  PROCESS LEFT — expand-then-shrink probing right buffer
    // ═══════════════════════════════════════════════════════

    public void processLeft(L element, long tL) {
        if (tL == Long.MIN_VALUE)
            throw new IllegalArgumentException("Timestamp must not be Long.MIN_VALUE.");

        K key = leftKeyExtractor.apply(element);

        if (tL < currentWatermark) {
            sideOutputRecords.add(new SideOutputRecord<>(leftLateDataOutputTag, element));
            return;
        }

        ensureBounds(key);
        long[] bounds = keyBounds.get(key);  // [alpha, beta]

        // 1. Buffer the left record
        addToBuffer(leftBuffer, key, tL, new BufferEntry<>(element, false));

        // 2. Expand-then-shrink: determine final α/β, then emit
        long startAlpha = bounds[0], startBeta = bounds[1];
        int expandSteps = 0, shrinkSteps = 0;

        // ── Phase 1: EXPAND until ≥1 candidate ──
        int candidateCount = countRightCandidates(key, tL, bounds[1], bounds[0]);
        while (candidateCount == 0 && canExpand(bounds)) {
            bounds[0] = Math.min(bounds[0] + delta, alphaMax);
            bounds[1] = Math.min(bounds[1] + delta, betaMax);
            expandSteps++;
            candidateCount = countRightCandidates(key, tL, bounds[1], bounds[0]);
        }
        int candidatesAtPeak = candidateCount;
        long peakAlpha = bounds[0], peakBeta = bounds[1];

        // ── Phase 2: SHRINK while >1 candidate and above minimum ──
        while (candidateCount > 1 && canShrink(bounds)) {
            long trialAlpha = Math.max(bounds[0] - delta, alpha0);
            long trialBeta  = Math.max(bounds[1] - delta, beta0);
            int trialCount = countRightCandidates(key, tL, trialBeta, trialAlpha);
            if (trialCount == 0) {
                // Shrinking too far would lose all candidates → stop
                break;
            }
            bounds[0] = trialAlpha;
            bounds[1] = trialBeta;
            candidateCount = trialCount;
            shrinkSteps++;
        }
        int candidatesAtFinal = candidateCount;

        // Log adaptation if any expand or shrink happened
        if (expandSteps > 0 || shrinkSteps > 0) {
            adaptationLog.add(new AdaptationEvent<>(key,
                    startAlpha, startBeta,
                    peakAlpha, peakBeta,
                    bounds[0], bounds[1],
                    expandSteps, shrinkSteps,
                    candidatesAtPeak, candidatesAtFinal,
                    "LEFT", tL));
        }

        // 3. Emit matches within final bounds
        emitRightCandidates(element, key, tL, bounds[1], bounds[0]);

        // 4. Cleanup timer based on final bounds
        registerTimer(leftCleanupTimers, key, tL + bounds[0]);
    }

    // ═══════════════════════════════════════════════════════
    //  PROCESS RIGHT — expand-then-shrink probing left buffer
    // ═══════════════════════════════════════════════════════

    public void processRight(R element, long tR) {
        if (tR == Long.MIN_VALUE)
            throw new IllegalArgumentException("Timestamp must not be Long.MIN_VALUE.");

        K key = rightKeyExtractor.apply(element);

        if (tR < currentWatermark) {
            sideOutputRecords.add(new SideOutputRecord<>(rightLateDataOutputTag, element));
            return;
        }

        ensureBounds(key);
        long[] bounds = keyBounds.get(key);  // [alpha, beta]

        // 1. Buffer the right record
        addToBuffer(rightBuffer, key, tR, new BufferEntry<>(element, false));

        // 2. Expand-then-shrink: determine final α/β, then emit
        long startAlpha = bounds[0], startBeta = bounds[1];
        int expandSteps = 0, shrinkSteps = 0;

        // ── Phase 1: EXPAND until ≥1 candidate ──
        // For right probing left: tL ∈ [tR − alpha, tR + beta]
        int candidateCount = countLeftCandidates(key, tR, bounds[0], bounds[1]);
        while (candidateCount == 0 && canExpand(bounds)) {
            bounds[0] = Math.min(bounds[0] + delta, alphaMax);
            bounds[1] = Math.min(bounds[1] + delta, betaMax);
            expandSteps++;
            candidateCount = countLeftCandidates(key, tR, bounds[0], bounds[1]);
        }
        int candidatesAtPeak = candidateCount;
        long peakAlpha = bounds[0], peakBeta = bounds[1];

        // ── Phase 2: SHRINK while >1 candidate and above minimum ──
        while (candidateCount > 1 && canShrink(bounds)) {
            long trialAlpha = Math.max(bounds[0] - delta, alpha0);
            long trialBeta  = Math.max(bounds[1] - delta, beta0);
            int trialCount = countLeftCandidates(key, tR, trialAlpha, trialBeta);
            if (trialCount == 0) {
                break;
            }
            bounds[0] = trialAlpha;
            bounds[1] = trialBeta;
            candidateCount = trialCount;
            shrinkSteps++;
        }
        int candidatesAtFinal = candidateCount;

        if (expandSteps > 0 || shrinkSteps > 0) {
            adaptationLog.add(new AdaptationEvent<>(key,
                    startAlpha, startBeta,
                    peakAlpha, peakBeta,
                    bounds[0], bounds[1],
                    expandSteps, shrinkSteps,
                    candidatesAtPeak, candidatesAtFinal,
                    "RIGHT", tR));
        }

        // 3. Emit matches within final bounds
        emitLeftCandidates(element, key, tR, bounds[0], bounds[1]);

        // 4. Cleanup timer based on final bounds
        registerTimer(rightCleanupTimers, key, tR + bounds[1]);
    }

    // ═══════════════════════════════════════════════════════
    //  COUNT HELPERS (non-emitting probes)
    // ═══════════════════════════════════════════════════════

    /**
     * Count right-buffer candidates for left record at tL with bounds
     * [tL − beta, tL + alpha].  Does NOT emit.
     */
    private int countRightCandidates(K key, long tL, long beta, long alpha) {
        TreeMap<Long, List<BufferEntry<R>>> rightForKey = rightBuffer.get(key);
        if (rightForKey == null || rightForKey.isEmpty()) return 0;

        long lo = tL - beta;
        long hi = tL + alpha;
        int count = 0;
        for (List<BufferEntry<R>> entries : rightForKey.subMap(lo, true, hi, true).values()) {
            count += entries.size();
        }
        return count;
    }

    /**
     * Count left-buffer candidates for right record at tR with bounds
     * [tR − alpha, tR + beta].  Does NOT emit.
     */
    private int countLeftCandidates(K key, long tR, long alpha, long beta) {
        TreeMap<Long, List<BufferEntry<L>>> leftForKey = leftBuffer.get(key);
        if (leftForKey == null || leftForKey.isEmpty()) return 0;

        long lo = tR - alpha;
        long hi = tR + beta;
        int count = 0;
        for (List<BufferEntry<L>> entries : leftForKey.subMap(lo, true, hi, true).values()) {
            count += entries.size();
        }
        return count;
    }

    // ═══════════════════════════════════════════════════════
    //  EMIT HELPERS (emitting probes using final bounds)
    // ═══════════════════════════════════════════════════════

    /**
     * Emit all right-buffer matches for left record at tL within
     * [tL − beta, tL + alpha].
     */
    private void emitRightCandidates(L leftElem, K key, long tL, long beta, long alpha) {
        TreeMap<Long, List<BufferEntry<R>>> rightForKey = rightBuffer.get(key);
        if (rightForKey == null || rightForKey.isEmpty()) return;

        long lo = tL - beta;
        long hi = tL + alpha;
        for (Map.Entry<Long, List<BufferEntry<R>>> tsEntry :
                rightForKey.subMap(lo, true, hi, true).entrySet()) {
            long tR = tsEntry.getKey();
            for (BufferEntry<R> re : tsEntry.getValue()) {
                emitJoin(leftElem, tL, re.getElement(), tR);
                re.markJoined();
            }
        }
    }

    /**
     * Emit all left-buffer matches for right record at tR within
     * [tR − alpha, tR + beta].
     */
    private void emitLeftCandidates(R rightElem, K key, long tR, long alpha, long beta) {
        TreeMap<Long, List<BufferEntry<L>>> leftForKey = leftBuffer.get(key);
        if (leftForKey == null || leftForKey.isEmpty()) return;

        long lo = tR - alpha;
        long hi = tR + beta;
        for (Map.Entry<Long, List<BufferEntry<L>>> tsEntry :
                leftForKey.subMap(lo, true, hi, true).entrySet()) {
            long tL = tsEntry.getKey();
            for (BufferEntry<L> le : tsEntry.getValue()) {
                emitJoin(le.getElement(), tL, rightElem, tR);
                le.markJoined();
            }
        }
    }

    // ═══════════════════════════════════════════════════════
    //  BOUNDS LOGIC
    // ═══════════════════════════════════════════════════════

    private void ensureBounds(K key) {
        keyBounds.computeIfAbsent(key, k -> new long[]{ alpha0, beta0 });
    }

    private boolean canExpand(long[] bounds) {
        return bounds[0] < alphaMax || bounds[1] < betaMax;
    }

    private boolean canShrink(long[] bounds) {
        return bounds[0] > alpha0 || bounds[1] > beta0;
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

    private void onLeftCleanupTimer(K key, long timerTs) {
        TreeMap<Long, List<BufferEntry<L>>> map = leftBuffer.get(key);
        if (map != null) {
            long[] bounds = keyBounds.getOrDefault(key, new long[]{ alpha0, beta0 });
            long tsToRemove = timerTs - bounds[0];
            map.remove(tsToRemove);
            if (map.isEmpty()) leftBuffer.remove(key);
        }
    }

    private void onRightCleanupTimer(K key, long timerTs) {
        TreeMap<Long, List<BufferEntry<R>>> map = rightBuffer.get(key);
        if (map != null) {
            long[] bounds = keyBounds.getOrDefault(key, new long[]{ alpha0, beta0 });
            long tsToRemove = timerTs - bounds[1];
            map.remove(tsToRemove);
            if (map.isEmpty()) rightBuffer.remove(key);
        }
    }

    // ═══════════════════════════════════════════════════════
    //  OUTPUT ACCESS
    // ═══════════════════════════════════════════════════════

    public List<TimestampedOutput<OUT>> drainOutputs() {
        List<TimestampedOutput<OUT>> r = new ArrayList<>(outputRecords);
        outputRecords.clear();
        return r;
    }

    public List<SideOutputRecord<?>> drainSideOutputs() {
        List<SideOutputRecord<?>> r = new ArrayList<>(sideOutputRecords);
        sideOutputRecords.clear();
        return r;
    }

    public List<TimestampedOutput<OUT>> peekOutputs() {
        return Collections.unmodifiableList(outputRecords);
    }

    public List<SideOutputRecord<?>> peekSideOutputs() {
        return Collections.unmodifiableList(sideOutputRecords);
    }

    public List<AdaptationEvent<K>> drainAdaptationLog() {
        List<AdaptationEvent<K>> r = new ArrayList<>(adaptationLog);
        adaptationLog.clear();
        return r;
    }

    public List<AdaptationEvent<K>> peekAdaptationLog() {
        return Collections.unmodifiableList(adaptationLog);
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

    public long getAlpha(K key) {
        long[] b = keyBounds.get(key);
        return b != null ? b[0] : alpha0;
    }

    public long getBeta(K key) {
        long[] b = keyBounds.get(key);
        return b != null ? b[1] : beta0;
    }

    public int getLeftBufferSize() {
        return leftBuffer.values().stream()
                .flatMap(tm -> tm.values().stream())
                .mapToInt(List::size).sum();
    }

    public int getRightBufferSize() {
        return rightBuffer.values().stream()
                .flatMap(tm -> tm.values().stream())
                .mapToInt(List::size).sum();
    }

    public int getLeftKeyCount()  { return leftBuffer.size(); }
    public int getRightKeyCount() { return rightBuffer.size(); }

    public int getLeftTimerCount() {
        return leftCleanupTimers.values().stream().mapToInt(Set::size).sum();
    }

    public int getRightTimerCount() {
        return rightCleanupTimers.values().stream().mapToInt(Set::size).sum();
    }

    /** Number of keys whose bounds differ from the initial (α₀, β₀). */
    public long getAdaptedKeyCount() {
        return keyBounds.entrySet().stream()
                .filter(e -> e.getValue()[0] != alpha0 || e.getValue()[1] != beta0)
                .count();
    }

    /** Total number of adaptation events logged (expand + shrink cycles). */
    public int getTotalAdaptations() {
        return adaptationLog.size();
    }

    /** Total expand steps across all adaptations. */
    public long getTotalExpandSteps() {
        return adaptationLog.stream().mapToInt(AdaptationEvent::getExpandSteps).sum();
    }

    /** Total shrink steps across all adaptations. */
    public long getTotalShrinkSteps() {
        return adaptationLog.stream().mapToInt(AdaptationEvent::getShrinkSteps).sum();
    }

    @Override
    public String toString() {
        return "ExpandShrinkIntervalJoinOperator{" +
                "α₀=" + alpha0 + ", β₀=" + beta0 +
                ", αmax=" + alphaMax + ", βmax=" + betaMax +
                ", Δ=" + delta +
                ", leftBuf=" + getLeftBufferSize() +
                ", rightBuf=" + getRightBufferSize() +
                ", adaptedKeys=" + getAdaptedKeyCount() +
                '}';
    }
}
