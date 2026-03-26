package benchmark.twostreameval;

import benchmark.twostreameval.TwoStreamDatasetGenerator.*;

import org.streamreasoning.polyflow.base.operatorsimpl.s2r.ExpandShrinkIntervalJoinOperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.FlinkIntervalJoinOperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.SlidingWindowJoinOperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.SymmetricExpandingIntervalJoinOperator;

import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

import static benchmark.twostreameval.TwoStreamDatasetGenerator.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Two-Stream Ground Truth Benchmark: Fixed Interval Join vs Symmetric Expanding.
 *
 * <h2>Streams</h2>
 * <pre>
 *   Left  = Orders(orderId, tO)
 *   Right = Payments(paymentId, orderId, tP)
 *   Key   = orderId
 * </pre>
 *
 * <h2>Pipelines</h2>
 * <pre>
 *   A) FixedIntervalJoin          window = [tO − β0, tO + α0]
 *   B) SymmetricExpandingJoin     starts [tO − β0, tO + α0], expands Δ on miss
 *   C) SlidingWindowJoin          sliding window of size W, slide S; cross-join per key within window
 *   D) ExpandShrinkJoin           expand until ≥1 match, then shrink to minimal window
 * </pre>
 *
 * <h2>Metrics</h2>
 * TP, FP, FN, Duplicates, Precision, Recall, F1, Latency, per-category recall.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TwoStreamBenchmarkTest {

    // ==================== Scale Configuration ====================
    /**
     * Number of times the base dataset chunk (110 orders, ~163 payments) is
     * repeated.  Adjust to reach the desired runtime.
     *
     * <pre>
     *   1       → ~270 events   (milliseconds — unit-test sanity check)
     *   1 000   → ~270 K events (seconds)
     *   10 000  → ~2.7 M events (minutes — thesis benchmark target)
     * </pre>
     */
    private static final int SCALE_FACTOR = 10_000;

    // ==================== Shared State ====================
    private static TwoStreamDatasetGenerator gen;
    private static List<Object[]> ingestion;   // merged event-time ingestion order

    private static List<EmittedMatch> fixedResults;
    private static List<EmittedMatch> expandingResults;
    private static List<EmittedMatch> slidingResults;
    private static List<EmittedMatch> expandShrinkResults;

    /** Wall-clock elapsed time in ms for each pipeline. */
    private static long fixedElapsedMs, expandingElapsedMs, slidingElapsedMs, expandShrinkElapsedMs;

    // ==================== Sliding Window Configuration ====================
    /**
     * Sliding window join parameters.
     * <p>
     * Window size = ALPHA_0 + BETA_0 = 40 (comparable span to fixed interval join).
     * Slide = half the window for overlap = 20.
     */
    private static final long SLIDING_WINDOW_SIZE = ALPHA_0 + BETA_0;  // 40
    private static final long SLIDING_SLIDE_SIZE  = SLIDING_WINDOW_SIZE / 2;  // 20

    // ==================== Emitted Match Record ====================
    /** One output row from an operator. */
    static class EmittedMatch {
        final String orderId;
        final String paymentId;
        final long orderTime;
        final long paymentTime;
        final long emitTime;          // event-time output: max(tO, tP)
        final long streamProgress;    // high-water-mark of all event-times seen at emit time

        EmittedMatch(String orderId, String paymentId, long orderTime, long paymentTime,
                     long emitTime, long streamProgress) {
            this.orderId = orderId;
            this.paymentId = paymentId;
            this.orderTime = orderTime;
            this.paymentTime = paymentTime;
            this.emitTime = emitTime;
            this.streamProgress = streamProgress;
        }

        String matchKey() { return orderId + "|" + paymentId; }

        @Override public String toString() {
            return orderId + "+" + paymentId + " @emit=" + emitTime + " @progress=" + streamProgress;
        }
    }

    // ==================== Joined Output Tuple ====================
    /** Carried through the ProcessJoinFunction. */
    static class JoinedTuple {
        final String orderId;
        final String paymentId;
        final long tO;
        final long tP;

        JoinedTuple(String orderId, String paymentId, long tO, long tP) {
            this.orderId = orderId;
            this.paymentId = paymentId;
            this.tO = tO;
            this.tP = tP;
        }

        @Override public String toString() {
            return "Joined{" + orderId + "+" + paymentId + " tO=" + tO + " tP=" + tP + "}";
        }
    }

    // ==================== 0) Generate Dataset ====================

    @BeforeAll
    static void generateDataset() throws IOException {
        System.out.println("\n" + "═".repeat(80));
        System.out.println("  TWO-STREAM BENCHMARK: Fixed Interval Join  vs  Symmetric Expanding");
        System.out.printf( "  Scale factor: %,d  (base chunk × %,d)%n", SCALE_FACTOR, SCALE_FACTOR);
        System.out.println("═".repeat(80));

        gen = new TwoStreamDatasetGenerator();
        gen.generate(SCALE_FACTOR);
        ingestion = gen.buildIngestionOrder();

        // Export (samples only at large scale)
        Path outDir = Path.of("benchmark_dataset", "two_stream_eval");
        gen.writeToCsv(outDir);

        // Sanity checks (scaled)
        long expectedOrders  = 110L * SCALE_FACTOR;
        long expectedPayments = 160L * SCALE_FACTOR;  // conservative lower bound
        assertTrue(gen.getOrders().size() >= expectedOrders,
                String.format("Need ≥%,d orders, got %,d", expectedOrders, gen.getOrders().size()));
        assertTrue(gen.getPayments().size() >= expectedPayments,
                String.format("Need ≥%,d payments, got %,d", expectedPayments, gen.getPayments().size()));
    }

    // ==================== 1) Pipeline A: Fixed Interval Join ====================

    @Test @Order(1)
    @DisplayName("Pipeline A — Fixed Interval Join")
    void runFixedIntervalJoin() {
        System.out.println("\n══════ PIPELINE A: Fixed Interval Join ══════");
        System.out.printf( "  window = [tO − %d, tO + %d]%n", BETA_0, ALPHA_0);
        System.out.printf( "  Ingesting %,d events ...%n", ingestion.size());

        FlinkIntervalJoinOperator<String, OrderEvent, PaymentEvent, JoinedTuple> op =
            FlinkIntervalJoinOperator.<String, OrderEvent, PaymentEvent, JoinedTuple>builder()
                .lowerBound(-BETA_0)     // negative = look into the past
                .upperBound(ALPHA_0)
                .leftKeyExtractor(o -> o.orderId)
                .rightKeyExtractor(p -> p.orderId)
                .processJoinFunction((left, right, ctx, collector) ->
                    collector.collect(
                        new JoinedTuple(left.orderId, right.paymentId, left.tO, right.tP),
                        ctx.getTimestamp()))
                .build();

        long t0 = System.currentTimeMillis();
        fixedResults = feedAndCollect(op);
        fixedElapsedMs = System.currentTimeMillis() - t0;

        double secs = fixedElapsedMs / 1000.0;
        double throughput = ingestion.size() / secs;
        System.out.printf( "  Output rows:  %,d%n", fixedResults.size());
        System.out.printf( "  Elapsed:      %.1f s%n", secs);
        System.out.printf( "  Throughput:   %,.0f events/s%n", throughput);
    }

    // ==================== 2) Pipeline B: Symmetric Expanding Join ====================

    @Test @Order(2)
    @DisplayName("Pipeline B — Symmetric Expanding Interval Join")
    void runExpandingIntervalJoin() {
        System.out.println("\n══════ PIPELINE B: Symmetric Expanding Interval Join ══════");
        System.out.printf( "  α₀=%d β₀=%d Δ=%d αmax=%d βmax=%d%n",
                ALPHA_0, BETA_0, DELTA, ALPHA_MAX, BETA_MAX);
        System.out.printf( "  Ingesting %,d events ...%n", ingestion.size());

        SymmetricExpandingIntervalJoinOperator<String, OrderEvent, PaymentEvent, JoinedTuple> op =
            SymmetricExpandingIntervalJoinOperator.<String, OrderEvent, PaymentEvent, JoinedTuple>builder()
                .alpha0(ALPHA_0)
                .beta0(BETA_0)
                .delta(DELTA)
                .alphaMax(ALPHA_MAX)
                .betaMax(BETA_MAX)
                .leftKeyExtractor(o -> o.orderId)
                .rightKeyExtractor(p -> p.orderId)
                .processJoinFunction((left, right, ctx, collector) ->
                    collector.collect(
                        new JoinedTuple(left.orderId, right.paymentId, left.tO, right.tP),
                        ctx.getTimestamp()))
                .build();

        long t0 = System.currentTimeMillis();
        expandingResults = feedAndCollect(op);
        expandingElapsedMs = System.currentTimeMillis() - t0;

        double secs = expandingElapsedMs / 1000.0;
        double throughput = ingestion.size() / secs;
        System.out.printf( "  Output rows:  %,d%n", expandingResults.size());
        System.out.printf( "  Elapsed:      %.1f s%n", secs);
        System.out.printf( "  Throughput:   %,.0f events/s%n", throughput);

        // expansion summary
        var exps = op.peekExpansionLog();
        System.out.printf( "  Expansions triggered: %,d%n", exps.size());
        System.out.printf( "  Expanded keys:        %,d%n", op.getExpandedKeyCount());
    }

    // ==================== 3) Pipeline C: Sliding Window Join ====================

    @Test @Order(3)
    @DisplayName("Pipeline C — Sliding Window Join")
    void runSlidingWindowJoin() {
        System.out.println("\n══════ PIPELINE C: Sliding Window Join ══════");
        System.out.printf( "  windowSize=%d  slideSize=%d%n", SLIDING_WINDOW_SIZE, SLIDING_SLIDE_SIZE);
        System.out.printf( "  Ingesting %,d events ...%n", ingestion.size());

        SlidingWindowJoinOperator<String, OrderEvent, PaymentEvent, JoinedTuple> op =
            SlidingWindowJoinOperator.<String, OrderEvent, PaymentEvent, JoinedTuple>builder()
                .windowSize(SLIDING_WINDOW_SIZE)
                .slideSize(SLIDING_SLIDE_SIZE)
                .allowedLateness(0)
                .leftKeyExtractor(o -> o.orderId)
                .rightKeyExtractor(p -> p.orderId)
                .processJoinFunction((left, right, ctx, collector) ->
                    collector.collect(
                        new JoinedTuple(left.orderId, right.paymentId, left.tO, right.tP),
                        ctx.getTimestamp()))
                .build();

        long t0 = System.currentTimeMillis();
        slidingResults = feedAndCollect(op);
        slidingElapsedMs = System.currentTimeMillis() - t0;

        double secs = slidingElapsedMs / 1000.0;
        double throughput = ingestion.size() / secs;
        System.out.printf( "  Output rows:  %,d%n", slidingResults.size());
        System.out.printf( "  Elapsed:      %.1f s%n", secs);
        System.out.printf( "  Throughput:   %,.0f events/s%n", throughput);
        System.out.printf( "  Windows fired: %,d%n", op.getWindowsFired());
    }

    // ==================== 4) Pipeline D: Expand-then-Shrink Join ====================

    @Test @Order(4)
    @DisplayName("Pipeline D — Expand-then-Shrink Interval Join")
    void runExpandShrinkIntervalJoin() {
        System.out.println("\n══════ PIPELINE D: Expand-then-Shrink Interval Join ══════");
        System.out.printf( "  α₀=%d β₀=%d Δ=%d αmax=%d βmax=%d%n",
                ALPHA_0, BETA_0, DELTA, ALPHA_MAX, BETA_MAX);
        System.out.println("  Policy: expand until ≥1 match → shrink to minimal window");
        System.out.printf( "  Ingesting %,d events ...%n", ingestion.size());

        ExpandShrinkIntervalJoinOperator<String, OrderEvent, PaymentEvent, JoinedTuple> op =
            ExpandShrinkIntervalJoinOperator.<String, OrderEvent, PaymentEvent, JoinedTuple>builder()
                .alpha0(ALPHA_0)
                .beta0(BETA_0)
                .delta(DELTA)
                .alphaMax(ALPHA_MAX)
                .betaMax(BETA_MAX)
                .leftKeyExtractor(o -> o.orderId)
                .rightKeyExtractor(p -> p.orderId)
                .processJoinFunction((left, right, ctx, collector) ->
                    collector.collect(
                        new JoinedTuple(left.orderId, right.paymentId, left.tO, right.tP),
                        ctx.getTimestamp()))
                .build();

        long t0 = System.currentTimeMillis();
        expandShrinkResults = feedAndCollect(op);
        expandShrinkElapsedMs = System.currentTimeMillis() - t0;

        double secs = expandShrinkElapsedMs / 1000.0;
        double throughput = ingestion.size() / secs;
        System.out.printf( "  Output rows:  %,d%n", expandShrinkResults.size());
        System.out.printf( "  Elapsed:      %.1f s%n", secs);
        System.out.printf( "  Throughput:   %,.0f events/s%n", throughput);

        // Adaptation summary
        var adaptations = op.peekAdaptationLog();
        System.out.printf( "  Adaptations triggered: %,d%n", adaptations.size());
        System.out.printf( "  Total expand steps:    %,d%n", op.getTotalExpandSteps());
        System.out.printf( "  Total shrink steps:    %,d%n", op.getTotalShrinkSteps());
        System.out.printf( "  Adapted keys:          %,d%n", op.getAdaptedKeyCount());
    }

    // ==================== 5) Evaluate All ====================

    @Test @Order(5)
    @DisplayName("Evaluation — Accuracy + Latency + Per-Category")
    void evaluate() {
        assertNotNull(fixedResults, "Run Pipeline A first");
        assertNotNull(expandingResults, "Run Pipeline B first");
        assertNotNull(slidingResults, "Run Pipeline C first");
        assertNotNull(expandShrinkResults, "Run Pipeline D first");

        Metrics fixedM  = computeMetrics("FixedIntervalJoin", fixedResults,
                fixedElapsedMs, ingestion.size());
        Metrics expandM = computeMetrics("ExpandingIntervalJoin", expandingResults,
                expandingElapsedMs, ingestion.size());
        Metrics slidingM = computeMetrics("SlidingWindowJoin", slidingResults,
                slidingElapsedMs, ingestion.size());
        Metrics expandShrinkM = computeMetrics("ExpandShrinkJoin", expandShrinkResults,
                expandShrinkElapsedMs, ingestion.size());

        // Individual reports
        System.out.println(fixedM.report());
        System.out.println(expandM.report());
        System.out.println(slidingM.report());
        System.out.println(expandShrinkM.report());

        // Side-by-side comparison table
        printComparisonTable(fixedM, expandM, slidingM, expandShrinkM);

        // Detailed error lists (top 10 each)
        printErrorDetails("FixedIntervalJoin", fixedM, 10);
        printErrorDetails("ExpandingIntervalJoin", expandM, 10);
        printErrorDetails("SlidingWindowJoin", slidingM, 10);
        printErrorDetails("ExpandShrinkJoin", expandShrinkM, 10);
    }

    // ==================== 6) Export Results CSV ====================

    @Test @Order(6)
    @DisplayName("Export — CSV artifacts")
    void exportCsv() throws IOException {
        assertNotNull(fixedResults, "Run Pipeline A first");
        assertNotNull(expandingResults, "Run Pipeline B first");
        assertNotNull(slidingResults, "Run Pipeline C first");
        assertNotNull(expandShrinkResults, "Run Pipeline D first");

        Path dir = Path.of("benchmark_dataset", "two_stream_eval");
        Files.createDirectories(dir);

        Metrics fixedM  = computeMetrics("FixedIntervalJoin", fixedResults,
                fixedElapsedMs, ingestion.size());
        Metrics expandM = computeMetrics("ExpandingIntervalJoin", expandingResults,
                expandingElapsedMs, ingestion.size());
        Metrics slidingM = computeMetrics("SlidingWindowJoin", slidingResults,
                slidingElapsedMs, ingestion.size());
        Metrics expandShrinkM = computeMetrics("ExpandShrinkJoin", expandShrinkResults,
                expandShrinkElapsedMs, ingestion.size());

        // Summary CSV (with throughput columns)
        StringBuilder csv = new StringBuilder();
        csv.append("operator,scale_factor,total_events,TP,FP,FN,duplicates,");
        csv.append("precision,recall,f1,dup_rate,mean_lat,median_lat,p95_lat,");
        csv.append("elapsed_s,throughput_events_per_s\n");
        appendMetricsCsv(csv, fixedM);
        appendMetricsCsv(csv, expandM);
        appendMetricsCsv(csv, slidingM);
        appendMetricsCsv(csv, expandShrinkM);
        csv.append("\noperator,category,TP,FN,total,recall\n");
        appendCategoryCsv(csv, fixedM);
        appendCategoryCsv(csv, expandM);
        appendCategoryCsv(csv, slidingM);
        appendCategoryCsv(csv, expandShrinkM);
        Files.writeString(dir.resolve("evaluation_results.csv"), csv.toString());

        // Per-record join log: only write at small scale to avoid multi-GB files
        if (SCALE_FACTOR <= 100) {
            StringBuilder log = new StringBuilder("operator,orderId,paymentId,emitTime,streamProgress\n");
            for (EmittedMatch m : fixedResults)
                log.append(String.format("fixed,%s,%s,%d,%d%n", m.orderId, m.paymentId, m.emitTime, m.streamProgress));
            for (EmittedMatch m : expandingResults)
                log.append(String.format("expanding,%s,%s,%d,%d%n", m.orderId, m.paymentId, m.emitTime, m.streamProgress));
            for (EmittedMatch m : slidingResults)
                log.append(String.format("sliding,%s,%s,%d,%d%n", m.orderId, m.paymentId, m.emitTime, m.streamProgress));
            for (EmittedMatch m : expandShrinkResults)
                log.append(String.format("expandshrink,%s,%s,%d,%d%n", m.orderId, m.paymentId, m.emitTime, m.streamProgress));
            Files.writeString(dir.resolve("join_log.csv"), log.toString());
            System.out.println("  Per-record join_log.csv written.");
        } else {
            System.out.printf("  Skipping per-record join_log.csv at scale %,d (too large).%n", SCALE_FACTOR);
        }

        System.out.println("\n✅ CSV artifacts written to " + dir.toAbsolutePath());
    }

    // ═══════════════════════════════════════════════════════════════════
    //  EVENT FEEDING — incremental drain with ingestion-time tagging
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feed ingestion events into a FlinkIntervalJoinOperator.
     * After each event we drain newly produced outputs and tag them with the
     * current stream-progress (= max event-time seen so far across all records).
     * This lets us compute a meaningful detection latency:
     *   latency = streamProgress − tO
     * which measures how far the stream had to advance past the order event
     * before the correct match was produced.
     * Always ≥ 0: tO has already been seen, so maxTs ≥ tO.
     */
    private List<EmittedMatch> feedAndCollect(
            FlinkIntervalJoinOperator<String, OrderEvent, PaymentEvent, JoinedTuple> op) {
        List<EmittedMatch> results = new ArrayList<>();
        long maxTs = 0;
        for (Object[] ev : ingestion) {
            long ts = (long) ev[2];
            if (ts > maxTs) maxTs = ts;
            if ("L".equals(ev[0])) op.processLeft((OrderEvent) ev[1], ts);
            else                    op.processRight((PaymentEvent) ev[1], ts);

            // Drain any outputs produced by this event; tag with stream progress
            for (var o : op.drainOutputs()) {
                JoinedTuple t = o.getRecord();
                results.add(new EmittedMatch(t.orderId, t.paymentId, t.tO, t.tP,
                        o.getTimestamp(), maxTs));
            }
        }
        op.advanceWatermark(maxTs + ALPHA_MAX + 1);
        for (var o : op.drainOutputs()) {
            JoinedTuple t = o.getRecord();
            results.add(new EmittedMatch(t.orderId, t.paymentId, t.tO, t.tP,
                    o.getTimestamp(), maxTs + ALPHA_MAX + 1));
        }
        return results;
    }

    /**
     * Feed ingestion events into a SymmetricExpandingIntervalJoinOperator.
     * Same incremental drain + stream-progress tagging as above.
     */
    private List<EmittedMatch> feedAndCollect(
            SymmetricExpandingIntervalJoinOperator<String, OrderEvent, PaymentEvent, JoinedTuple> op) {
        List<EmittedMatch> results = new ArrayList<>();
        long maxTs = 0;
        for (Object[] ev : ingestion) {
            long ts = (long) ev[2];
            if (ts > maxTs) maxTs = ts;
            if ("L".equals(ev[0])) op.processLeft((OrderEvent) ev[1], ts);
            else                    op.processRight((PaymentEvent) ev[1], ts);

            // Drain any outputs produced by this event; tag with stream progress
            for (var o : op.drainOutputs()) {
                JoinedTuple t = o.getRecord();
                results.add(new EmittedMatch(t.orderId, t.paymentId, t.tO, t.tP,
                        o.getTimestamp(), maxTs));
            }
        }
        op.advanceWatermark(maxTs + ALPHA_MAX + 1);
        for (var o : op.drainOutputs()) {
            JoinedTuple t = o.getRecord();
            results.add(new EmittedMatch(t.orderId, t.paymentId, t.tO, t.tP,
                    o.getTimestamp(), maxTs + ALPHA_MAX + 1));
        }
        return results;
    }

    /**
     * Feed ingestion events into a SlidingWindowJoinOperator.
     * <p>
     * The sliding-window operator only emits output when a watermark advance
     * triggers window firing, so we advance the watermark after every event.
     * The stream-progress tag at each output is the watermark at fire time.
     */
    private List<EmittedMatch> feedAndCollect(
            SlidingWindowJoinOperator<String, OrderEvent, PaymentEvent, JoinedTuple> op) {
        List<EmittedMatch> results = new ArrayList<>();
        long maxTs = 0;
        for (Object[] ev : ingestion) {
            long ts = (long) ev[2];
            if (ts > maxTs) maxTs = ts;
            if ("L".equals(ev[0])) op.processLeft((OrderEvent) ev[1], ts);
            else                    op.processRight((PaymentEvent) ev[1], ts);

            // Advance watermark to current stream progress so windows can fire
            op.advanceWatermark(maxTs);

            // Drain any outputs produced by the watermark advance
            for (var o : op.drainOutputs()) {
                JoinedTuple t = o.getRecord();
                results.add(new EmittedMatch(t.orderId, t.paymentId, t.tO, t.tP,
                        o.getTimestamp(), maxTs));
            }
        }
        // Final watermark flush — fire all remaining windows
        op.advanceWatermark(maxTs + SLIDING_WINDOW_SIZE + 1);
        for (var o : op.drainOutputs()) {
            JoinedTuple t = o.getRecord();
            results.add(new EmittedMatch(t.orderId, t.paymentId, t.tO, t.tP,
                    o.getTimestamp(), maxTs + SLIDING_WINDOW_SIZE + 1));
        }
        return results;
    }

    /**
     * Feed ingestion events into an ExpandShrinkIntervalJoinOperator.
     * Same incremental drain + stream-progress tagging as the other interval joins.
     */
    private List<EmittedMatch> feedAndCollect(
            ExpandShrinkIntervalJoinOperator<String, OrderEvent, PaymentEvent, JoinedTuple> op) {
        List<EmittedMatch> results = new ArrayList<>();
        long maxTs = 0;
        for (Object[] ev : ingestion) {
            long ts = (long) ev[2];
            if (ts > maxTs) maxTs = ts;
            if ("L".equals(ev[0])) op.processLeft((OrderEvent) ev[1], ts);
            else                    op.processRight((PaymentEvent) ev[1], ts);

            for (var o : op.drainOutputs()) {
                JoinedTuple t = o.getRecord();
                results.add(new EmittedMatch(t.orderId, t.paymentId, t.tO, t.tP,
                        o.getTimestamp(), maxTs));
            }
        }
        op.advanceWatermark(maxTs + ALPHA_MAX + 1);
        for (var o : op.drainOutputs()) {
            JoinedTuple t = o.getRecord();
            results.add(new EmittedMatch(t.orderId, t.paymentId, t.tO, t.tP,
                    o.getTimestamp(), maxTs + ALPHA_MAX + 1));
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  METRICS COMPUTATION
    // ═══════════════════════════════════════════════════════════════════

    static class Metrics {
        String name;
        int tp, fp, fn, duplicates, totalOutput;
        double precision, recall, f1, dupRate;
        double meanLat, medianLat, p95Lat;
        long elapsedMs;          // wall-clock for the pipeline
        long totalEvents;        // ingestion size
        Map<String, int[]> perCategory = new LinkedHashMap<>(); // cat → [tp, fn, total]
        List<String> fpExamples = new ArrayList<>();
        List<String> fnExamples = new ArrayList<>();

        String report() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%n╔═══════════════════════════════════════════════════════════════════╗%n"));
            sb.append(String.format("║  EVALUATION: %-52s║%n", name));
            sb.append(String.format("╠═══════════════════════════════════════════════════════════════════╣%n"));
            sb.append(String.format("║  TP: %,10d    FP: %,10d    FN: %,10d            ║%n", tp, fp, fn));
            sb.append(String.format("║  Duplicates: %,10d    Total output: %,10d             ║%n", duplicates, totalOutput));
            sb.append(String.format("╠═══════════════════════════════════════════════════════════════════╣%n"));
            sb.append(String.format("║  Precision:      %.4f                                          ║%n", precision));
            sb.append(String.format("║  Recall:         %.4f                                          ║%n", recall));
            sb.append(String.format("║  F1:             %.4f                                          ║%n", f1));
            sb.append(String.format("║  Duplicate Rate: %.4f                                          ║%n", dupRate));
            sb.append(String.format("╠═══════════════════════════════════════════════════════════════════╣%n"));
            if (elapsedMs > 0) {
                double secs = elapsedMs / 1000.0;
                double throughput = totalEvents / secs;
                sb.append(String.format("║  Elapsed:    %8.1f s                                         ║%n", secs));
                sb.append(String.format("║  Throughput: %,.0f events/s                                 ║%n", throughput));
                sb.append(String.format("╠═══════════════════════════════════════════════════════════════════╣%n"));
            }
            sb.append(String.format("║  Detection Latency (stream_progress − tO):                      ║%n"));
            sb.append(String.format("║    Mean:   %8.1f   Median: %8.1f   p95: %8.1f             ║%n",
                    meanLat, medianLat, p95Lat));
            sb.append(String.format("╠═══════════════════════════════════════════════════════════════════╣%n"));
            sb.append(String.format("║  Per-Category Recall:                                             ║%n"));
            for (var e : perCategory.entrySet()) {
                int[] v = e.getValue();
                double r = v[2] > 0 ? (double) v[0] / v[2] : 1.0;
                sb.append(String.format("║    %-22s TP=%,7d  FN=%,7d  total=%,7d  recall=%.4f ║%n",
                        e.getKey(), v[0], v[1], v[2], r));
            }
            sb.append(String.format("╚═══════════════════════════════════════════════════════════════════╝%n"));
            return sb.toString();
        }
    }

    private Metrics computeMetrics(String name, List<EmittedMatch> output,
                                   long elapsedMs, long totalEvents) {
        Metrics m = new Metrics();
        m.name = name;
        m.totalOutput = output.size();
        m.elapsedMs = elapsedMs;
        m.totalEvents = totalEvents;

        List<GroundTruthRecord> gt = gen.getGroundTruth();

        // Build GT lookup: orderId → correct paymentId (null for NO_PAYMENT)
        Map<String, String> gtPayment = new HashMap<>(gt.size() * 2);
        Map<String, GroundTruthRecord> gtRecord = new HashMap<>(gt.size() * 2);
        for (GroundTruthRecord g : gt) {
            gtPayment.put(g.orderId, g.correctPaymentId);
            gtRecord.put(g.orderId, g);
        }

        // Deduplicate output per orderId|paymentId and count duplicates
        Map<String, List<EmittedMatch>> byKey = new HashMap<>();
        for (EmittedMatch em : output) {
            byKey.computeIfAbsent(em.matchKey(), k -> new ArrayList<>()).add(em);
        }
        int dupCount = 0;
        for (var entry : byKey.values()) {
            if (entry.size() > 1) dupCount += entry.size() - 1;
        }
        m.duplicates = dupCount;

        // Group output by orderId (for per-order evaluation)
        Map<String, List<EmittedMatch>> byOrder = new HashMap<>();
        for (EmittedMatch em : output) {
            byOrder.computeIfAbsent(em.orderId, k -> new ArrayList<>()).add(em);
        }

        // Compute TP, FP, FN per order
        int tp = 0, fp = 0, fn = 0;
        List<Long> latencies = new ArrayList<>();
        Set<String> tpOrders = new HashSet<>();

        for (GroundTruthRecord g : gt) {
            String oid = g.orderId;
            String correctPid = g.correctPaymentId;
            List<EmittedMatch> matches = byOrder.getOrDefault(oid, List.of());

            if (correctPid == null) {
                // NO_PAYMENT case: every output for this order is FP
                fp += matches.size();
                for (EmittedMatch em : matches) {
                    m.fpExamples.add("FP[NO_PAY] " + oid + "+" + em.paymentId);
                }
            } else {
                // There is a correct payment
                boolean found = false;
                for (EmittedMatch em : matches) {
                    if (em.paymentId.equals(correctPid)) {
                        if (!found) {
                            tp++;
                            found = true;
                            tpOrders.add(oid);
                            // Detection latency = streamProgress − tO
                            // Measures how far the stream had to advance past the
                            // order event before the correct match was produced.
                            //
                            // For NORMAL cases (payment close to order): small latency.
                            // For LATE/EARLY cases recovered by expanding: high latency
                            //   — the operator had to wait for the late payment.
                            // For Fixed join: late cases are FN (no latency sample),
                            //   so only prompt matches contribute → low mean latency.
                            //
                            // This directly captures the recall-vs-latency tradeoff.
                            latencies.add(em.streamProgress - g.tO);
                        }
                        // extra copies of same correct match → duplicate (counted above)
                    } else {
                        fp++;
                        m.fpExamples.add("FP[WRONG] " + oid + " expected=" + correctPid
                                + " got=" + em.paymentId);
                    }
                }
                if (!found) {
                    fn++;
                    m.fnExamples.add("FN " + g);
                }
            }
        }

        m.tp = tp;
        m.fp = fp;
        m.fn = fn;
        m.precision = (tp + fp) > 0 ? (double) tp / (tp + fp) : 0.0;
        m.recall    = (tp + fn) > 0 ? (double) tp / (tp + fn) : 0.0;
        m.f1        = (m.precision + m.recall) > 0
                ? 2 * m.precision * m.recall / (m.precision + m.recall) : 0.0;
        int gtMatchable = (int) gt.stream().filter(g -> g.correctPaymentId != null).count();
        m.dupRate = gtMatchable > 0 ? (double) dupCount / gtMatchable : 0.0;

        // Latency stats
        if (!latencies.isEmpty()) {
            Collections.sort(latencies);
            long sum = 0; for (long l : latencies) sum += l;
            m.meanLat = (double) sum / latencies.size();
            m.medianLat = percentile(latencies, 50);
            m.p95Lat = percentile(latencies, 95);
        }

        // Per-category
        Map<String, List<GroundTruthRecord>> byCat = gt.stream()
                .collect(Collectors.groupingBy(g -> g.category, LinkedHashMap::new, Collectors.toList()));
        for (var e : byCat.entrySet()) {
            String cat = e.getKey();
            int catTP = 0, catFN = 0;
            for (GroundTruthRecord g : e.getValue()) {
                if (g.correctPaymentId == null) continue; // NO_PAYMENT has no TP/FN
                if (tpOrders.contains(g.orderId)) catTP++; else catFN++;
            }
            int catTotal = catTP + catFN;
            // For NO_PAYMENT category count orders where operator correctly emitted nothing
            if ("NO_PAYMENT".equals(cat)) {
                int fpForCat = 0;
                for (GroundTruthRecord g : e.getValue()) {
                    if (byOrder.containsKey(g.orderId)) fpForCat++;
                }
                m.perCategory.put(cat, new int[]{e.getValue().size() - fpForCat, fpForCat, e.getValue().size()});
            } else {
                m.perCategory.put(cat, new int[]{catTP, catFN, catTotal});
            }
        }

        return m;
    }

    private static double percentile(List<Long> sorted, int pct) {
        if (sorted.isEmpty()) return 0;
        double idx = (pct / 100.0) * (sorted.size() - 1);
        int lo = (int) Math.floor(idx);
        int hi = Math.min((int) Math.ceil(idx), sorted.size() - 1);
        if (lo == hi) return sorted.get(lo);
        double frac = idx - lo;
        return sorted.get(lo) * (1 - frac) + sorted.get(hi) * frac;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  COMPARISON TABLE
    // ═══════════════════════════════════════════════════════════════════

    private void printComparisonTable(Metrics... operators) {
        List<Metrics> ops = Arrays.asList(operators);
        int n = ops.size();

        // Column widths
        int labelW = 22;
        int colW = 20;
        int totalW = labelW + 4 + n * (colW + 5) + 2;
        String sep = "═".repeat(totalW);

        System.out.println("\n╔" + sep + "╗");
        System.out.printf( "║  COMPARISON TABLE — GROUND TRUTH BENCHMARK%" + (totalW - 45) + "s║%n", "");
        System.out.println("╠" + sep + "╣");

        // Header
        StringBuilder hdr = new StringBuilder();
        hdr.append(String.format("║  %-" + labelW + "s │", "Metric"));
        for (Metrics m : ops) hdr.append(String.format(" %-" + colW + "s │", m.name));
        System.out.println(hdr + "");
        System.out.println("╠" + sep + "╣");

        // Accuracy rows
        rowN("True Positives",  ops.stream().mapToInt(m -> m.tp).toArray(), labelW, colW);
        rowN("False Positives", ops.stream().mapToInt(m -> m.fp).toArray(), labelW, colW);
        rowN("False Negatives", ops.stream().mapToInt(m -> m.fn).toArray(), labelW, colW);
        rowN("Duplicates",      ops.stream().mapToInt(m -> m.duplicates).toArray(), labelW, colW);
        rowNF("Precision",      ops.stream().mapToDouble(m -> m.precision).toArray(), labelW, colW);
        rowNF("Recall",         ops.stream().mapToDouble(m -> m.recall).toArray(), labelW, colW);
        rowNF("F1",             ops.stream().mapToDouble(m -> m.f1).toArray(), labelW, colW);
        rowNF("Duplicate Rate", ops.stream().mapToDouble(m -> m.dupRate).toArray(), labelW, colW);

        System.out.println("╠" + sep + "╣");
        System.out.printf( "║  Detection Latency (stream_progress − tO):%" + (totalW - 44) + "s║%n", "");
        rowNF("Mean Latency",   ops.stream().mapToDouble(m -> m.meanLat).toArray(), labelW, colW);
        rowNF("Median Latency", ops.stream().mapToDouble(m -> m.medianLat).toArray(), labelW, colW);
        rowNF("p95 Latency",    ops.stream().mapToDouble(m -> m.p95Lat).toArray(), labelW, colW);

        System.out.println("╠" + sep + "╣");
        System.out.printf( "║  Elapsed / Throughput:%" + (totalW - 23) + "s║%n", "");
        rowNF("Elapsed (s)",    ops.stream().mapToDouble(m -> m.elapsedMs / 1000.0).toArray(), labelW, colW);
        rowNF("Throughput (ev/s)",
              ops.stream().mapToDouble(m -> m.totalEvents / (m.elapsedMs / 1000.0)).toArray(), labelW, colW);

        System.out.println("╠" + sep + "╣");
        System.out.printf( "║  Per-Category Recall:%" + (totalW - 22) + "s║%n", "");

        Set<String> allCats = new LinkedHashSet<>();
        for (Metrics m : ops) allCats.addAll(m.perCategory.keySet());
        for (String cat : allCats) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("║    %-" + labelW + "s │", cat));
            for (Metrics m : ops) {
                int[] v = m.perCategory.getOrDefault(cat, new int[]{0,0,0});
                double r = v[2] > 0 ? (double) v[0] / v[2] : 0;
                sb.append(String.format(" %6d/%-6d(%.2f) │", v[0], v[2], r));
            }
            System.out.println(sb);
        }
        System.out.println("╚" + sep + "╝");

        // Interpretation
        System.out.println("\n  INTERPRETATION:");
        Metrics bestRecall = ops.stream().max(Comparator.comparingDouble(m -> m.recall)).orElse(ops.get(0));
        Metrics worstRecall = ops.stream().min(Comparator.comparingDouble(m -> m.recall)).orElse(ops.get(0));
        if (bestRecall != worstRecall) {
            System.out.printf("  • %s has HIGHEST recall (%.4f); %s has lowest (%.4f).%n",
                    bestRecall.name, bestRecall.recall, worstRecall.name, worstRecall.recall);
        }
        Metrics fewestFP = ops.stream().min(Comparator.comparingInt(m -> m.fp)).orElse(ops.get(0));
        System.out.printf("  • %s has FEWEST false positives (%,d).%n", fewestFP.name, fewestFP.fp);
        Metrics bestF1 = ops.stream().max(Comparator.comparingDouble(m -> m.f1)).orElse(ops.get(0));
        System.out.printf("  • %s has BEST F1 score (%.4f).%n", bestF1.name, bestF1.f1);
        Metrics lowestLat = ops.stream().min(Comparator.comparingDouble(m -> m.meanLat)).orElse(ops.get(0));
        Metrics highestLat = ops.stream().max(Comparator.comparingDouble(m -> m.meanLat)).orElse(ops.get(0));
        System.out.printf("  • %s has LOWEST mean latency (%.1f); %s has highest (%.1f).%n",
                lowestLat.name, lowestLat.meanLat, highestLat.name, highestLat.meanLat);
        System.out.printf("  • SlidingWindowJoin: window=%d slide=%d (deferred output at window close).%n",
                SLIDING_WINDOW_SIZE, SLIDING_SLIDE_SIZE);
        System.out.println("  • ExpandShrinkJoin: expands to find match, then shrinks to reduce FP.");
    }

    private void rowN(String label, int[] vals, int labelW, int colW) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("║  %-" + labelW + "s │", label));
        for (int v : vals) sb.append(String.format(" %-" + colW + "d │", v));
        System.out.println(sb);
    }

    private void rowNF(String label, double[] vals, int labelW, int colW) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("║  %-" + labelW + "s │", label));
        for (double v : vals) sb.append(String.format(" %-" + colW + ".4f │", v));
        System.out.println(sb);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  DETAILED ERROR LISTS
    // ═══════════════════════════════════════════════════════════════════

    private void printErrorDetails(String name, Metrics m, int topN) {
        System.out.println("\n═══ Error Details: " + name + " ═══");
        if (!m.fpExamples.isEmpty()) {
            System.out.println("  FALSE POSITIVES (top " + Math.min(topN, m.fpExamples.size()) + "):");
            m.fpExamples.stream().limit(topN).forEach(s -> System.out.println("    " + s));
        }
        if (!m.fnExamples.isEmpty()) {
            System.out.println("  FALSE NEGATIVES (top " + Math.min(topN, m.fnExamples.size()) + "):");
            m.fnExamples.stream().limit(topN).forEach(s -> System.out.println("    " + s));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  CSV HELPERS
    // ═══════════════════════════════════════════════════════════════════

    private void appendMetricsCsv(StringBuilder sb, Metrics m) {
        sb.append(String.format("%s,%d,%d,%d,%d,%.4f,%.4f,%.4f,%.4f,%.1f,%.1f,%.1f%n",
                m.name, m.tp, m.fp, m.fn, m.duplicates,
                m.precision, m.recall, m.f1, m.dupRate,
                m.meanLat, m.medianLat, m.p95Lat));
    }

    private void appendCategoryCsv(StringBuilder sb, Metrics m) {
        for (var e : m.perCategory.entrySet()) {
            int[] v = e.getValue();
            double r = v[2] > 0 ? (double) v[0] / v[2] : 0;
            sb.append(String.format("%s,%s,%d,%d,%d,%.4f%n",
                    m.name, e.getKey(), v[0], v[1], v[2], r));
        }
    }
}
