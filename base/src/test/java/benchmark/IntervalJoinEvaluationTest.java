package benchmark;

import benchmark.model.*;
import benchmark.model.GroundTruthMatch.MatchCategory;
import benchmark.GroundTruthEvaluator.*;

import org.streamreasoning.polyflow.base.operatorsimpl.s2r.FlinkIntervalJoinOperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.SymmetricExpandingIntervalJoinOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin.*;

import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Interval Join Evaluation Framework — compares Fixed vs Expanding interval join.
 *
 * <h2>What This Test Does</h2>
 * <ol>
 *   <li>Generates a synthetic dataset (orders + shipments) with ground truth</li>
 *   <li>Runs Pipeline A: Fixed Interval Join (Flink-style)</li>
 *   <li>Runs Pipeline B: Symmetric Expanding Interval Join</li>
 *   <li>Evaluates each pipeline against ground truth</li>
 *   <li>Reports: precision, recall, duplicate rate, median/p95 latency</li>
 * </ol>
 *
 * <h2>Pipelines</h2>
 * <pre>
 *   Pipeline A:  Orders ──┐
 *                         ├── FixedIntervalJoin ──▸ (orderId, orderTime, shipTime)
 *                Shipments┘
 *
 *   Pipeline B:  Orders ──┐
 *                         ├── SymmetricExpandingIntervalJoin ──▸ (orderId, orderTime, shipTime)
 *                Shipments┘
 * </pre>
 *
 * <h2>Ground Truth</h2>
 * <pre>
 *   G = { (orderId, orderTime, shipTime) | correct matches }
 *   matchId = orderId (one correct shipment per order in the simple two-stream case)
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IntervalJoinEvaluationTest {

    // ==================== Dataset ====================

    /** Simple order event for two-stream join */
    static class SimpleOrder {
        final String orderId;
        final long orderTime;

        SimpleOrder(String orderId, long orderTime) {
            this.orderId = orderId;
            this.orderTime = orderTime;
        }

        @Override
        public String toString() {
            return "Order{" + orderId + " @" + orderTime + '}';
        }
    }

    /** Simple shipment event for two-stream join */
    static class SimpleShipment {
        final String orderId;
        final long shipTime;

        SimpleShipment(String orderId, long shipTime) {
            this.orderId = orderId;
            this.shipTime = shipTime;
        }

        @Override
        public String toString() {
            return "Ship{" + orderId + " @" + shipTime + '}';
        }
    }

    /** A joined output tuple */
    static class JoinedTuple {
        final String orderId;
        final long orderTime;
        final long shipTime;

        JoinedTuple(String orderId, long orderTime, long shipTime) {
            this.orderId = orderId;
            this.orderTime = orderTime;
            this.shipTime = shipTime;
        }

        @Override
        public String toString() {
            return "Joined{" + orderId + ", tO=" + orderTime + ", tS=" + shipTime + '}';
        }
    }

    /** Ground truth entry for two-stream join */
    static class TwoStreamGroundTruth {
        final String orderId;
        final long orderTime;
        final long shipTime;
        final String category;

        TwoStreamGroundTruth(String orderId, long orderTime, long shipTime, String category) {
            this.orderId = orderId;
            this.orderTime = orderTime;
            this.shipTime = shipTime;
            this.category = category;
        }

        String matchKey() {
            return orderId;
        }
    }

    // ==================== Test State ====================

    private static List<SimpleOrder> orders;
    private static List<SimpleShipment> shipments;
    private static List<TwoStreamGroundTruth> groundTruth;
    private static Set<String> negativeOrders;

    private static List<OperatorMatch> fixedOutput;
    private static List<OperatorMatch> expandingOutput;

    /** All join log records for CSV export */
    private static final List<String> joinLogLines = new ArrayList<>();

    // Window parameters
    private static final long ALPHA = 100;  // forward bound
    private static final long BETA = 0;     // backward bound

    // Expanding operator parameters
    private static final long ALPHA0 = 50;   // initial forward bound (smaller to show expansion)
    private static final long BETA0 = 0;     // initial backward bound
    private static final long ALPHA_MAX = 200;
    private static final long BETA_MAX = 50;
    private static final long DELTA = 25;    // expansion step

    // ==================== Dataset Generation ====================

    @BeforeAll
    static void generateDataset() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  INTERVAL JOIN EVALUATION FRAMEWORK");
        System.out.println("  Fixed Interval Join  vs  Symmetric Expanding Interval Join");
        System.out.println("=".repeat(80));

        Random rng = new Random(42);
        orders = new ArrayList<>();
        shipments = new ArrayList<>();
        groundTruth = new ArrayList<>();
        negativeOrders = new HashSet<>();

        int orderId = 0;

        // ── NORMAL MATCHES (20): shipment well within [0, ALPHA] ──
        for (int i = 0; i < 20; i++) {
            String oid = "O" + (++orderId);
            long tO = orderId * 10L;
            long tS = tO + 10 + rng.nextInt(70); // well within ALPHA=100
            orders.add(new SimpleOrder(oid, tO));
            shipments.add(new SimpleShipment(oid, tS));
            groundTruth.add(new TwoStreamGroundTruth(oid, tO, tS, "NORMAL"));
        }

        // ── LATE ARRIVALS (10): shipment outside base ALPHA but within ALPHA_MAX ──
        // Fixed join will MISS these. Expanding join should CATCH them.
        for (int i = 0; i < 10; i++) {
            String oid = "O" + (++orderId);
            long tO = orderId * 10L;
            long tS = tO + ALPHA + 10 + rng.nextInt(80); // outside ALPHA, within ALPHA_MAX
            orders.add(new SimpleOrder(oid, tO));
            shipments.add(new SimpleShipment(oid, tS));
            groundTruth.add(new TwoStreamGroundTruth(oid, tO, tS, "LATE_ARRIVAL"));
        }

        // ── DELAYED OUTSIDE BASE (8): shipment just beyond ALPHA boundary ──
        for (int i = 0; i < 8; i++) {
            String oid = "O" + (++orderId);
            long tO = orderId * 10L;
            long tS = tO + ALPHA + 1 + rng.nextInt(20); // barely outside base
            orders.add(new SimpleOrder(oid, tO));
            shipments.add(new SimpleShipment(oid, tS));
            groundTruth.add(new TwoStreamGroundTruth(oid, tO, tS, "DELAYED_OUTSIDE_BASE"));
        }

        // ── NEAR BOUNDARY (8): shipment right at ALPHA boundary ──
        for (int i = 0; i < 8; i++) {
            String oid = "O" + (++orderId);
            long tO = orderId * 10L;
            long tS = tO + ALPHA - rng.nextInt(5); // right at boundary
            orders.add(new SimpleOrder(oid, tO));
            shipments.add(new SimpleShipment(oid, tS));
            groundTruth.add(new TwoStreamGroundTruth(oid, tO, tS, "NEAR_BOUNDARY"));
        }

        // ── UNMATCHED ORDERS (5): no shipment at all ──
        for (int i = 0; i < 5; i++) {
            String oid = "O" + (++orderId);
            long tO = orderId * 10L;
            orders.add(new SimpleOrder(oid, tO));
            negativeOrders.add(oid);
            // No shipment, no ground truth
        }

        // ── UNMATCHED SHIPMENTS (5): no order for them ──
        for (int i = 0; i < 5; i++) {
            String oid = "ORPHAN" + i;
            long tS = 500 + rng.nextInt(200);
            shipments.add(new SimpleShipment(oid, tS));
            // No order, no ground truth
        }

        // Sort by timestamp (stream arrival order)
        orders.sort(Comparator.comparingLong(o -> o.orderTime));
        shipments.sort(Comparator.comparingLong(s -> s.shipTime));

        System.out.println("\n  Dataset Summary:");
        System.out.printf("    Orders:     %d%n", orders.size());
        System.out.printf("    Shipments:  %d%n", shipments.size());
        System.out.printf("    Ground Truth: %d correct matches%n", groundTruth.size());
        System.out.printf("    Negative Orders: %d (should NOT match)%n", negativeOrders.size());

        Map<String, Long> catCounts = groundTruth.stream()
                .collect(Collectors.groupingBy(g -> g.category, Collectors.counting()));
        for (var e : catCounts.entrySet()) {
            System.out.printf("      %-25s %d%n", e.getKey(), e.getValue());
        }
    }

    // ==================== Pipeline A: Fixed Interval Join ====================

    @Test
    @Order(1)
    @DisplayName("Pipeline A: Fixed Interval Join (Flink-style)")
    void runFixedIntervalJoin() {
        System.out.println("\n══════ PIPELINE A: Fixed Interval Join ══════");
        System.out.println("  bounds: [tL - " + BETA + ", tL + " + ALPHA + "]");

        FlinkIntervalJoinOperator<String, SimpleOrder, SimpleShipment, JoinedTuple> operator =
                FlinkIntervalJoinOperator.<String, SimpleOrder, SimpleShipment, JoinedTuple>builder()
                        .lowerBound(BETA)
                        .upperBound(ALPHA)
                        .leftKeyExtractor(o -> o.orderId)
                        .rightKeyExtractor(s -> s.orderId)
                        .processJoinFunction((left, right, ctx, collector) -> {
                            JoinedTuple tuple = new JoinedTuple(
                                    left.orderId, left.orderTime, right.shipTime);
                            collector.collect(tuple, ctx.getTimestamp());
                        })
                        .build();

        // Feed all events in event-time order
        List<Object[]> allEvents = new ArrayList<>();
        for (SimpleOrder o : orders) allEvents.add(new Object[]{"L", o, o.orderTime});
        for (SimpleShipment s : shipments) allEvents.add(new Object[]{"R", s, s.shipTime});
        allEvents.sort(Comparator.comparingLong(e -> (long) e[2]));

        long maxTs = 0;
        for (Object[] event : allEvents) {
            long ts = (long) event[2];
            if (ts > maxTs) maxTs = ts;
            if ("L".equals(event[0])) {
                operator.processLeft((SimpleOrder) event[1], ts);
            } else {
                operator.processRight((SimpleShipment) event[1], ts);
            }
        }
        // Advance watermark to flush
        operator.advanceWatermark(maxTs + ALPHA + 1);

        // Collect output
        fixedOutput = new ArrayList<>();
        for (var out : operator.drainOutputs()) {
            JoinedTuple t = out.getRecord();
            long emitEventTime = out.getTimestamp();
            long emitProcessingTimeNanos = System.nanoTime();

            OperatorMatch match = new OperatorMatch(
                    t.orderId, t.orderId, t.orderId,  // paymentId=shipmentId=orderId for two-stream
                    emitEventTime,
                    emitProcessingTimeNanos,
                    emitEventTime,
                    "fixed",
                    ALPHA, BETA);
            fixedOutput.add(match);

            // Enhanced logging
            joinLogLines.add(String.format(
                    "fixed,%s,%d,%d,%d,%d,%d,%d",
                    t.orderId, t.orderTime, t.shipTime,
                    emitEventTime, emitProcessingTimeNanos,
                    ALPHA, BETA));
        }

        System.out.println("  Output: " + fixedOutput.size() + " matches");
    }

    // ==================== Pipeline B: Symmetric Expanding Interval Join ====================

    @Test
    @Order(2)
    @DisplayName("Pipeline B: Symmetric Expanding Interval Join")
    void runExpandingIntervalJoin() {
        System.out.println("\n══════ PIPELINE B: Symmetric Expanding Interval Join ══════");
        System.out.println("  initial: α₀=" + ALPHA0 + ", β₀=" + BETA0);
        System.out.println("  max:     αmax=" + ALPHA_MAX + ", βmax=" + BETA_MAX);
        System.out.println("  step:    Δ=" + DELTA);

        SymmetricExpandingIntervalJoinOperator<String, SimpleOrder, SimpleShipment, JoinedTuple> operator =
                SymmetricExpandingIntervalJoinOperator.<String, SimpleOrder, SimpleShipment, JoinedTuple>builder()
                        .alpha0(ALPHA0)
                        .beta0(BETA0)
                        .alphaMax(ALPHA_MAX)
                        .betaMax(BETA_MAX)
                        .delta(DELTA)
                        .leftKeyExtractor(o -> o.orderId)
                        .rightKeyExtractor(s -> s.orderId)
                        .processJoinFunction((left, right, ctx, collector) -> {
                            JoinedTuple tuple = new JoinedTuple(
                                    left.orderId, left.orderTime, right.shipTime);
                            collector.collect(tuple, ctx.getTimestamp());
                        })
                        .build();

        // Feed all events in event-time order
        List<Object[]> allEvents = new ArrayList<>();
        for (SimpleOrder o : orders) allEvents.add(new Object[]{"L", o, o.orderTime});
        for (SimpleShipment s : shipments) allEvents.add(new Object[]{"R", s, s.shipTime});
        allEvents.sort(Comparator.comparingLong(e -> (long) e[2]));

        long maxTs = 0;
        for (Object[] event : allEvents) {
            long ts = (long) event[2];
            if (ts > maxTs) maxTs = ts;
            if ("L".equals(event[0])) {
                operator.processLeft((SimpleOrder) event[1], ts);
            } else {
                operator.processRight((SimpleShipment) event[1], ts);
            }
        }
        // Advance watermark to flush
        operator.advanceWatermark(maxTs + ALPHA_MAX + 1);

        // Collect output
        expandingOutput = new ArrayList<>();
        for (var out : operator.drainOutputs()) {
            JoinedTuple t = out.getRecord();
            long emitEventTime = out.getTimestamp();
            long emitProcessingTimeNanos = System.nanoTime();

            // Retrieve per-key bounds
            long currentAlpha = operator.getAlpha(t.orderId);
            long currentBeta = operator.getBeta(t.orderId);

            OperatorMatch match = new OperatorMatch(
                    t.orderId, t.orderId, t.orderId,
                    emitEventTime,
                    emitProcessingTimeNanos,
                    emitEventTime,
                    "expanding",
                    currentAlpha, currentBeta);
            expandingOutput.add(match);

            // Enhanced logging
            joinLogLines.add(String.format(
                    "expanding,%s,%d,%d,%d,%d,%d,%d",
                    t.orderId, t.orderTime, t.shipTime,
                    emitEventTime, emitProcessingTimeNanos,
                    currentAlpha, currentBeta));
        }

        System.out.println("  Output: " + expandingOutput.size() + " matches");

        // Print expansion log summary
        var expansions = operator.peekExpansionLog();
        System.out.println("  Expansions triggered: " + expansions.size());
        System.out.println("  Expanded keys: " + operator.getExpandedKeyCount());
        for (var exp : expansions) {
            System.out.println("    " + exp);
        }
    }

    // ==================== Evaluation ====================

    @Test
    @Order(3)
    @DisplayName("Evaluation: Accuracy + Latency + Duplicates")
    void evaluateAndCompare() {
        assertNotNull(fixedOutput, "Run Pipeline A first");
        assertNotNull(expandingOutput, "Run Pipeline B first");

        // Convert ground truth to GroundTruthMatch format for evaluation
        List<GroundTruthMatch> gtMatches = groundTruth.stream()
                .map(g -> new GroundTruthMatch(
                        g.orderId, g.orderId, g.orderId,  // paymentId=shipmentId=orderId
                        g.orderTime, g.orderTime, g.shipTime,
                        MatchCategory.NORMAL))  // category not used in main metrics
                .collect(Collectors.toList());

        // Evaluate both pipelines
        EvaluationResult fixedResult = GroundTruthEvaluator.evaluate(
                "FixedIntervalJoin", fixedOutput, gtMatches, negativeOrders);
        EvaluationResult expandResult = GroundTruthEvaluator.evaluate(
                "ExpandingIntervalJoin", expandingOutput, gtMatches, negativeOrders);

        // Print individual results
        System.out.println(fixedResult);
        System.out.println(expandResult);

        // Print side-by-side comparison
        GroundTruthEvaluator.printComparison(List.of(fixedResult, expandResult));

        // ── Final Comparison Table (thesis format) ──
        System.out.println("\n" + "═".repeat(90));
        System.out.println("  FINAL COMPARISON TABLE");
        System.out.println("═".repeat(90));
        System.out.printf("  %-28s │ %-9s │ %-6s │ %-14s │ %-14s │ %-11s%n",
                "Approach", "Precision", "Recall", "Duplicate Rate", "Median Latency", "p95 Latency");
        System.out.println("  " + "─".repeat(86));
        printFinalRow("Fixed Interval Join", fixedResult);
        printFinalRow("Expanding Interval Join", expandResult);
        System.out.println("═".repeat(90));

        // ── Interpretation ──
        System.out.println("\n  INTERPRETATION:");
        if (fixedResult.recall() < expandResult.recall()) {
            System.out.printf("  • Expanding join has HIGHER recall (%.4f vs %.4f) — recovers late matches.%n",
                    expandResult.recall(), fixedResult.recall());
        }
        if (fixedResult.duplicateRate() < expandResult.duplicateRate()) {
            System.out.printf("  • Fixed join has LOWER duplicate rate (%.4f vs %.4f).%n",
                    fixedResult.duplicateRate(), expandResult.duplicateRate());
        }
        if (expandResult.latencyMetrics.medianLatency > fixedResult.latencyMetrics.medianLatency) {
            System.out.printf("  • Expanding join has HIGHER median latency (%.1f vs %.1f) — cost of expansion.%n",
                    expandResult.latencyMetrics.medianLatency, fixedResult.latencyMetrics.medianLatency);
        }
    }

    private void printFinalRow(String name, EvaluationResult r) {
        System.out.printf("  %-28s │ %-9.4f │ %-6.4f │ %-14.4f │ %-14.1f │ %-11.1f%n",
                name,
                r.precision(), r.recall(), r.duplicateRate(),
                r.latencyMetrics.medianLatency, r.latencyMetrics.p95Latency);
    }

    // ==================== Export ====================

    @Test
    @Order(4)
    @DisplayName("Export: CSV results + join logs")
    void exportResults() throws IOException {
        assertNotNull(fixedOutput, "Run Pipeline A first");
        assertNotNull(expandingOutput, "Run Pipeline B first");

        Path outputDir = Path.of("benchmark_dataset", "interval_join_eval");
        Files.createDirectories(outputDir);

        // Convert ground truth
        List<GroundTruthMatch> gtMatches = groundTruth.stream()
                .map(g -> new GroundTruthMatch(
                        g.orderId, g.orderId, g.orderId,
                        g.orderTime, g.orderTime, g.shipTime,
                        MatchCategory.NORMAL))
                .collect(Collectors.toList());

        EvaluationResult fixedResult = GroundTruthEvaluator.evaluate(
                "FixedIntervalJoin", fixedOutput, gtMatches, negativeOrders);
        EvaluationResult expandResult = GroundTruthEvaluator.evaluate(
                "ExpandingIntervalJoin", expandingOutput, gtMatches, negativeOrders);

        // Write evaluation CSV
        Files.writeString(
                outputDir.resolve("evaluation_results.csv"),
                GroundTruthEvaluator.toCsv(List.of(fixedResult, expandResult)));

        // Write join log CSV
        StringBuilder logCsv = new StringBuilder();
        logCsv.append("approach,matchId,orderTime,shipTime,emitEventTime,emitProcessingTime,currentAlpha,currentBeta\n");
        for (String line : joinLogLines) {
            logCsv.append(line).append("\n");
        }
        Files.writeString(outputDir.resolve("join_log.csv"), logCsv.toString());

        // Write ground truth CSV
        StringBuilder gtCsv = new StringBuilder();
        gtCsv.append("orderId,orderTime,shipTime,category\n");
        for (TwoStreamGroundTruth g : groundTruth) {
            gtCsv.append(String.format("%s,%d,%d,%s%n", g.orderId, g.orderTime, g.shipTime, g.category));
        }
        Files.writeString(outputDir.resolve("ground_truth.csv"), gtCsv.toString());

        // Write orders CSV
        StringBuilder ordersCsv = new StringBuilder("orderId,orderTime\n");
        for (SimpleOrder o : orders) {
            ordersCsv.append(String.format("%s,%d%n", o.orderId, o.orderTime));
        }
        Files.writeString(outputDir.resolve("orders.csv"), ordersCsv.toString());

        // Write shipments CSV
        StringBuilder shipCsv = new StringBuilder("orderId,shipTime\n");
        for (SimpleShipment s : shipments) {
            shipCsv.append(String.format("%s,%d%n", s.orderId, s.shipTime));
        }
        Files.writeString(outputDir.resolve("shipments.csv"), shipCsv.toString());

        System.out.println("\n✅ Results exported to: " + outputDir.toAbsolutePath());
        System.out.println("  • evaluation_results.csv");
        System.out.println("  • join_log.csv");
        System.out.println("  • ground_truth.csv");
        System.out.println("  • orders.csv");
        System.out.println("  • shipments.csv");
    }
}
