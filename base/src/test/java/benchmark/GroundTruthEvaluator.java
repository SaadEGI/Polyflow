package benchmark;

import benchmark.model.GroundTruthMatch;
import benchmark.model.GroundTruthMatch.MatchCategory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Evaluates operator output against ground truth.
 *
 * <h2>Metrics (as specified by thesis committee)</h2>
 * <ul>
 *   <li><b>True Positives (TP):</b> Operator produced match that exists in ground truth</li>
 *   <li><b>False Positives (FP):</b> Operator produced match NOT in ground truth</li>
 *   <li><b>False Negatives (FN):</b> Ground truth match that operator MISSED</li>
 *   <li><b>Duplicates:</b> Operator produced same match more than once</li>
 *   <li><b>Precision:</b> TP / (TP + FP)</li>
 *   <li><b>Recall:</b> TP / (TP + FN)</li>
 *   <li><b>F1 Score:</b> 2 * Precision * Recall / (Precision + Recall)</li>
 * </ul>
 *
 * <h2>Per-Category Analysis</h2>
 * <p>Results are broken down by conflict category to understand WHERE each operator
 * succeeds and fails. This is the core comparison for the thesis.</p>
 */
public class GroundTruthEvaluator {

    /**
     * Represents one output match from an operator.
     */
    public static class OperatorMatch {
        public final String orderId;
        public final String paymentId;
        public final String shipmentId;
        public final long outputTime;

        // ── Enhanced logging fields for thesis evaluation ──
        /** Wall-clock time (System.nanoTime()) when this match was emitted */
        public final long emitProcessingTimeNanos;
        /** Event-time at which the match was emitted: max(tL, tR) */
        public final long emitEventTime;
        /** Name of the operator that produced this match */
        public final String approach;
        /** Current α bound at emit time (for expanding operator; 0 for fixed) */
        public final long currentAlpha;
        /** Current β bound at emit time (for expanding operator; 0 for fixed) */
        public final long currentBeta;

        /** Simple constructor (backward compatible) */
        public OperatorMatch(String orderId, String paymentId, String shipmentId, long outputTime) {
            this(orderId, paymentId, shipmentId, outputTime, 0L, outputTime, "", 0, 0);
        }

        /** Full constructor with latency and logging fields */
        public OperatorMatch(String orderId, String paymentId, String shipmentId, long outputTime,
                             long emitProcessingTimeNanos, long emitEventTime, String approach,
                             long currentAlpha, long currentBeta) {
            this.orderId = orderId;
            this.paymentId = paymentId;
            this.shipmentId = shipmentId;
            this.outputTime = outputTime;
            this.emitProcessingTimeNanos = emitProcessingTimeNanos;
            this.emitEventTime = emitEventTime;
            this.approach = approach;
            this.currentAlpha = currentAlpha;
            this.currentBeta = currentBeta;
        }

        public String matchKey() {
            return orderId + "|" + paymentId + "|" + shipmentId;
        }

        public String contentKey() {
            return orderId + "|" + shipmentId;
        }

        @Override
        public String toString() {
            return "Match{" + orderId + ", " + paymentId + ", " + shipmentId + ", t=" + outputTime + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OperatorMatch that = (OperatorMatch) o;
            return Objects.equals(orderId, that.orderId) &&
                   Objects.equals(paymentId, that.paymentId) &&
                   Objects.equals(shipmentId, that.shipmentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId, paymentId, shipmentId);
        }
    }

    // ==================== Latency Metrics ====================

    /**
     * Latency statistics for a set of correct matches.
     * Latency(g) = emitEventTime - max(orderTime, shipmentTime)
     */
    public static class LatencyMetrics {
        public final int count;
        public final double meanLatency;
        public final double medianLatency;
        public final double p95Latency;
        public final double minLatency;
        public final double maxLatency;
        public final List<Long> rawLatencies;

        public LatencyMetrics(List<Long> latencies) {
            this.rawLatencies = latencies;
            this.count = latencies.size();
            if (count == 0) {
                meanLatency = medianLatency = p95Latency = minLatency = maxLatency = 0.0;
                return;
            }
            List<Long> sorted = new ArrayList<>(latencies);
            Collections.sort(sorted);
            long sum = 0;
            for (long v : sorted) sum += v;
            this.meanLatency = (double) sum / count;
            this.medianLatency = percentile(sorted, 50);
            this.p95Latency = percentile(sorted, 95);
            this.minLatency = sorted.get(0);
            this.maxLatency = sorted.get(sorted.size() - 1);
        }

        private static double percentile(List<Long> sorted, int pct) {
            if (sorted.isEmpty()) return 0;
            double idx = (pct / 100.0) * (sorted.size() - 1);
            int lo = (int) Math.floor(idx);
            int hi = (int) Math.ceil(idx);
            if (lo == hi) return sorted.get(lo);
            double frac = idx - lo;
            return sorted.get(lo) * (1 - frac) + sorted.get(hi) * frac;
        }

        @Override
        public String toString() {
            return String.format("Latency{n=%d, mean=%.1f, median=%.1f, p95=%.1f, min=%.0f, max=%.0f}",
                    count, meanLatency, medianLatency, p95Latency, minLatency, maxLatency);
        }
    }

    /**
     * Evaluation result for a single operator.
     */
    public static class EvaluationResult {
        public final String operatorName;
        public final int truePositives;
        public final int falsePositives;
        public final int falseNegatives;
        public final int duplicates;
        public final int totalOutput;
        public final int totalGroundTruth;

        /** Per-category breakdown */
        public final Map<MatchCategory, CategoryResult> categoryResults;

        /** The actual FP and FN details for debugging */
        public final List<OperatorMatch> falsePositiveDetails;
        public final List<GroundTruthMatch> falseNegativeDetails;

        /** Latency metrics for true positive matches */
        public final LatencyMetrics latencyMetrics;

        /** Legacy constructor (no latency) */
        public EvaluationResult(String operatorName, int tp, int fp, int fn, int dups,
                                int totalOutput, int totalGT,
                                Map<MatchCategory, CategoryResult> categoryResults,
                                List<OperatorMatch> fpDetails, List<GroundTruthMatch> fnDetails) {
            this(operatorName, tp, fp, fn, dups, totalOutput, totalGT,
                    categoryResults, fpDetails, fnDetails, new LatencyMetrics(List.of()));
        }

        /** Full constructor with latency metrics */
        public EvaluationResult(String operatorName, int tp, int fp, int fn, int dups,
                                int totalOutput, int totalGT,
                                Map<MatchCategory, CategoryResult> categoryResults,
                                List<OperatorMatch> fpDetails, List<GroundTruthMatch> fnDetails,
                                LatencyMetrics latencyMetrics) {
            this.operatorName = operatorName;
            this.truePositives = tp;
            this.falsePositives = fp;
            this.falseNegatives = fn;
            this.duplicates = dups;
            this.totalOutput = totalOutput;
            this.totalGroundTruth = totalGT;
            this.categoryResults = categoryResults;
            this.falsePositiveDetails = fpDetails;
            this.falseNegativeDetails = fnDetails;
            this.latencyMetrics = latencyMetrics;
        }

        /** Duplicate rate: total duplicates / |G| */
        public double duplicateRate() {
            if (totalGroundTruth == 0) return 0.0;
            return (double) duplicates / totalGroundTruth;
        }

        public double precision() {
            if (truePositives + falsePositives == 0) return 0.0;
            return (double) truePositives / (truePositives + falsePositives);
        }

        public double recall() {
            if (truePositives + falseNegatives == 0) return 0.0;
            return (double) truePositives / (truePositives + falseNegatives);
        }

        public double f1() {
            double p = precision(), r = recall();
            if (p + r == 0.0) return 0.0;
            return 2 * p * r / (p + r);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("╔══════════════════════════════════════════════════════════╗%n"));
            sb.append(String.format("║  EVALUATION: %-40s  ║%n", operatorName));
            sb.append(String.format("╠══════════════════════════════════════════════════════════╣%n"));
            sb.append(String.format("║  True Positives:   %4d / %d ground truth               ║%n", truePositives, totalGroundTruth));
            sb.append(String.format("║  False Positives:  %4d (spurious matches)               ║%n", falsePositives));
            sb.append(String.format("║  False Negatives:  %4d (missed matches)                 ║%n", falseNegatives));
            sb.append(String.format("║  Duplicates:       %4d                                  ║%n", duplicates));
            sb.append(String.format("║  Total Output:     %4d                                  ║%n", totalOutput));
            sb.append(String.format("╠══════════════════════════════════════════════════════════╣%n"));
            sb.append(String.format("║  Precision:      %.4f                                  ║%n", precision()));
            sb.append(String.format("║  Recall:         %.4f                                  ║%n", recall()));
            sb.append(String.format("║  F1 Score:       %.4f                                  ║%n", f1()));
            sb.append(String.format("║  Duplicate Rate: %.4f                                  ║%n", duplicateRate()));
            sb.append(String.format("╠══════════════════════════════════════════════════════════╣%n"));
            if (latencyMetrics != null && latencyMetrics.count > 0) {
                sb.append(String.format("║  Latency (event-time detection):                         ║%n"));
                sb.append(String.format("║    Mean:   %8.1f                                      ║%n", latencyMetrics.meanLatency));
                sb.append(String.format("║    Median: %8.1f                                      ║%n", latencyMetrics.medianLatency));
                sb.append(String.format("║    p95:    %8.1f                                      ║%n", latencyMetrics.p95Latency));
                sb.append(String.format("║    Min:    %8.0f  Max: %8.0f                        ║%n", latencyMetrics.minLatency, latencyMetrics.maxLatency));
                sb.append(String.format("╠══════════════════════════════════════════════════════════╣%n"));
            }
            sb.append(String.format("║  Per-Category Breakdown:                                 ║%n"));
            for (var entry : categoryResults.entrySet()) {
                CategoryResult cr = entry.getValue();
                sb.append(String.format("║    %-20s TP=%2d FN=%2d recall=%.2f             ║%n",
                        entry.getKey().name(), cr.truePositives, cr.falseNegatives, cr.recall()));
            }
            sb.append(String.format("╚══════════════════════════════════════════════════════════╝%n"));
            return sb.toString();
        }
    }

    /**
     * Per-category evaluation result.
     */
    public static class CategoryResult {
        public final MatchCategory category;
        public final int truePositives;
        public final int falseNegatives;
        public final int total;

        public CategoryResult(MatchCategory category, int tp, int fn, int total) {
            this.category = category;
            this.truePositives = tp;
            this.falseNegatives = fn;
            this.total = total;
        }

        public double recall() {
            if (total == 0) return 1.0; // No cases → vacuously true
            return (double) truePositives / total;
        }
    }

    // ==================== Core Evaluation ====================

    /**
     * Evaluate operator output against ground truth.
     *
     * @param operatorName     Name of the operator being evaluated
     * @param operatorOutput   The matches produced by the operator
     * @param groundTruth      The ground truth matches
     * @param negativeOrders   Orders that should NOT produce any output
     * @return Evaluation result with all metrics
     */
    public static EvaluationResult evaluate(
            String operatorName,
            List<OperatorMatch> operatorOutput,
            List<GroundTruthMatch> groundTruth,
            Set<String> negativeOrders) {

        // Build ground truth lookup by match key
        Set<String> gtKeys = groundTruth.stream()
                .map(GroundTruthMatch::matchKey)
                .collect(Collectors.toSet());

        // Build category lookup
        Map<String, MatchCategory> gtCategoryMap = new HashMap<>();
        for (GroundTruthMatch gt : groundTruth) {
            gtCategoryMap.put(gt.matchKey(), gt.category);
        }

        // Detect duplicates in operator output
        Map<String, Integer> outputKeyCounts = new HashMap<>();
        for (OperatorMatch om : operatorOutput) {
            outputKeyCounts.merge(om.matchKey(), 1, Integer::sum);
        }
        int duplicates = 0;
        for (int count : outputKeyCounts.values()) {
            if (count > 1) duplicates += (count - 1);
        }

        // Deduplicated output keys
        Set<String> outputKeys = new HashSet<>(outputKeyCounts.keySet());

        // True Positives: in both GT and output
        Set<String> tpKeys = new HashSet<>(outputKeys);
        tpKeys.retainAll(gtKeys);
        int tp = tpKeys.size();

        // False Positives: in output but NOT in GT
        List<OperatorMatch> fpDetails = new ArrayList<>();
        for (OperatorMatch om : operatorOutput) {
            if (!gtKeys.contains(om.matchKey())) {
                fpDetails.add(om);
            }
        }
        // Deduplicate FP details
        Set<String> fpSeen = new HashSet<>();
        fpDetails.removeIf(om -> !fpSeen.add(om.matchKey()));
        int fp = fpDetails.size();

        // Also count matches against negative orders as FP
        for (OperatorMatch om : operatorOutput) {
            if (negativeOrders.contains(om.orderId) && !fpDetails.contains(om)) {
                fpDetails.add(om);
                fp++;
            }
        }

        // False Negatives: in GT but NOT in output
        List<GroundTruthMatch> fnDetails = new ArrayList<>();
        for (GroundTruthMatch gt : groundTruth) {
            if (!outputKeys.contains(gt.matchKey())) {
                fnDetails.add(gt);
            }
        }
        int fn = fnDetails.size();

        // Per-category breakdown
        Map<MatchCategory, CategoryResult> categoryResults = new LinkedHashMap<>();
        for (MatchCategory cat : MatchCategory.values()) {
            List<GroundTruthMatch> catGT = groundTruth.stream()
                    .filter(gt -> gt.category == cat)
                    .collect(Collectors.toList());

            if (catGT.isEmpty()) continue;

            int catTP = 0, catFN = 0;
            for (GroundTruthMatch gt : catGT) {
                if (outputKeys.contains(gt.matchKey())) {
                    catTP++;
                } else {
                    catFN++;
                }
            }
            categoryResults.put(cat, new CategoryResult(cat, catTP, catFN, catGT.size()));
        }

        // ── Latency Metrics ──
        // For each true positive match, compute event-time detection latency:
        //   Latency(g) = emitEventTime - max(orderTime, shipmentTime)
        // Build lookup: matchKey → earliest emitEventTime from operator output
        Map<String, Long> firstEmitTime = new HashMap<>();
        for (OperatorMatch om : operatorOutput) {
            String mk = om.matchKey();
            if (tpKeys.contains(mk)) {
                firstEmitTime.merge(mk, om.emitEventTime, Math::min);
            }
        }

        // Build ground truth lookup by key for reference times
        Map<String, GroundTruthMatch> gtByKey = new HashMap<>();
        for (GroundTruthMatch gt : groundTruth) {
            gtByKey.put(gt.matchKey(), gt);
        }

        List<Long> latencies = new ArrayList<>();
        for (String tpKey : tpKeys) {
            GroundTruthMatch gt = gtByKey.get(tpKey);
            Long emitTime = firstEmitTime.get(tpKey);
            if (gt != null && emitTime != null) {
                long tTruth = Math.max(gt.orderTime, gt.shipmentTime);
                long latency = emitTime - tTruth;
                latencies.add(latency);
            }
        }
        LatencyMetrics latencyMetrics = new LatencyMetrics(latencies);

        return new EvaluationResult(operatorName, tp, fp, fn, duplicates,
                operatorOutput.size(), groundTruth.size(),
                categoryResults, fpDetails, fnDetails, latencyMetrics);
    }

    // ==================== Comparison Reporting ====================

    /**
     * Print a side-by-side comparison of multiple operators.
     */
    public static void printComparison(List<EvaluationResult> results) {
        System.out.println("\n╔════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    OPERATOR COMPARISON — GROUND TRUTH                     ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════════════╣");

        // Header
        System.out.printf("║  %-20s", "Metric");
        for (EvaluationResult r : results) {
            System.out.printf(" │ %-14s", r.operatorName);
        }
        System.out.println("  ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════════════╣");

        // Metrics rows
        printRow("True Positives", results, r -> String.valueOf(r.truePositives));
        printRow("False Positives", results, r -> String.valueOf(r.falsePositives));
        printRow("False Negatives", results, r -> String.valueOf(r.falseNegatives));
        printRow("Duplicates", results, r -> String.valueOf(r.duplicates));
        printRow("Precision", results, r -> String.format("%.4f", r.precision()));
        printRow("Recall", results, r -> String.format("%.4f", r.recall()));
        printRow("F1 Score", results, r -> String.format("%.4f", r.f1()));
        printRow("Duplicate Rate", results, r -> String.format("%.4f", r.duplicateRate()));

        System.out.println("╠════════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  Latency Metrics (event-time detection):                                  ║");
        printRow("Mean Latency", results, r -> r.latencyMetrics.count > 0
                ? String.format("%.1f", r.latencyMetrics.meanLatency) : "N/A");
        printRow("Median Latency", results, r -> r.latencyMetrics.count > 0
                ? String.format("%.1f", r.latencyMetrics.medianLatency) : "N/A");
        printRow("p95 Latency", results, r -> r.latencyMetrics.count > 0
                ? String.format("%.1f", r.latencyMetrics.p95Latency) : "N/A");

        System.out.println("╠════════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  Per-Category Recall:                                                     ║");

        // Get all categories present
        Set<MatchCategory> allCats = new LinkedHashSet<>();
        for (EvaluationResult r : results) {
            allCats.addAll(r.categoryResults.keySet());
        }

        for (MatchCategory cat : allCats) {
            System.out.printf("║    %-18s", cat.name());
            for (EvaluationResult r : results) {
                CategoryResult cr = r.categoryResults.get(cat);
                if (cr != null) {
                    System.out.printf(" │ %2d/%-2d (%.2f)  ", cr.truePositives, cr.total, cr.recall());
                } else {
                    System.out.printf(" │ %-14s", "N/A");
                }
            }
            System.out.println("  ║");
        }

        System.out.println("╚════════════════════════════════════════════════════════════════════════════╝");
    }

    private static void printRow(String label, List<EvaluationResult> results,
                                  java.util.function.Function<EvaluationResult, String> extractor) {
        System.out.printf("║  %-20s", label);
        for (EvaluationResult r : results) {
            System.out.printf(" │ %-14s", extractor.apply(r));
        }
        System.out.println("  ║");
    }

    /**
     * Print detailed false positive and false negative analysis.
     * Useful for understanding WHERE each operator fails.
     */
    public static void printDetailedAnalysis(EvaluationResult result) {
        System.out.println("\n═══ Detailed Analysis: " + result.operatorName + " ═══");

        if (!result.falsePositiveDetails.isEmpty()) {
            System.out.println("\n  FALSE POSITIVES (spurious matches):");
            for (OperatorMatch fp : result.falsePositiveDetails) {
                System.out.println("    FP: " + fp);
            }
        }

        if (!result.falseNegativeDetails.isEmpty()) {
            System.out.println("\n  FALSE NEGATIVES (missed ground truth):");
            for (GroundTruthMatch fn : result.falseNegativeDetails) {
                System.out.println("    FN: " + fn);
            }
        }

        // Analyze conflict direction (past vs future)
        if (!result.falsePositiveDetails.isEmpty()) {
            System.out.println("\n  CONFLICT DIRECTION ANALYSIS:");
            for (OperatorMatch fp : result.falsePositiveDetails) {
                // A false positive means the operator expanded and caught something extra
                System.out.println("    " + fp.orderId + ": Expansion introduced spurious match " +
                        fp.paymentId + "/" + fp.shipmentId);
            }
        }
    }

    /**
     * Export comparison results to CSV.
     */
    public static String toCsv(List<EvaluationResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("operator,TP,FP,FN,duplicates,precision,recall,f1,duplicate_rate,mean_latency,median_latency,p95_latency,total_output,total_gt\n");
        for (EvaluationResult r : results) {
            sb.append(String.format("%s,%d,%d,%d,%d,%.4f,%.4f,%.4f,%.4f,%.1f,%.1f,%.1f,%d,%d%n",
                    r.operatorName, r.truePositives, r.falsePositives, r.falseNegatives,
                    r.duplicates, r.precision(), r.recall(), r.f1(), r.duplicateRate(),
                    r.latencyMetrics.meanLatency, r.latencyMetrics.medianLatency,
                    r.latencyMetrics.p95Latency,
                    r.totalOutput, r.totalGroundTruth));
        }

        // Per-category
        sb.append("\noperator,category,TP,FN,total,recall\n");
        for (EvaluationResult r : results) {
            for (var entry : r.categoryResults.entrySet()) {
                CategoryResult cr = entry.getValue();
                sb.append(String.format("%s,%s,%d,%d,%d,%.4f%n",
                        r.operatorName, cr.category.name(),
                        cr.truePositives, cr.falseNegatives, cr.total, cr.recall()));
            }
        }

        return sb.toString();
    }
}
