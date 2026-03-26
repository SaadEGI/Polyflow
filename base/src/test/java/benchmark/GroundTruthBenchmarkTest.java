package benchmark;

import benchmark.model.*;
import benchmark.model.GroundTruthMatch.MatchCategory;
import benchmark.GroundTruthEvaluator.*;

import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ground Truth Benchmark — Core thesis evaluation.
 *
 * <h2>What This Test Does</h2>
 * <ol>
 *   <li>Generates synthetic dataset (ground truth first, then input streams)</li>
 *   <li>Runs two operator strategies against the same input</li>
 *   <li>Compares each operator's output against ground truth</li>
 *   <li>Reports: TP, FP, FN, duplicates, precision, recall, F1, duplicate rate, latency</li>
 *   <li>Breaks down results by conflict category</li>
 * </ol>
 *
 * <h2>Operators Compared</h2>
 * <table>
 *   <tr><th>Operator</th><th>Invariant</th><th>Window Type</th></tr>
 *   <tr><td>Interval Join (fixed)</td><td>∀t: W_t = [t-β, t+α]</td><td>Data-independent</td></tr>
 *   <tr><td>Elastic Validation (expanding)</td><td>Window adapts for integrity</td><td>Data-dependent</td></tr>
 * </table>
 *
 * <h2>Research Question</h2>
 * <p>"What is the invariant of the integrity-aware window operator, and how does
 * its accuracy compare against data-independent operators on streams with
 * controlled integrity conflicts?"</p>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GroundTruthBenchmarkTest {

    private static SyntheticDatasetGenerator generator;
    private static List<OrderEvent> orders;
    private static List<PaymentEvent> payments;
    private static List<ShipmentEvent> shipments;
    private static List<GroundTruthMatch> groundTruth;
    private static Set<String> negativeOrders;

    // Results from each operator
    private static List<OperatorMatch> intervalJoinOutput;
    private static List<OperatorMatch> elasticOutput;

    // Window parameters (must match SyntheticDatasetGenerator)
    private static final long ALPHA = SyntheticDatasetGenerator.ALPHA;
    private static final long BETA = SyntheticDatasetGenerator.BETA;
    private static final long U = SyntheticDatasetGenerator.U;
    private static final long V = SyntheticDatasetGenerator.V;
    private static final long G = SyntheticDatasetGenerator.G;

    @BeforeAll
    static void generateDataset() throws IOException {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("  GROUND TRUTH BENCHMARK — Thesis Evaluation");
        System.out.println("=".repeat(70));

        generator = new SyntheticDatasetGenerator();
        generator.generate();

        orders = generator.getOrders();
        payments = generator.getPayments();
        shipments = generator.getShipments();
        groundTruth = generator.getGroundTruth();
        negativeOrders = generator.getNegativeOrders();

        // Write dataset to disk
        Path outputDir = Path.of("benchmark_dataset");
        generator.writeToCsv(outputDir);

        // Basic sanity checks
        assertTrue(generator.getTotalEventCount() >= 200,
                "Dataset should have at least 200 events, got: " + generator.getTotalEventCount());
        assertTrue(generator.getTotalEventCount() <= 400,
                "Dataset should have at most 400 events, got: " + generator.getTotalEventCount());
        assertFalse(groundTruth.isEmpty(), "Ground truth must not be empty");
        assertFalse(negativeOrders.isEmpty(), "Must have negative cases");
    }

    // ==================== OPERATOR 1: Classic Interval Join ====================

    /**
     * Simulates a classic interval join operator.
     *
     * <p>Invariant: ∀t: W_t = [t - β, t + α] — all windows have same size (data-independent)</p>
     *
     * <p>Temporal semantics:
     * - Only considers PAID payments (status filter)
     * - No 1-to-1 restriction (Cartesian product)</p>
     */
    @Test
    @Order(1)
    @DisplayName("Operator 1: Interval Join (fixed-size, data-independent, status-aware)")
    void runIntervalJoin() {
        System.out.println("\n══════ OPERATOR 2: Interval Join ══════");
        System.out.println("Invariant: ∀t: W_t = [t - β, t + α] — same size for all");
        System.out.println("Window: [tO - " + BETA + ", tO + " + ALPHA + "], status filter: PAID only");

        intervalJoinOutput = new ArrayList<>();

        Map<String, List<PaymentEvent>> paymentsByOrder = payments.stream()
                .collect(Collectors.groupingBy(p -> p.orderId));
        Map<String, List<ShipmentEvent>> shipmentsByOrder = shipments.stream()
                .collect(Collectors.groupingBy(s -> s.orderId));

        for (OrderEvent order : orders) {
            long windowStart = order.timestamp - BETA;
            long windowEnd = order.timestamp + ALPHA;

            // Only PAID payments within window
            List<PaymentEvent> matchedPayments = paymentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(p -> p.isPaid())
                    .filter(p -> p.timestamp >= windowStart && p.timestamp <= windowEnd)
                    .collect(Collectors.toList());

            // All shipments within window
            List<ShipmentEvent> matchedShipments = shipmentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(s -> s.timestamp >= windowStart && s.timestamp <= windowEnd)
                    .collect(Collectors.toList());

            // Cartesian product
            for (PaymentEvent p : matchedPayments) {
                for (ShipmentEvent s : matchedShipments) {
                    intervalJoinOutput.add(new OperatorMatch(
                            order.orderId, p.paymentId, s.shipmentId,
                            Math.max(p.timestamp, s.timestamp)));
                }
            }
        }

        System.out.println("Output: " + intervalJoinOutput.size() + " matches");

        EvaluationResult result = GroundTruthEvaluator.evaluate(
                "IntervalJoin", intervalJoinOutput, groundTruth, negativeOrders);
        System.out.println(result);
    }

    // ==================== OPERATOR 2: Elastic Validation ====================

    /**
     * Simulates the elastic validation operator (data-dependent windows).
     *
     * <p>Invariant proposal: "Window contains at least one valid (PAID) payment
     * and at least one shipment for the same order."</p>
     *
     * <p>Window semantics:
     * - Base window: [tO, tO + V] for payment validation
     * - If no PAID payment found, expand to [tO, tO + V + G] (grace)
     * - Content window: [tO, tO + U] for shipment matching
     * - Output only when BOTH conditions met</p>
     *
     * <p>This is data-dependent because the window expansion depends on
     * whether a payment was found in the base window.</p>
     */
    @Test
    @Order(2)
    @DisplayName("Operator 2: Elastic Validation (data-dependent, integrity-aware)")
    void runElasticValidation() {
        System.out.println("\n══════ OPERATOR 3: Elastic Validation ══════");
        System.out.println("Invariant: Window adapts until valid payment + shipment found");
        System.out.println("Base payment window: [tO, tO + " + V + "]");
        System.out.println("Grace expansion:     [tO, tO + " + (V + G) + "]");
        System.out.println("Content window:      [tO, tO + " + U + "]");

        elasticOutput = new ArrayList<>();

        Map<String, List<PaymentEvent>> paymentsByOrder = payments.stream()
                .collect(Collectors.groupingBy(p -> p.orderId));
        Map<String, List<ShipmentEvent>> shipmentsByOrder = shipments.stream()
                .collect(Collectors.groupingBy(s -> s.orderId));

        for (OrderEvent order : orders) {
            long tO = order.timestamp;

            // STEP 1: Try base validation window [tO, tO + V]
            List<PaymentEvent> basePayments = paymentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(p -> p.isPaid())
                    .filter(p -> p.timestamp >= tO && p.timestamp <= tO + V)
                    .collect(Collectors.toList());

            List<PaymentEvent> validPayments;

            if (!basePayments.isEmpty()) {
                // Found payment in base window — no expansion needed
                validPayments = basePayments;
            } else {
                // STEP 2: Expand to grace period [tO, tO + V + G]
                validPayments = paymentsByOrder.getOrDefault(order.orderId, List.of())
                        .stream()
                        .filter(p -> p.isPaid())
                        .filter(p -> p.timestamp >= tO && p.timestamp <= tO + V + G)
                        .collect(Collectors.toList());
            }

            // STEP 3: Content window for shipments [tO, tO + U]
            List<ShipmentEvent> matchedShipments = shipmentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(s -> s.timestamp >= tO && s.timestamp <= tO + U)
                    .collect(Collectors.toList());

            // STEP 4: Only output if BOTH validation AND content conditions met
            if (!validPayments.isEmpty() && !matchedShipments.isEmpty()) {
                // Use earliest valid payment (selection strategy)
                // NOTE: This is one possible selection — thesis should analyze alternatives
                PaymentEvent selectedPayment = validPayments.stream()
                        .min(Comparator.comparingLong(p -> p.timestamp))
                        .orElse(null);

                // Cartesian product with all valid shipments
                for (ShipmentEvent s : matchedShipments) {
                    elasticOutput.add(new OperatorMatch(
                            order.orderId, selectedPayment.paymentId, s.shipmentId,
                            Math.max(selectedPayment.timestamp, s.timestamp)));
                }
            }
        }

        System.out.println("Output: " + elasticOutput.size() + " matches");

        EvaluationResult result = GroundTruthEvaluator.evaluate(
                "Elastic", elasticOutput, groundTruth, negativeOrders);
        System.out.println(result);
    }

    // ==================== Comparison ====================

    @Test
    @Order(3)
    @DisplayName("Comparison: All operators vs ground truth")
    void compareAllOperators() {
        assertNotNull(intervalJoinOutput, "Run interval join test first");
        assertNotNull(elasticOutput, "Run elastic test first");

        List<EvaluationResult> results = List.of(
                GroundTruthEvaluator.evaluate("IntervalJoin", intervalJoinOutput, groundTruth, negativeOrders),
                GroundTruthEvaluator.evaluate("Elastic", elasticOutput, groundTruth, negativeOrders)
        );

        GroundTruthEvaluator.printComparison(results);

        // Write comparison CSV
        try {
            Path csvPath = Path.of("benchmark_dataset", "evaluation_results.csv");
            Files.writeString(csvPath, GroundTruthEvaluator.toCsv(results));
            System.out.println("\n✅ Results written to: " + csvPath.toAbsolutePath());
        } catch (IOException e) {
            System.err.println("Warning: Could not write CSV: " + e.getMessage());
        }
    }

    // ==================== Detailed Analysis ====================

    @Test
    @Order(4)
    @DisplayName("Detailed: Where does each operator fail?")
    void detailedAnalysis() {
        assertNotNull(intervalJoinOutput, "Run interval join test first");
        assertNotNull(elasticOutput, "Run elastic test first");

        EvaluationResult ijResult = GroundTruthEvaluator.evaluate(
                "IntervalJoin", intervalJoinOutput, groundTruth, negativeOrders);
        EvaluationResult elResult = GroundTruthEvaluator.evaluate(
                "Elastic", elasticOutput, groundTruth, negativeOrders);

        GroundTruthEvaluator.printDetailedAnalysis(ijResult);
        GroundTruthEvaluator.printDetailedAnalysis(elResult);
    }

    // ==================== Window Invariant Analysis ====================

    @Test
    @Order(5)
    @DisplayName("Analysis: Window invariant properties")
    void analyzeWindowInvariants() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║            WINDOW INVARIANT ANALYSIS                         ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║                                                              ║");
        System.out.println("║  Interval Join (fixed):                                      ║");
        System.out.println("║    Invariant: ∀t: size(W_t) = α + β = " + (ALPHA + BETA) + "                  ║");
        System.out.println("║    Property: DATA-INDEPENDENT                                ║");
        System.out.println("║    Can define window without seeing data: YES                ║");
        System.out.println("║                                                              ║");
        System.out.println("║  Elastic Validation (expanding):                             ║");
        System.out.println("║    Invariant: W_t contains ≥1 valid payment + ≥1 shipment   ║");
        System.out.println("║    Property: DATA-DEPENDENT                                  ║");
        System.out.println("║    Can define window without seeing data: NO                 ║");
        System.out.println("║                                                              ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");

        // Analyze actual window sizes for elastic operator
        Map<String, List<PaymentEvent>> paymentsByOrder = payments.stream()
                .collect(Collectors.groupingBy(p -> p.orderId));

        int baseWindowUsed = 0;
        int graceWindowUsed = 0;
        int noPaymentFound = 0;

        for (OrderEvent order : orders) {
            long tO = order.timestamp;

            boolean hasBasePayment = paymentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(p -> p.isPaid())
                    .anyMatch(p -> p.timestamp >= tO && p.timestamp <= tO + V);

            boolean hasGracePayment = paymentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(p -> p.isPaid())
                    .anyMatch(p -> p.timestamp > tO + V && p.timestamp <= tO + V + G);

            if (hasBasePayment) {
                baseWindowUsed++;
            } else if (hasGracePayment) {
                graceWindowUsed++;
            } else {
                noPaymentFound++;
            }
        }

        System.out.println("║  Elastic Window Size Distribution:                           ║");
        System.out.printf("║    Base window [tO, tO+%d] sufficient:  %4d orders          ║%n", V, baseWindowUsed);
        System.out.printf("║    Grace expansion [tO, tO+%d] needed:  %4d orders          ║%n", V + G, graceWindowUsed);
        System.out.printf("║    No valid payment found:             %4d orders          ║%n", noPaymentFound);
        System.out.println("║                                                              ║");
        System.out.println("║  ⚠ When grace expansion is used, the window CHANGES SIZE.   ║");
        System.out.println("║  This means the interval join invariant is BROKEN.           ║");
        System.out.println("║  The elastic invariant is: \"output only if integrity holds\" ║");
        System.out.println("║                                                              ║");

        // Analyze expansion conflicts (past vs future)
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  EXPANSION CONFLICT ANALYSIS (Past vs Future):               ║");

        int pastConflicts = 0;
        int futureConflicts = 0;

        Map<String, List<ShipmentEvent>> shipmentsByOrder = shipments.stream()
                .collect(Collectors.groupingBy(s -> s.orderId));

        for (OrderEvent order : orders) {
            long tO = order.timestamp;

            // Check if expanding to grace introduces extra shipments not in base window
            List<ShipmentEvent> baseShipments = shipmentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(s -> s.timestamp >= tO && s.timestamp <= tO + V)
                    .collect(Collectors.toList());

            List<ShipmentEvent> expandedShipments = shipmentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(s -> s.timestamp >= tO && s.timestamp <= tO + V + G)
                    .collect(Collectors.toList());

            if (expandedShipments.size() > baseShipments.size()) {
                futureConflicts++; // Expanding forward introduced extra joins
            }

            // Check backward: are there earlier payments that could create duplicate matches?
            long earliestPayment = paymentsByOrder.getOrDefault(order.orderId, List.of())
                    .stream()
                    .filter(p -> p.isPaid())
                    .mapToLong(p -> p.timestamp)
                    .min().orElse(Long.MAX_VALUE);

            if (earliestPayment < tO) {
                pastConflicts++; // Payment before order time → past conflict
            }
        }

        System.out.printf("║    Past conflicts  (backward expansion risk):  %4d          ║%n", pastConflicts);
        System.out.printf("║    Future conflicts (forward expansion risk):  %4d          ║%n", futureConflicts);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    // ==================== Dataset Validation ====================

    @Test
    @Order(6)
    @DisplayName("Validation: Dataset integrity checks")
    void validateDataset() {
        System.out.println("\n══════ DATASET INTEGRITY CHECKS ══════");

        // 1. All ground truth orders must exist in orders stream
        Set<String> orderIds = orders.stream().map(o -> o.orderId).collect(Collectors.toSet());
        for (GroundTruthMatch gt : groundTruth) {
            assertTrue(orderIds.contains(gt.orderId),
                    "Ground truth references missing order: " + gt.orderId);
        }
        System.out.println("✅ All ground truth orders exist in order stream");

        // 2. All ground truth payments must exist in payments stream
        Set<String> paymentIds = payments.stream().map(p -> p.paymentId).collect(Collectors.toSet());
        for (GroundTruthMatch gt : groundTruth) {
            assertTrue(paymentIds.contains(gt.paymentId),
                    "Ground truth references missing payment: " + gt.paymentId);
        }
        System.out.println("✅ All ground truth payments exist in payment stream");

        // 3. All ground truth shipments must exist in shipments stream
        Set<String> shipmentIds = shipments.stream().map(s -> s.shipmentId).collect(Collectors.toSet());
        for (GroundTruthMatch gt : groundTruth) {
            assertTrue(shipmentIds.contains(gt.shipmentId),
                    "Ground truth references missing shipment: " + gt.shipmentId);
        }
        System.out.println("✅ All ground truth shipments exist in shipment stream");

        // 4. Negative orders should NOT appear in ground truth
        Set<String> gtOrderIds = groundTruth.stream().map(gt -> gt.orderId).collect(Collectors.toSet());
        for (String negOrder : negativeOrders) {
            assertFalse(gtOrderIds.contains(negOrder),
                    "Negative order " + negOrder + " should not appear in ground truth");
        }
        System.out.println("✅ Negative orders excluded from ground truth");

        // 5. Ground truth timestamps must be consistent
        for (GroundTruthMatch gt : groundTruth) {
            // Payment must reference correct order
            PaymentEvent payment = payments.stream()
                    .filter(p -> p.paymentId.equals(gt.paymentId))
                    .findFirst().orElse(null);
            assertNotNull(payment, "Payment " + gt.paymentId + " not found");
            assertEquals(gt.orderId, payment.orderId,
                    "Payment " + gt.paymentId + " references wrong order");

            // Shipment must reference correct order
            ShipmentEvent shipment = shipments.stream()
                    .filter(s -> s.shipmentId.equals(gt.shipmentId))
                    .findFirst().orElse(null);
            assertNotNull(shipment, "Shipment " + gt.shipmentId + " not found");
            assertEquals(gt.orderId, shipment.orderId,
                    "Shipment " + gt.shipmentId + " references wrong order");
        }
        System.out.println("✅ All foreign keys are consistent");

        // 6. Check that duplicate payments actually exist for DUPLICATE_PAYMENT cases
        List<GroundTruthMatch> dupCases = groundTruth.stream()
                .filter(gt -> gt.category == MatchCategory.DUPLICATE_PAYMENT || gt.category == MatchCategory.COMPLEX)
                .collect(Collectors.toList());
        for (GroundTruthMatch gt : dupCases) {
            long paymentCount = payments.stream()
                    .filter(p -> p.orderId.equals(gt.orderId))
                    .count();
            assertTrue(paymentCount > 1,
                    "DUPLICATE_PAYMENT case " + gt.orderId + " should have >1 payment, got: " + paymentCount);
        }
        System.out.println("✅ Duplicate payment cases verified");

        // 7. Check that multiple shipment cases actually exist
        List<GroundTruthMatch> multiShipCases = groundTruth.stream()
                .filter(gt -> gt.category == MatchCategory.MULTIPLE_SHIPMENTS || gt.category == MatchCategory.COMPLEX)
                .collect(Collectors.toList());
        Set<String> multiShipOrders = multiShipCases.stream()
                .map(gt -> gt.orderId).collect(Collectors.toSet());
        for (String orderId : multiShipOrders) {
            long shipCount = shipments.stream()
                    .filter(s -> s.orderId.equals(orderId))
                    .count();
            assertTrue(shipCount > 1,
                    "MULTIPLE_SHIPMENTS case " + orderId + " should have >1 shipment, got: " + shipCount);
        }
        System.out.println("✅ Multiple shipment cases verified");

        // 8. Total event count in range
        int total = generator.getTotalEventCount();
        System.out.println("\n  Total events: " + total + " (target: 200-300)");
        assertTrue(total >= 200 && total <= 400,
                "Expected 200-400 events, got: " + total);
        System.out.println("✅ Event count within target range");
    }
}
