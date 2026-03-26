package benchmark;

import benchmark.model.*;
import benchmark.model.GroundTruthMatch.MatchCategory;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Generates a synthetic dataset with controlled conflicts for thesis evaluation.
 *
 * <h2>Methodology (as specified by thesis committee)</h2>
 * <ol>
 *   <li><b>Step 1:</b> Define ground truth output — the perfect (Order, Payment, Shipment) triples</li>
 *   <li><b>Step 2:</b> Generate input streams that lead to that ground truth</li>
 *   <li><b>Step 3:</b> Inject controlled conflicts: duplicates, late arrivals, missing events</li>
 * </ol>
 *
 * <h2>Key Design Decisions</h2>
 * <ul>
 *   <li><b>No 1-to-1 assumption:</b> Multiple payments per order, multiple shipments per order</li>
 *   <li><b>Cartesian products can appear:</b> Expanding a window may introduce extra joins</li>
 *   <li><b>Data-dependent conflicts:</b> Fixing one conflict can introduce another</li>
 * </ul>
 *
 * <h2>Conflict Categories (200-300 events)</h2>
 * <table>
 *   <tr><th>Category</th><th>Count</th><th>Description</th></tr>
 *   <tr><td>NORMAL</td><td>~20</td><td>All events within standard interval window</td></tr>
 *   <tr><td>LATE_PAYMENT</td><td>~10</td><td>Payment outside base window, within grace</td></tr>
 *   <tr><td>LATE_SHIPMENT</td><td>~8</td><td>Shipment near content window boundary</td></tr>
 *   <tr><td>DUPLICATE_PAYMENT</td><td>~10</td><td>Multiple payments, only one correct</td></tr>
 *   <tr><td>MULTIPLE_SHIPMENTS</td><td>~8</td><td>Multiple shipments per order (each valid)</td></tr>
 *   <tr><td>COMPLEX</td><td>~6</td><td>Both duplicate payments AND multiple shipments</td></tr>
 *   <tr><td>NO_SHIPMENT</td><td>~5</td><td>Payment exists but no shipment (negative case)</td></tr>
 *   <tr><td>NO_PAYMENT</td><td>~5</td><td>Shipment exists but no payment (negative case)</td></tr>
 * </table>
 */
public class SyntheticDatasetGenerator {

    // ==================== Window Parameters ====================
    // These MUST match the operator configurations used in evaluation

    /** Content window: shipment must arrive within [tO, tO + U] */
    public static final long U = 100;

    /** Base validation window for payment */
    public static final long V = 50;

    /** Grace period for late payments */
    public static final long G = 30;

    /** Validation deadline: payment must arrive within [tO, tO + V + G] = [tO, tO + 80] */
    public static final long VALIDATION_DEADLINE = V + G;

    /** Classic interval join lower bound (β): how far back from order time */
    public static final long BETA = 0;

    /** Classic interval join upper bound (α): how far forward from order time */
    public static final long ALPHA = 100;

    // ==================== Generation Parameters ====================

    /** Time spacing between consecutive orders */
    private static final long ORDER_SPACING = 10;

    /** Random seed for reproducibility */
    private static final long SEED = 42;

    private final Random rng;
    private final String[] carriers = {"FedEx", "UPS", "DHL"};

    private int orderCounter = 0;
    private int paymentCounter = 0;
    private int shipmentCounter = 0;
    private int customerCounter = 0;

    // ==================== Generated Data ====================

    private final List<OrderEvent> orders = new ArrayList<>();
    private final List<PaymentEvent> payments = new ArrayList<>();
    private final List<ShipmentEvent> shipments = new ArrayList<>();
    private final List<GroundTruthMatch> groundTruth = new ArrayList<>();

    /** Tracks which orders are "negative" — should produce NO output */
    private final Set<String> negativeOrders = new HashSet<>();

    public SyntheticDatasetGenerator() {
        this(SEED);
    }

    public SyntheticDatasetGenerator(long seed) {
        this.rng = new Random(seed);
    }

    // ==================== Main Generation Logic ====================

    /**
     * Generate the full dataset. This method:
     * 1. Defines ground truth matches (backward approach)
     * 2. Creates order events
     * 3. Creates payment events (including duplicates)
     * 4. Creates shipment events (including multiple per order)
     * 5. Injects negative cases (missing payment, missing shipment)
     */
    public void generate() {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║     SYNTHETIC DATASET GENERATOR — Ground Truth First        ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Window params: U=" + U + ", V=" + V + ", G=" + G + 
                           ", α=" + ALPHA + ", β=" + BETA + "            ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // STEP 1: Generate NORMAL cases (order + 1 payment + 1 shipment, all within window)
        generateNormalCases(20);

        // STEP 2: Generate LATE_PAYMENT cases (payment outside V but within V+G)
        generateLatePaymentCases(10);

        // STEP 3: Generate LATE_SHIPMENT cases (shipment near U boundary)
        generateLateShipmentCases(8);

        // STEP 4: Generate DUPLICATE_PAYMENT cases (multiple payments, one correct)
        generateDuplicatePaymentCases(10);

        // STEP 5: Generate MULTIPLE_SHIPMENTS cases (multiple shipments per order, each valid)
        generateMultipleShipmentCases(8);

        // STEP 6: Generate COMPLEX cases (duplicate payments AND multiple shipments)
        generateComplexCases(6);

        // STEP 7: Generate NO_SHIPMENT cases (payment exists, no shipment → negative)
        generateNoShipmentCases(5);

        // STEP 8: Generate NO_PAYMENT cases (shipment exists, no payment → negative)
        generateNoPaymentCases(5);

        // Sort all streams by timestamp (as they would arrive in a real stream)
        orders.sort(Comparator.comparingLong(o -> o.timestamp));
        payments.sort(Comparator.comparingLong(p -> p.timestamp));
        shipments.sort(Comparator.comparingLong(s -> s.timestamp));

        printSummary();
    }

    // ==================== Case Generators ====================

    /**
     * NORMAL: All events well within the standard interval window.
     * These should be matched by ALL operators (sliding, interval join, elastic).
     */
    private void generateNormalCases(int count) {
        for (int i = 0; i < count; i++) {
            String orderId = nextOrderId();
            String customerId = nextCustomerId();
            long orderTime = nextOrderTime();
            double amount = randomAmount();

            // Payment: well within V (not even near grace boundary)
            long paymentTime = orderTime + randomBetween(5, (int) V - 5);
            String paymentId = nextPaymentId();

            // Shipment: well within U
            long shipmentTime = orderTime + randomBetween(10, (int) U - 10);
            String shipmentId = nextShipmentId();
            String carrier = randomCarrier();

            orders.add(new OrderEvent(orderId, customerId, amount, orderTime));
            payments.add(new PaymentEvent(paymentId, orderId, "PAID", amount, paymentTime));
            shipments.add(new ShipmentEvent(shipmentId, orderId, carrier, shipmentTime));
            groundTruth.add(new GroundTruthMatch(orderId, paymentId, shipmentId,
                    orderTime, paymentTime, shipmentTime, MatchCategory.NORMAL));
        }
    }

    /**
     * LATE_PAYMENT: Payment arrives after V but before V+G (grace period).
     * Classic interval join MISSES these. Elastic operator should CATCH them.
     */
    private void generateLatePaymentCases(int count) {
        for (int i = 0; i < count; i++) {
            String orderId = nextOrderId();
            String customerId = nextCustomerId();
            long orderTime = nextOrderTime();
            double amount = randomAmount();

            // Payment: outside V but within V+G
            long paymentTime = orderTime + randomBetween((int) V + 1, (int) VALIDATION_DEADLINE);
            String paymentId = nextPaymentId();

            // Shipment: within U
            long shipmentTime = orderTime + randomBetween(10, (int) U - 5);
            String shipmentId = nextShipmentId();
            String carrier = randomCarrier();

            orders.add(new OrderEvent(orderId, customerId, amount, orderTime));
            payments.add(new PaymentEvent(paymentId, orderId, "PAID", amount, paymentTime));
            shipments.add(new ShipmentEvent(shipmentId, orderId, carrier, shipmentTime));
            groundTruth.add(new GroundTruthMatch(orderId, paymentId, shipmentId,
                    orderTime, paymentTime, shipmentTime, MatchCategory.LATE_PAYMENT));
        }
    }

    /**
     * LATE_SHIPMENT: Shipment arrives near or at the content window boundary (U).
     * Tests sensitivity of the content window.
     */
    private void generateLateShipmentCases(int count) {
        for (int i = 0; i < count; i++) {
            String orderId = nextOrderId();
            String customerId = nextCustomerId();
            long orderTime = nextOrderTime();
            double amount = randomAmount();

            // Payment: within V
            long paymentTime = orderTime + randomBetween(5, (int) V);
            String paymentId = nextPaymentId();

            // Shipment: near U boundary (within last 15% of U)
            long shipmentTime = orderTime + randomBetween((int) (U * 0.85), (int) U);
            String shipmentId = nextShipmentId();
            String carrier = randomCarrier();

            orders.add(new OrderEvent(orderId, customerId, amount, orderTime));
            payments.add(new PaymentEvent(paymentId, orderId, "PAID", amount, paymentTime));
            shipments.add(new ShipmentEvent(shipmentId, orderId, carrier, shipmentTime));
            groundTruth.add(new GroundTruthMatch(orderId, paymentId, shipmentId,
                    orderTime, paymentTime, shipmentTime, MatchCategory.LATE_SHIPMENT));
        }
    }

    /**
     * DUPLICATE_PAYMENT: Multiple payments for same order. Only one is "correct".
     * 
     * <p>This is the key scenario discussed in the meeting:
     * removing 1-to-1 assumption means Cartesian product can appear.
     * The ground truth defines which payment is correct.</p>
     * 
     * <p>Conflict analysis:
     * - Past conflict: early duplicate causes extra match
     * - Future conflict: late duplicate causes extra match after expansion</p>
     */
    private void generateDuplicatePaymentCases(int count) {
        for (int i = 0; i < count; i++) {
            String orderId = nextOrderId();
            String customerId = nextCustomerId();
            long orderTime = nextOrderTime();
            double amount = randomAmount();

            // CORRECT payment: within V
            long correctPaymentTime = orderTime + randomBetween(10, (int) V);
            String correctPaymentId = nextPaymentId();
            payments.add(new PaymentEvent(correctPaymentId, orderId, "PAID", amount, correctPaymentTime));

            // DUPLICATE payment(s): also references same order
            int numDuplicates = rng.nextBoolean() ? 1 : 2;
            for (int d = 0; d < numDuplicates; d++) {
                String dupPaymentId = nextPaymentId();
                long dupPaymentTime;
                String dupStatus;

                // 50% chance: duplicate is a FAILED retry (less ambiguous)
                // 50% chance: duplicate is also PAID (creates real ambiguity)
                if (rng.nextBoolean()) {
                    dupPaymentTime = orderTime + randomBetween(1, (int) VALIDATION_DEADLINE);
                    dupStatus = "FAILED";
                } else {
                    dupPaymentTime = orderTime + randomBetween(1, (int) VALIDATION_DEADLINE);
                    dupStatus = "PAID";
                }
                payments.add(new PaymentEvent(dupPaymentId, orderId, dupStatus, amount, dupPaymentTime));
            }

            // Shipment: within U
            long shipmentTime = orderTime + randomBetween(10, (int) U - 5);
            String shipmentId = nextShipmentId();
            String carrier = randomCarrier();

            orders.add(new OrderEvent(orderId, customerId, amount, orderTime));
            shipments.add(new ShipmentEvent(shipmentId, orderId, carrier, shipmentTime));
            groundTruth.add(new GroundTruthMatch(orderId, correctPaymentId, shipmentId,
                    orderTime, correctPaymentTime, shipmentTime, MatchCategory.DUPLICATE_PAYMENT));
        }
    }

    /**
     * MULTIPLE_SHIPMENTS: Multiple shipments for same order — each is a valid match.
     * 
     * <p>This is realistic: partial shipments, split deliveries.
     * Ground truth contains MULTIPLE entries for same order.
     * Cartesian product with payment is expected.</p>
     */
    private void generateMultipleShipmentCases(int count) {
        for (int i = 0; i < count; i++) {
            String orderId = nextOrderId();
            String customerId = nextCustomerId();
            long orderTime = nextOrderTime();
            double amount = randomAmount();

            // Payment: within V
            long paymentTime = orderTime + randomBetween(5, (int) V);
            String paymentId = nextPaymentId();
            payments.add(new PaymentEvent(paymentId, orderId, "PAID", amount, paymentTime));

            // Multiple shipments (2-3), each within U
            int numShipments = 2 + rng.nextInt(2); // 2 or 3
            for (int s = 0; s < numShipments; s++) {
                String shipmentId = nextShipmentId();
                long shipmentTime = orderTime + randomBetween(10, (int) U - 5);
                String carrier = randomCarrier();

                shipments.add(new ShipmentEvent(shipmentId, orderId, carrier, shipmentTime));

                // Each shipment is a valid ground truth match
                groundTruth.add(new GroundTruthMatch(orderId, paymentId, shipmentId,
                        orderTime, paymentTime, shipmentTime, MatchCategory.MULTIPLE_SHIPMENTS));
            }

            orders.add(new OrderEvent(orderId, customerId, amount, orderTime));
        }
    }

    /**
     * COMPLEX: Both duplicate payments AND multiple shipments.
     * 
     * <p>This is the hardest case. Expanding window to catch a late payment
     * may introduce extra shipments. Shrinking to avoid duplicate may lose valid matches.
     * This is where "fixing one conflict introduces another."</p>
     */
    private void generateComplexCases(int count) {
        for (int i = 0; i < count; i++) {
            String orderId = nextOrderId();
            String customerId = nextCustomerId();
            long orderTime = nextOrderTime();
            double amount = randomAmount();

            // CORRECT payment: within V but close to boundary (stressful)
            long correctPaymentTime = orderTime + randomBetween((int) (V * 0.7), (int) V);
            String correctPaymentId = nextPaymentId();
            payments.add(new PaymentEvent(correctPaymentId, orderId, "PAID", amount, correctPaymentTime));

            // DUPLICATE payment: also PAID, creates ambiguity
            String dupPaymentId = nextPaymentId();
            long dupPaymentTime = orderTime + randomBetween((int) V + 1, (int) VALIDATION_DEADLINE);
            payments.add(new PaymentEvent(dupPaymentId, orderId, "PAID", amount, dupPaymentTime));

            // Multiple shipments (2)
            List<String> shipmentIds = new ArrayList<>();
            for (int s = 0; s < 2; s++) {
                String shipmentId = nextShipmentId();
                long shipmentTime = orderTime + randomBetween(15, (int) U - 5);
                String carrier = randomCarrier();

                shipments.add(new ShipmentEvent(shipmentId, orderId, carrier, shipmentTime));
                shipmentIds.add(shipmentId);

                // Each (correct_payment, shipment) is a valid ground truth match
                groundTruth.add(new GroundTruthMatch(orderId, correctPaymentId, shipmentId,
                        orderTime, correctPaymentTime, shipmentTime, MatchCategory.COMPLEX));
            }

            orders.add(new OrderEvent(orderId, customerId, amount, orderTime));
        }
    }

    /**
     * NO_SHIPMENT: Order has payment but no shipment. Should NOT produce output.
     * Used to measure false positives.
     */
    private void generateNoShipmentCases(int count) {
        for (int i = 0; i < count; i++) {
            String orderId = nextOrderId();
            String customerId = nextCustomerId();
            long orderTime = nextOrderTime();
            double amount = randomAmount();

            // Payment exists and is valid
            long paymentTime = orderTime + randomBetween(5, (int) V);
            String paymentId = nextPaymentId();

            orders.add(new OrderEvent(orderId, customerId, amount, orderTime));
            payments.add(new PaymentEvent(paymentId, orderId, "PAID", amount, paymentTime));
            // NO shipment for this order

            negativeOrders.add(orderId);
            // No ground truth entry — this order should NOT appear in any operator's output
        }
    }

    /**
     * NO_PAYMENT: Order has shipment but no payment. Should NOT produce output
     * (at least for validation-aware operators).
     */
    private void generateNoPaymentCases(int count) {
        for (int i = 0; i < count; i++) {
            String orderId = nextOrderId();
            String customerId = nextCustomerId();
            long orderTime = nextOrderTime();
            double amount = randomAmount();

            // Shipment exists and is within U
            long shipmentTime = orderTime + randomBetween(10, (int) U - 5);
            String shipmentId = nextShipmentId();
            String carrier = randomCarrier();

            orders.add(new OrderEvent(orderId, customerId, amount, orderTime));
            shipments.add(new ShipmentEvent(shipmentId, orderId, carrier, shipmentTime));
            // NO payment for this order

            negativeOrders.add(orderId);
            // No ground truth entry — this order should NOT appear in any operator's output
        }
    }

    // ==================== CSV Export ====================

    /**
     * Write all generated data to CSV files in the specified directory.
     */
    public void writeToCsv(Path outputDir) throws IOException {
        Files.createDirectories(outputDir);

        writeOrdersCsv(outputDir.resolve("orders.csv"));
        writePaymentsCsv(outputDir.resolve("payments.csv"));
        writeShipmentsCsv(outputDir.resolve("shipments.csv"));
        writeGroundTruthCsv(outputDir.resolve("ground_truth.csv"));
        writeNegativeCasesCsv(outputDir.resolve("negative_cases.csv"));
        writeMetadataCsv(outputDir.resolve("dataset_metadata.csv"));

        System.out.println("\n✅ Dataset written to: " + outputDir.toAbsolutePath());
    }

    private void writeOrdersCsv(Path path) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
            pw.println("orderId,customerId,amount,t");
            for (OrderEvent o : orders) {
                pw.printf("%s,%s,%.1f,%d%n", o.orderId, o.customerId, o.amount, o.timestamp);
            }
        }
        System.out.println("  Orders:     " + orders.size() + " events → " + path.getFileName());
    }

    private void writePaymentsCsv(Path path) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
            pw.println("paymentId,orderId,status,amount,t");
            for (PaymentEvent p : payments) {
                pw.printf("%s,%s,%s,%.1f,%d%n", p.paymentId, p.orderId, p.status, p.amount, p.timestamp);
            }
        }
        System.out.println("  Payments:   " + payments.size() + " events → " + path.getFileName());
    }

    private void writeShipmentsCsv(Path path) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
            pw.println("shipmentId,orderId,carrier,t");
            for (ShipmentEvent s : shipments) {
                pw.printf("%s,%s,%s,%d%n", s.shipmentId, s.orderId, s.carrier, s.timestamp);
            }
        }
        System.out.println("  Shipments:  " + shipments.size() + " events → " + path.getFileName());
    }

    private void writeGroundTruthCsv(Path path) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
            pw.println("orderId,paymentId,shipmentId,orderTime,paymentTime,shipmentTime,category");
            for (GroundTruthMatch gt : groundTruth) {
                pw.printf("%s,%s,%s,%d,%d,%d,%s%n",
                        gt.orderId, gt.paymentId, gt.shipmentId,
                        gt.orderTime, gt.paymentTime, gt.shipmentTime, gt.category);
            }
        }
        System.out.println("  Ground Truth: " + groundTruth.size() + " matches → " + path.getFileName());
    }

    private void writeNegativeCasesCsv(Path path) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
            pw.println("orderId,reason");
            for (String orderId : negativeOrders) {
                // Determine reason
                boolean hasPayment = payments.stream().anyMatch(p -> p.orderId.equals(orderId));
                boolean hasShipment = shipments.stream().anyMatch(s -> s.orderId.equals(orderId));
                String reason = !hasPayment ? "NO_PAYMENT" : (!hasShipment ? "NO_SHIPMENT" : "UNKNOWN");
                pw.printf("%s,%s%n", orderId, reason);
            }
        }
        System.out.println("  Negatives:  " + negativeOrders.size() + " cases → " + path.getFileName());
    }

    private void writeMetadataCsv(Path path) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(path))) {
            pw.println("parameter,value");
            pw.println("U," + U);
            pw.println("V," + V);
            pw.println("G," + G);
            pw.println("ALPHA," + ALPHA);
            pw.println("BETA," + BETA);
            pw.println("VALIDATION_DEADLINE," + VALIDATION_DEADLINE);
            pw.println("SEED," + SEED);
            pw.println("total_orders," + orders.size());
            pw.println("total_payments," + payments.size());
            pw.println("total_shipments," + shipments.size());
            pw.println("total_events," + (orders.size() + payments.size() + shipments.size()));
            pw.println("ground_truth_matches," + groundTruth.size());
            pw.println("negative_cases," + negativeOrders.size());

            // Category breakdown
            Map<MatchCategory, Long> categoryCounts = new LinkedHashMap<>();
            for (MatchCategory cat : MatchCategory.values()) {
                categoryCounts.put(cat, groundTruth.stream().filter(gt -> gt.category == cat).count());
            }
            for (var entry : categoryCounts.entrySet()) {
                pw.println("category_" + entry.getKey().name().toLowerCase() + "," + entry.getValue());
            }
        }
        System.out.println("  Metadata:   → " + path.getFileName());
    }

    // ==================== Accessors ====================

    public List<OrderEvent> getOrders() { return Collections.unmodifiableList(orders); }
    public List<PaymentEvent> getPayments() { return Collections.unmodifiableList(payments); }
    public List<ShipmentEvent> getShipments() { return Collections.unmodifiableList(shipments); }
    public List<GroundTruthMatch> getGroundTruth() { return Collections.unmodifiableList(groundTruth); }
    public Set<String> getNegativeOrders() { return Collections.unmodifiableSet(negativeOrders); }

    public int getTotalEventCount() {
        return orders.size() + payments.size() + shipments.size();
    }

    // ==================== Helpers ====================

    private String nextOrderId() { return "O" + (++orderCounter); }
    private String nextPaymentId() { return "P" + (++paymentCounter); }
    private String nextShipmentId() { return "S" + (++shipmentCounter); }
    private String nextCustomerId() { return "C" + (++customerCounter); }
    private long nextOrderTime() { return (long) orderCounter * ORDER_SPACING; }
    private double randomAmount() { return 50.0 + rng.nextInt(500); }
    private String randomCarrier() { return carriers[rng.nextInt(carriers.length)]; }
    private int randomBetween(int min, int max) {
        if (min >= max) return min;
        return min + rng.nextInt(max - min + 1);
    }

    // ==================== Summary ====================

    private void printSummary() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                     DATASET SUMMARY                         ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Orders:          %4d events                               ║%n", orders.size());
        System.out.printf("║  Payments:        %4d events                               ║%n", payments.size());
        System.out.printf("║  Shipments:       %4d events                               ║%n", shipments.size());
        System.out.printf("║  TOTAL:           %4d events                               ║%n", getTotalEventCount());
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Ground Truth:    %4d matches                              ║%n", groundTruth.size());
        System.out.printf("║  Negative Cases:  %4d orders (should NOT match)            ║%n", negativeOrders.size());
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Category Breakdown:                                        ║");

        Map<MatchCategory, Long> counts = new LinkedHashMap<>();
        for (MatchCategory cat : MatchCategory.values()) {
            long c = groundTruth.stream().filter(gt -> gt.category == cat).count();
            if (c > 0) counts.put(cat, c);
        }
        for (var entry : counts.entrySet()) {
            System.out.printf("║    %-25s %4d matches                   ║%n",
                    entry.getKey().name(), entry.getValue());
        }

        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Conflict Characteristics:                                   ║");

        long dupPayments = payments.stream()
                .filter(p -> payments.stream().filter(q -> q.orderId.equals(p.orderId)).count() > 1)
                .map(p -> p.orderId).distinct().count();
        long multiShipments = shipments.stream()
                .filter(s -> shipments.stream().filter(q -> q.orderId.equals(s.orderId)).count() > 1)
                .map(s -> s.orderId).distinct().count();

        System.out.printf("║    Orders with multiple payments: %4d                       ║%n", dupPayments);
        System.out.printf("║    Orders with multiple shipments: %3d                       ║%n", multiShipments);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    // ==================== Main Entry Point ====================

    /**
     * Run standalone to generate dataset files.
     */
    public static void main(String[] args) throws IOException {
        SyntheticDatasetGenerator gen = new SyntheticDatasetGenerator();
        gen.generate();

        Path outputDir = Path.of("benchmark_dataset");
        gen.writeToCsv(outputDir);
    }
}
