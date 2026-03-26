package benchmark.twostreameval;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Generates a two-stream synthetic dataset (Orders × Payments) with ground truth.
 *
 * <h2>Design: Ground-Truth-First</h2>
 * <ol>
 *   <li>Define the correct (orderId, correctPaymentId) pair for each order.</li>
 *   <li>Generate the order and payment events to match.</li>
 *   <li>Inject controlled conflicts: late payments, very late, early, duplicates, missing.</li>
 * </ol>
 *
 * <h2>Case Categories (≥10 orders each, ≥100 orders total, ≥160 payments total)</h2>
 * <table>
 *   <tr><th>Case</th><th>Description</th></tr>
 *   <tr><td>A — NORMAL</td><td>Payment within base window [tO−β0, tO+α0]</td></tr>
 *   <tr><td>B — LATE_PAYMENT</td><td>Payment outside base, inside expanded window</td></tr>
 *   <tr><td>C — VERY_LATE_PAYMENT</td><td>Payment beyond αmax/βmax — both miss</td></tr>
 *   <tr><td>D — EARLY_PAYMENT</td><td>Payment before order (negative offset, needs β)</td></tr>
 *   <tr><td>E — DUPLICATE_PAYMENTS</td><td>Correct + spurious payment in range</td></tr>
 *   <tr><td>F — NO_PAYMENT</td><td>No payment for the order</td></tr>
 * </table>
 *
 * <h2>Window Parameters (must match benchmark test)</h2>
 * <pre>
 *   α0 = 30, β0 = 10, Δ = 20, αmax = 90, βmax = 70
 *   Fixed join:    [tO − β0,  tO + α0]  = [tO−10, tO+30]
 *   Expanding max: [tO − βmax, tO + αmax] = [tO−70, tO+90]
 * </pre>
 */
public class TwoStreamDatasetGenerator {

    // ==================== Window Parameters ====================
    public static final long ALPHA_0  = 30;
    public static final long BETA_0   = 10;
    public static final long DELTA    = 20;
    public static final long ALPHA_MAX = 90;
    public static final long BETA_MAX  = 70;

    /** Fixed join window: [tO − BETA_0, tO + ALPHA_0] */
    public static final long FIXED_LOWER = BETA_0;    // 10
    public static final long FIXED_UPPER = ALPHA_0;   // 30

    // ==================== Generation Parameters ====================
    private static final long ORDER_SPACING = 20;  // time gap between consecutive orders
    private static final long SEED = 42;

    /**
     * Time gap inserted between repeated chunks so their event-time ranges
     * never overlap.  Must be > αmax + βmax to prevent cross-chunk joins.
     */
    private static final long CHUNK_GAP = 500;

    private final Random rng;
    private int orderSeq = 0;
    private int paymentSeq = 0;
    private int customerSeq = 0;

    /** Current base timestamp offset for the chunk being generated. */
    private long timeOffset = 0;

    // ==================== Generated Data ====================
    private final List<OrderEvent> orders = new ArrayList<>();
    private final List<PaymentEvent> payments = new ArrayList<>();
    private final List<GroundTruthRecord> groundTruth = new ArrayList<>();
    private final Set<String> noPaymentOrders = new HashSet<>();

    // ==================== Data Model ====================

    public static class OrderEvent {
        public final String orderId;
        public final long tO;
        public final String customerId;
        public final double amount;

        public OrderEvent(String orderId, long tO, String customerId, double amount) {
            this.orderId = orderId;
            this.tO = tO;
            this.customerId = customerId;
            this.amount = amount;
        }

        @Override public String toString() {
            return "Order{" + orderId + " @" + tO + "}";
        }
    }

    public static class PaymentEvent {
        public final String paymentId;
        public final String orderId;
        public final long tP;
        public final String status;  // VALID, DUPLICATE, FAILED

        public PaymentEvent(String paymentId, String orderId, long tP, String status) {
            this.paymentId = paymentId;
            this.orderId = orderId;
            this.tP = tP;
            this.status = status;
        }

        @Override public String toString() {
            return "Pay{" + paymentId + " ord=" + orderId + " @" + tP + " " + status + "}";
        }
    }

    /** Ground truth: exactly one correct payment per order (or NONE for NO_PAYMENT). */
    public static class GroundTruthRecord {
        public final String orderId;
        public final String correctPaymentId;  // null for NO_PAYMENT
        public final long tO;
        public final long tP;                  // 0 for NO_PAYMENT
        public final String category;

        public GroundTruthRecord(String orderId, String correctPaymentId,
                                  long tO, long tP, String category) {
            this.orderId = orderId;
            this.correctPaymentId = correctPaymentId;
            this.tO = tO;
            this.tP = tP;
            this.category = category;
        }

        /** Match key for evaluation: orderId|paymentId */
        public String matchKey() {
            return orderId + "|" + (correctPaymentId != null ? correctPaymentId : "NONE");
        }

        @Override public String toString() {
            return "GT{" + orderId + ", " + correctPaymentId + " | tO=" + tO
                    + ", tP=" + tP + " | " + category + "}";
        }
    }

    // ==================== Constructor ====================

    public TwoStreamDatasetGenerator() { this(SEED); }
    public TwoStreamDatasetGenerator(long seed) { this.rng = new Random(seed); }

    // ==================== Main Generation ====================

    /** Generate a single chunk (scale factor = 1).  Original behaviour. */
    public void generate() {
        generate(1);
    }

    /**
     * Generate the dataset by repeating the base chunk {@code scaleFactor} times.
     * Each chunk has the same category distribution but unique IDs and offset
     * timestamps so that cross-chunk joins are impossible.
     *
     * <p>Base chunk: 110 orders, ~163 payments.
     * At scaleFactor=10 000 → ~1.1 M orders, ~1.63 M payments.
     *
     * @param scaleFactor number of chunk repetitions (≥ 1)
     */
    public void generate(int scaleFactor) {
        if (scaleFactor < 1) throw new IllegalArgumentException("scaleFactor must be ≥ 1");

        System.out.println("╔═══════════════════════════════════════════════════════════════════╗");
        System.out.println("║  TWO-STREAM DATASET GENERATOR — Orders × Payments               ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
        System.out.printf( "║  α0=%d  β0=%d  Δ=%d  αmax=%d  βmax=%d                          ║%n",
                ALPHA_0, BETA_0, DELTA, ALPHA_MAX, BETA_MAX);
        System.out.printf( "║  Fixed window:    [tO − %d, tO + %d]                             ║%n",
                BETA_0, ALPHA_0);
        System.out.printf( "║  Expanding max:   [tO − %d, tO + %d]                             ║%n",
                BETA_MAX, ALPHA_MAX);
        System.out.printf( "║  Scale factor:    %,d                                            ║%n",
                scaleFactor);
        System.out.println("╚═══════════════════════════════════════════════════════════════════╝");

        long genStart = System.currentTimeMillis();

        for (int chunk = 0; chunk < scaleFactor; chunk++) {
            generateOneChunk();

            if (chunk > 0 && chunk % 1000 == 0) {
                System.out.printf("  ... generated chunk %,d / %,d  (%,d orders, %,d payments)%n",
                        chunk, scaleFactor, orders.size(), payments.size());
            }
        }

        // Sort streams by event-time (ingestion order will add out-of-order later)
        orders.sort(Comparator.comparingLong(o -> o.tO));
        payments.sort(Comparator.comparingLong(p -> p.tP));

        long genMs = System.currentTimeMillis() - genStart;
        System.out.printf("  Dataset generated in %.1f s%n", genMs / 1000.0);

        printSummary();
    }

    /**
     * Generate one chunk of the base dataset (110 orders, ~163 payments).
     * Uses {@code timeOffset} to shift timestamps into a unique range.
     */
    private void generateOneChunk() {
        // Remember the orderSeq at chunk start so nextOrderTime() produces
        // timestamps in the right range via timeOffset.
        int chunkStartSeq = orderSeq;

        // Case A — NORMAL: 30 orders
        generateNormalCases(30);

        // Case B — LATE_PAYMENT: 20 orders
        generateLatePaymentCases(20);

        // Case C — VERY_LATE_PAYMENT: 15 orders
        generateVeryLatePaymentCases(15);

        // Case D — EARLY_PAYMENT: 15 orders
        generateEarlyPaymentCases(15);

        // Case E — DUPLICATE_PAYMENTS: 20 orders
        generateDuplicatePaymentCases(20);

        // Case F — NO_PAYMENT: 10 orders
        generateNoPaymentCases(10);

        // Advance timeOffset so next chunk's timestamps don't overlap.
        // Current chunk used orderSeq [chunkStartSeq+1 .. orderSeq].
        // Max timestamp in this chunk ≈ timeOffset + orderSeq_in_chunk * ORDER_SPACING + ALPHA_MAX + some margin
        int ordersInChunk = orderSeq - chunkStartSeq;
        timeOffset += (ordersInChunk + 1) * ORDER_SPACING + CHUNK_GAP;
    }

    // ==================== Case A — NORMAL ====================
    /**
     * Payment well within fixed window [tO − β0, tO + α0].
     * Both operators should catch this.
     */
    private void generateNormalCases(int count) {
        for (int i = 0; i < count; i++) {
            String oid = nextOrderId();
            long tO = nextOrderTime();
            OrderEvent order = new OrderEvent(oid, tO, nextCustomerId(), randomAmount());

            // Payment within [tO − β0 + 2, tO + α0 − 2] (comfortably inside)
            long tP = tO + randomBetween(-(int) BETA_0 + 2, (int) ALPHA_0 - 2);
            String pid = nextPaymentId();
            PaymentEvent pay = new PaymentEvent(pid, oid, tP, "VALID");

            orders.add(order);
            payments.add(pay);
            groundTruth.add(new GroundTruthRecord(oid, pid, tO, tP, "NORMAL"));

            // 50% of normal orders also get a FAILED retry payment (noise, not GT)
            if (i % 2 == 0) {
                long tPfail = tO + randomBetween(1, (int) ALPHA_0);
                payments.add(new PaymentEvent(nextPaymentId(), oid, tPfail, "FAILED"));
            }
        }
    }

    // ==================== Case B — LATE_PAYMENT ====================
    /**
     * Payment outside fixed window but within expanding max.
     * Fixed join misses → FN.  Expanding join should recover → TP.
     *
     * Payment at tO + (α0+5 .. αmax−2): outside [tO−β0, tO+α0], inside [tO−βmax, tO+αmax].
     */
    private void generateLatePaymentCases(int count) {
        for (int i = 0; i < count; i++) {
            String oid = nextOrderId();
            long tO = nextOrderTime();
            orders.add(new OrderEvent(oid, tO, nextCustomerId(), randomAmount()));

            long tP = tO + randomBetween((int) ALPHA_0 + 5, (int) ALPHA_MAX - 2);
            String pid = nextPaymentId();
            payments.add(new PaymentEvent(pid, oid, tP, "VALID"));
            groundTruth.add(new GroundTruthRecord(oid, pid, tO, tP, "LATE_PAYMENT"));

            // 50% also get a FAILED attempt before the real payment (noise)
            if (i % 2 == 0) {
                long tPfail = tO + randomBetween(1, (int) ALPHA_0);
                payments.add(new PaymentEvent(nextPaymentId(), oid, tPfail, "FAILED"));
            }
        }
    }

    // ==================== Case C — VERY_LATE_PAYMENT ====================
    /**
     * Payment beyond αmax (or before −βmax).  Both operators miss → FN for both.
     */
    private void generateVeryLatePaymentCases(int count) {
        for (int i = 0; i < count; i++) {
            String oid = nextOrderId();
            long tO = nextOrderTime();
            orders.add(new OrderEvent(oid, tO, nextCustomerId(), randomAmount()));

            // Payment way beyond αmax
            long tP = tO + ALPHA_MAX + randomBetween(10, 60);
            String pid = nextPaymentId();
            payments.add(new PaymentEvent(pid, oid, tP, "VALID"));
            groundTruth.add(new GroundTruthRecord(oid, pid, tO, tP, "VERY_LATE_PAYMENT"));

            // 50% also get a FAILED attempt much earlier (noise, not joinable)
            if (i % 2 == 0) {
                long tPfail = tO + randomBetween(1, (int) ALPHA_0);
                payments.add(new PaymentEvent(nextPaymentId(), oid, tPfail, "FAILED"));
            }
        }
    }

    // ==================== Case D — EARLY_PAYMENT ====================
    /**
     * Payment arrives before the order.  Needs β expansion.
     * tP ∈ [tO − βmax + 2, tO − β0 − 1]:  outside fixed, inside expanding.
     *
     * If β0 is too small to reach, the fixed join misses.
     * If expanding β grows enough, expanding join catches.
     */
    private void generateEarlyPaymentCases(int count) {
        for (int i = 0; i < count; i++) {
            String oid = nextOrderId();
            long tO = nextOrderTime();
            orders.add(new OrderEvent(oid, tO, nextCustomerId(), randomAmount()));

            // Payment between [tO − βmax + 2, tO − β0 − 1]
            long tP = tO - randomBetween((int) BETA_0 + 1, (int) BETA_MAX - 2);
            String pid = nextPaymentId();
            payments.add(new PaymentEvent(pid, oid, tP, "VALID"));
            groundTruth.add(new GroundTruthRecord(oid, pid, tO, tP, "EARLY_PAYMENT"));
        }
    }

    // ==================== Case E — DUPLICATE_PAYMENTS ====================
    /**
     * Two payments for the same order within joinable range.
     * P_good is the GT-correct one; P_dup is spurious.
     * Both operators will produce both as matches (2 output rows for orderId).
     * Evaluation will count P_good as TP and P_dup as FP.
     */
    private void generateDuplicatePaymentCases(int count) {
        for (int i = 0; i < count; i++) {
            String oid = nextOrderId();
            long tO = nextOrderTime();
            orders.add(new OrderEvent(oid, tO, nextCustomerId(), randomAmount()));

            // P_good: within base window
            long tPgood = tO + randomBetween(1, (int) ALPHA_0 - 2);
            String pidGood = nextPaymentId();
            payments.add(new PaymentEvent(pidGood, oid, tPgood, "VALID"));

            // P_dup: also within some joinable range (could be base or expanded)
            long tPdup;
            if (rng.nextBoolean()) {
                tPdup = tO + randomBetween(1, (int) ALPHA_0);        // dup inside base
            } else {
                tPdup = tO + randomBetween((int) ALPHA_0 + 1, (int) ALPHA_MAX - 5); // dup in expanded
            }
            String pidDup = nextPaymentId();
            payments.add(new PaymentEvent(pidDup, oid, tPdup, "DUPLICATE"));

            // 50% get an additional FAILED duplicate (triple payment)
            if (i % 2 == 0) {
                long tPfail = tO + randomBetween(-(int) BETA_0, (int) ALPHA_0);
                payments.add(new PaymentEvent(nextPaymentId(), oid, tPfail, "FAILED"));
            }

            // GT says pidGood is correct
            groundTruth.add(new GroundTruthRecord(oid, pidGood, tO, tPgood, "DUPLICATE_PAYMENTS"));
        }
    }

    // ==================== Case F — NO_PAYMENT ====================
    /**
     * No payment for this order.  GT has no match.
     * Any operator output for this orderId is a false positive.
     */
    private void generateNoPaymentCases(int count) {
        for (int i = 0; i < count; i++) {
            String oid = nextOrderId();
            long tO = nextOrderTime();
            orders.add(new OrderEvent(oid, tO, nextCustomerId(), randomAmount()));

            noPaymentOrders.add(oid);
            // GT with null payment
            groundTruth.add(new GroundTruthRecord(oid, null, tO, 0, "NO_PAYMENT"));
        }
    }

    // ==================== Ingestion-Order Shuffle ====================
    /**
     * Returns a merged list of all events in event-time order, but with
     * light out-of-order perturbation to simulate realistic ingestion.
     *
     * Each element is Object[]{side, event, eventTime}:
     *   side = "L" for Order, "R" for Payment
     */
    public List<Object[]> buildIngestionOrder() {
        System.out.printf("  Building ingestion order for %,d events ...%n",
                orders.size() + payments.size());
        long t0 = System.currentTimeMillis();

        List<Object[]> events = new ArrayList<>(orders.size() + payments.size());
        for (OrderEvent o : orders) events.add(new Object[]{"L", o, o.tO});
        for (PaymentEvent p : payments) events.add(new Object[]{"R", p, p.tP});

        // Sort by event-time first
        events.sort(Comparator.comparingLong(e -> (long) e[2]));

        // Introduce light out-of-order: swap adjacent pairs with 20% probability
        for (int i = 0; i < events.size() - 1; i++) {
            if (rng.nextDouble() < 0.20) {
                Object[] tmp = events.get(i);
                events.set(i, events.get(i + 1));
                events.set(i + 1, tmp);
                i++; // skip next to avoid re-swapping
            }
        }

        long ms = System.currentTimeMillis() - t0;
        System.out.printf("  Ingestion order ready in %.1f s%n", ms / 1000.0);
        return events;
    }

    // ==================== CSV Export ====================

    /**
     * Write dataset to CSV.  At large scale (> 50 000 orders) only metadata
     * and a sample of orders/payments/GT are written to avoid multi-GB files.
     */
    public void writeToCsv(Path dir) throws IOException {
        Files.createDirectories(dir);

        boolean large = orders.size() > 50_000;
        int sampleSize = large ? 5_000 : Integer.MAX_VALUE;

        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(dir.resolve("orders.csv")))) {
            pw.println("orderId,tO,customerId,amount");
            int n = 0;
            for (OrderEvent o : orders) {
                if (n++ >= sampleSize) break;
                pw.printf("%s,%d,%s,%.1f%n", o.orderId, o.tO, o.customerId, o.amount);
            }
        }
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(dir.resolve("payments.csv")))) {
            pw.println("paymentId,orderId,tP,status");
            int n = 0;
            for (PaymentEvent p : payments) {
                if (n++ >= sampleSize) break;
                pw.printf("%s,%s,%d,%s%n", p.paymentId, p.orderId, p.tP, p.status);
            }
        }
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(dir.resolve("ground_truth.csv")))) {
            pw.println("orderId,correctPaymentId,tO,tP,category");
            int n = 0;
            for (GroundTruthRecord gt : groundTruth) {
                if (n++ >= sampleSize) break;
                pw.printf("%s,%s,%d,%d,%s%n", gt.orderId,
                        gt.correctPaymentId != null ? gt.correctPaymentId : "",
                        gt.tO, gt.tP, gt.category);
            }
        }
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(dir.resolve("metadata.csv")))) {
            pw.println("param,value");
            pw.println("ALPHA_0," + ALPHA_0);
            pw.println("BETA_0," + BETA_0);
            pw.println("DELTA," + DELTA);
            pw.println("ALPHA_MAX," + ALPHA_MAX);
            pw.println("BETA_MAX," + BETA_MAX);
            pw.println("total_orders," + orders.size());
            pw.println("total_payments," + payments.size());
            pw.println("total_events," + (orders.size() + payments.size()));
            pw.println("ground_truth_entries," + groundTruth.size());
            pw.println("no_payment_orders," + noPaymentOrders.size());
            pw.println("csv_sample_size," + (large ? sampleSize : "ALL"));
        }
        System.out.println("✅ Dataset written to " + dir.toAbsolutePath()
                + (large ? " (sampled " + sampleSize + " rows per file)" : ""));
    }

    // ==================== Accessors ====================
    public List<OrderEvent> getOrders()             { return Collections.unmodifiableList(orders); }
    public List<PaymentEvent> getPayments()         { return Collections.unmodifiableList(payments); }
    public List<GroundTruthRecord> getGroundTruth()  { return Collections.unmodifiableList(groundTruth); }
    public Set<String> getNoPaymentOrders()          { return Collections.unmodifiableSet(noPaymentOrders); }

    // ==================== Helpers ====================
    private String nextOrderId()    { return "O" + (++orderSeq); }
    private String nextPaymentId()  { return "P" + (++paymentSeq); }
    private String nextCustomerId() { return "C" + (++customerSeq % 1_000_000); }
    private long nextOrderTime()    { return timeOffset + (long) orderSeq * ORDER_SPACING; }
    private double randomAmount()   { return 50.0 + rng.nextInt(500); }
    private int randomBetween(int lo, int hi) {
        if (lo >= hi) return lo;
        return lo + rng.nextInt(hi - lo + 1);
    }

    // ==================== Summary ====================
    private void printSummary() {
        System.out.println("\n╔═══════════════════════════════════════════════════════════════════╗");
        System.out.println("║                      DATASET SUMMARY                             ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
        System.out.printf( "║  Orders:          %,12d                                    ║%n", orders.size());
        System.out.printf( "║  Payments:        %,12d                                    ║%n", payments.size());
        System.out.printf( "║  Total events:    %,12d                                    ║%n", orders.size() + payments.size());
        System.out.printf( "║  Ground Truth:    %,12d entries                             ║%n", groundTruth.size());
        System.out.printf( "║  No-payment:      %,12d orders                              ║%n", noPaymentOrders.size());
        System.out.println("╠═══════════════════════════════════════════════════════════════════╣");

        Map<String, Long> cats = new LinkedHashMap<>();
        for (GroundTruthRecord gt : groundTruth)
            cats.merge(gt.category, 1L, Long::sum);
        for (var e : cats.entrySet())
            System.out.printf("║    %-25s %,8d                           ║%n", e.getKey(), e.getValue());
        System.out.println("╚═══════════════════════════════════════════════════════════════════╝");
    }
}
