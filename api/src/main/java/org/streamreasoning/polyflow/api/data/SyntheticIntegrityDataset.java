import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Step 1: Generate synthetic streams (Orders, Payments, Shipments) with anomalies.
 * Step 2: Build offline "ground truth" outputs using full knowledge of the streams.
 *
 * Semantics used for ground truth (you can change easily):
 * - valid(order) iff exists PAID payment p with: tO <= tP <= tO + (V+G)
 * - content(order) iff exists shipment s with: tO <= tS <= tO + U
 * - output: (order, chosenPayment, eachShipmentInWindow)   [or choose one shipment if you want]
 */
public class SyntheticIntegrityDataset {

    // -------------------- Domain events --------------------

    public static final class Order {
        public final String orderId;
        public final String customerId;
        public final double amount;
        public final long t;

        public Order(String orderId, String customerId, double amount, long t) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.amount = amount;
            this.t = t;
        }
    }

    public static final class Payment {
        public final String paymentId;
        public final String orderId;
        public final String status; // "PAID", "FAILED"
        public final long t;

        public Payment(String paymentId, String orderId, String status, long t) {
            this.paymentId = paymentId;
            this.orderId = orderId;
            this.status = status;
            this.t = t;
        }

        public boolean isPaid() {
            return "PAID".equals(status);
        }
    }

    public static final class Shipment {
        public final String shipmentId;
        public final String orderId;
        public final String carrier;
        public final long t;

        public Shipment(String shipmentId, String orderId, String carrier, long t) {
            this.shipmentId = shipmentId;
            this.orderId = orderId;
            this.carrier = carrier;
            this.t = t;
        }
    }

    // Ground truth output record: one per (Order, Shipment) with a chosen valid payment
    public static final class TruthTriple {
        public final String orderId;
        public final String paymentId;
        public final String shipmentId;
        public final long orderTime;
        public final long paymentTime;
        public final long shipmentTime;

        public TruthTriple(String orderId, String paymentId, String shipmentId,
                           long orderTime, long paymentTime, long shipmentTime) {
            this.orderId = orderId;
            this.paymentId = paymentId;
            this.shipmentId = shipmentId;
            this.orderTime = orderTime;
            this.paymentTime = paymentTime;
            this.shipmentTime = shipmentTime;
        }
    }

    // -------------------- Config --------------------

    public static final class Config {
        public final long U;      // content window upper bound: shipment in [tO, tO+U]
        public final long V;      // validation window base
        public final long G;      // grace
        public final long baseGapBetweenOrders; // spacing of order timestamps

        // anomaly rates (roughly)
        public final double pMissingPayment;
        public final double pMissingShipment;
        public final double pLatePaymentBeyondDeadline;
        public final double pFailedPayment;
        public final double pDuplicatePayments;
        public final double pLateShipmentBeyondU;
        public final double pMultipleShipments;

        // ground truth choice rules
        public final boolean truthKeepAllShipmentsInWindow; // if false: choose earliest shipment only

        public Config(long U, long V, long G,
                      long baseGapBetweenOrders,
                      double pMissingPayment,
                      double pMissingShipment,
                      double pLatePaymentBeyondDeadline,
                      double pFailedPayment,
                      double pDuplicatePayments,
                      double pLateShipmentBeyondU,
                      double pMultipleShipments,
                      boolean truthKeepAllShipmentsInWindow) {

            this.U = U;
            this.V = V;
            this.G = G;
            this.baseGapBetweenOrders = baseGapBetweenOrders;
            this.pMissingPayment = pMissingPayment;
            this.pMissingShipment = pMissingShipment;
            this.pLatePaymentBeyondDeadline = pLatePaymentBeyondDeadline;
            this.pFailedPayment = pFailedPayment;
            this.pDuplicatePayments = pDuplicatePayments;
            this.pLateShipmentBeyondU = pLateShipmentBeyondU;
            this.pMultipleShipments = pMultipleShipments;
            this.truthKeepAllShipmentsInWindow = truthKeepAllShipmentsInWindow;
        }

        public long deadlineVG() {
            return V + G;
        }
    }

    // -------------------- Dataset container --------------------

    public static final class Dataset {
        public final List<Order> orders;
        public final List<Payment> payments;
        public final List<Shipment> shipments;
        public final List<TruthTriple> groundTruth;

        public Dataset(List<Order> orders, List<Payment> payments, List<Shipment> shipments, List<TruthTriple> groundTruth) {
            this.orders = orders;
            this.payments = payments;
            this.shipments = shipments;
            this.groundTruth = groundTruth;
        }
    }

    // -------------------- Step 1: Generate synthetic streams --------------------

    public static Dataset generate(long seed, int nOrders, Config cfg) {
        Random rnd = new Random(seed);

        List<Order> orders = new ArrayList<>();
        List<Payment> payments = new ArrayList<>();
        List<Shipment> shipments = new ArrayList<>();

        String[] carriers = {"DHL", "UPS", "FedEx"};
        long t = 0;

        int payCounter = 1;
        int shipCounter = 1;

        for (int i = 1; i <= nOrders; i++) {
            String oid = "O" + i;
            String cid = "C" + i;
            double amount = 50.0 + rnd.nextInt(500);

            // create order time
            long orderTime = t;
            orders.add(new Order(oid, cid, amount, orderTime));

            // decide payment anomaly
            boolean missingPayment = rnd.nextDouble() < cfg.pMissingPayment;
            boolean failedPayment = (!missingPayment) && (rnd.nextDouble() < cfg.pFailedPayment);
            boolean lateBeyondDeadline = (!missingPayment) && (rnd.nextDouble() < cfg.pLatePaymentBeyondDeadline);
            boolean duplicatePayments = (!missingPayment) && (rnd.nextDouble() < cfg.pDuplicatePayments);

            if (!missingPayment) {
                String status = failedPayment ? "FAILED" : "PAID";

                // payment time:
                // - normally within [tO, tO+V]
                // - or in grace within [tO+V, tO+V+G]
                // - or late beyond deadline > tO+V+G
                long deadline = orderTime + cfg.deadlineVG();
                long paymentTime;
                if (lateBeyondDeadline) {
                    paymentTime = deadline + 1 + rnd.nextInt((int) Math.max(2, cfg.deadlineVG()));
                } else {
                    paymentTime = orderTime + rnd.nextInt((int) Math.max(1, cfg.deadlineVG() + 1));
                }

                payments.add(new Payment("P" + (payCounter++), oid, status, paymentTime));

                // optional duplicate: second PAID payment (or same status)
                if (duplicatePayments) {
                    long paymentTime2 = paymentTime + 1 + rnd.nextInt((int) Math.max(1, cfg.deadlineVG()));
                    payments.add(new Payment("P" + (payCounter++), oid, status, paymentTime2));
                }

                // optional: a payment *before* order (out-of-order / weird)
                // (handy to test your “ignore if before tO” rule)
                if (rnd.nextDouble() < 0.10) {
                    long early = Math.max(0, orderTime - (1 + rnd.nextInt(20)));
                    payments.add(new Payment("P" + (payCounter++), oid, "PAID", early));
                }
            }

            // decide shipment anomaly
            boolean missingShipment = rnd.nextDouble() < cfg.pMissingShipment;
            boolean lateShipmentBeyondU = (!missingShipment) && (rnd.nextDouble() < cfg.pLateShipmentBeyondU);
            boolean multipleShipments = (!missingShipment) && (rnd.nextDouble() < cfg.pMultipleShipments);

            if (!missingShipment) {
                long shipmentTime;
                if (lateShipmentBeyondU) {
                    shipmentTime = orderTime + cfg.U + 1 + rnd.nextInt((int) Math.max(2, cfg.U));
                } else {
                    shipmentTime = orderTime + rnd.nextInt((int) Math.max(1, cfg.U + 1));
                }
                shipments.add(new Shipment("S" + (shipCounter++), oid, carriers[rnd.nextInt(carriers.length)], shipmentTime));

                if (multipleShipments) {
                    long shipmentTime2 = shipmentTime + 1 + rnd.nextInt((int) Math.max(1, cfg.U));
                    shipments.add(new Shipment("S" + (shipCounter++), oid, carriers[rnd.nextInt(carriers.length)], shipmentTime2));
                }
            }

            // advance time for next order
            t += cfg.baseGapBetweenOrders;
        }

        // Step 2: compute ground truth offline
        List<TruthTriple> truth = buildGroundTruth(orders, payments, shipments, cfg);

        return new Dataset(orders, payments, shipments, truth);
    }

    // -------------------- Step 2: Ground truth builder (offline, full knowledge) --------------------

    public static List<TruthTriple> buildGroundTruth(List<Order> orders,
                                                     List<Payment> payments,
                                                     List<Shipment> shipments,
                                                     Config cfg) {

        Map<String, List<Payment>> payByOrder = payments.stream()
                .collect(Collectors.groupingBy(p -> p.orderId));

        Map<String, List<Shipment>> shipByOrder = shipments.stream()
                .collect(Collectors.groupingBy(s -> s.orderId));

        List<TruthTriple> out = new ArrayList<>();

        for (Order o : orders) {
            long tO = o.t;

            // valid payment candidates: PAID and within [tO, tO+V+G]
            List<Payment> candidates = payByOrder.getOrDefault(o.orderId, List.of()).stream()
                    .filter(Payment::isPaid)
                    .filter(p -> p.t >= tO && p.t <= tO + cfg.deadlineVG())
                    .sorted(Comparator.comparingLong(p -> p.t))
                    .collect(Collectors.toList());

            if (candidates.isEmpty()) continue; // invalid (no valid payment)

            // choose rule for payment (typical: earliest valid payment)
            Payment chosen = candidates.get(0);

            // shipment candidates: within [tO, tO+U]
            List<Shipment> shipCands = shipByOrder.getOrDefault(o.orderId, List.of()).stream()
                    .filter(s -> s.t >= tO && s.t <= tO + cfg.U)
                    .sorted(Comparator.comparingLong(s -> s.t))
                    .collect(Collectors.toList());

            if (shipCands.isEmpty()) continue; // no content

            if (!cfg.truthKeepAllShipmentsInWindow) {
                Shipment earliest = shipCands.get(0);
                out.add(new TruthTriple(o.orderId, chosen.paymentId, earliest.shipmentId,
                        tO, chosen.t, earliest.t));
            } else {
                for (Shipment s : shipCands) {
                    out.add(new TruthTriple(o.orderId, chosen.paymentId, s.shipmentId,
                            tO, chosen.t, s.t));
                }
            }
        }

        // Sort truth outputs for readability
        out.sort(Comparator.comparingLong((TruthTriple tt) -> tt.orderTime)
                .thenComparing(tt -> tt.orderId)
                .thenComparing(tt -> tt.shipmentId));
        return out;
    }

    // -------------------- CSV export helpers --------------------

    public static void writeOrdersCsv(List<Order> orders, Path file) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            w.write("orderId,customerId,amount,t\n");
            for (Order o : orders) {
                w.write(o.orderId + "," + o.customerId + "," + o.amount + "," + o.t + "\n");
            }
        }
    }

    public static void writePaymentsCsv(List<Payment> payments, Path file) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            w.write("paymentId,orderId,status,t\n");
            for (Payment p : payments) {
                w.write(p.paymentId + "," + p.orderId + "," + p.status + "," + p.t + "\n");
            }
        }
    }

    public static void writeShipmentsCsv(List<Shipment> shipments, Path file) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            w.write("shipmentId,orderId,carrier,t\n");
            for (Shipment s : shipments) {
                w.write(s.shipmentId + "," + s.orderId + "," + s.carrier + "," + s.t + "\n");
            }
        }
    }

    public static void writeTruthCsv(List<TruthTriple> truth, Path file) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            w.write("orderId,paymentId,shipmentId,orderTime,paymentTime,shipmentTime\n");
            for (TruthTriple tt : truth) {
                w.write(tt.orderId + "," + tt.paymentId + "," + tt.shipmentId + ","
                        + tt.orderTime + "," + tt.paymentTime + "," + tt.shipmentTime + "\n");
            }
        }
    }

    // -------------------- Demo main --------------------

    public static void main(String[] args) throws Exception {
        Config cfg = new Config(
                100,  // U
                50,   // V
                30,   // G
                10,   // baseGapBetweenOrders
                0.10, // pMissingPayment
                0.10, // pMissingShipment
                0.15, // pLatePaymentBeyondDeadline
                0.10, // pFailedPayment
                0.20, // pDuplicatePayments
                0.15, // pLateShipmentBeyondU
                0.20, // pMultipleShipments
                true  // truthKeepAllShipmentsInWindow
        );

        Dataset ds = generate(42L, 50, cfg);

        Path outDir = Paths.get("out_dataset");
        Files.createDirectories(outDir);

        writeOrdersCsv(ds.orders, outDir.resolve("orders.csv"));
        writePaymentsCsv(ds.payments, outDir.resolve("payments.csv"));
        writeShipmentsCsv(ds.shipments, outDir.resolve("shipments.csv"));
        writeTruthCsv(ds.groundTruth, outDir.resolve("ground_truth.csv"));

        System.out.println("Generated:");
        System.out.println("  Orders:   " + ds.orders.size());
        System.out.println("  Payments: " + ds.payments.size());
        System.out.println("  Shipments:" + ds.shipments.size());
        System.out.println("  Truth:    " + ds.groundTruth.size());
        System.out.println("CSV written to: " + outDir.toAbsolutePath());
    }
}