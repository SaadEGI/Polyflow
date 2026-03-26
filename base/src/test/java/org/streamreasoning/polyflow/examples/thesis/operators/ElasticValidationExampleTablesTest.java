package org.streamreasoning.polyflow.examples.thesis.operators;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.*;
import java.util.stream.Collectors;


public class ElasticValidationExampleTablesTest {

    // ==================== Domain Classes ====================

    static class Order {
        final String orderId;
        final long t;
        final String description;

        Order(String orderId, long t, String description) {
            this.orderId = orderId;
            this.t = t;
            this.description = description;
        }

        @Override
        public String toString() {
            return String.format("Order{%s, t=%d}", orderId, t);
        }
    }

    static class Shipment {
        final String shipmentId;
        final String orderId;
        final long t;
        final String description;

        Shipment(String shipmentId, String orderId, long t, String description) {
            this.shipmentId = shipmentId;
            this.orderId = orderId;
            this.t = t;
            this.description = description;
        }

        @Override
        public String toString() {
            return String.format("Shipment{%s->%s, t=%d}", shipmentId, orderId, t);
        }
    }

    static class Payment {
        final String paymentId;
        final String orderId;
        final long t;
        final String status;
        final String description;

        Payment(String paymentId, String orderId, long t, String status, String description) {
            this.paymentId = paymentId;
            this.orderId = orderId;
            this.t = t;
            this.status = status;
            this.description = description;
        }

        boolean isSuccess() {
            return "SUCCESS".equals(status);
        }

        @Override
        public String toString() {
            return String.format("Payment{%s->%s, t=%d, %s}", paymentId, orderId, t, status);
        }
    }

    static class ValidatedResult {
        final Order order;
        final Shipment shipment;
        final Payment payment;
        final long outputTime;

        ValidatedResult(Order order, Shipment shipment, Payment payment, long outputTime) {
            this.order = order;
            this.shipment = shipment;
            this.payment = payment;
            this.outputTime = outputTime;
        }
    }

    enum FinalizationStatus {
        VALID,      // Payment + Shipment within windows
        INVALID,    // No valid payment within deadline
        NO_CONTENT, // Payment valid but no shipment
        PENDING     // Still waiting
    }

    static class OrderEvaluation {
        final Order order;
        final FinalizationStatus status;
        final List<ValidatedResult> results;
        final String reason;
        final Payment validPayment;
        final List<Shipment> validShipments;

        OrderEvaluation(Order order, FinalizationStatus status, List<ValidatedResult> results,
                        String reason, Payment validPayment, List<Shipment> validShipments) {
            this.order = order;
            this.status = status;
            this.results = results;
            this.reason = reason;
            this.validPayment = validPayment;
            this.validShipments = validShipments;
        }
    }

    // ==================== Example Dataset ====================

    private List<Order> createOrders() {
        return Arrays.asList(
            new Order("O1", 10, "Normal case - paid and shipped on time"),
            new Order("O2", 15, "Payment inside validation window (no grace needed)"),
            new Order("O3", 20, "Payment in grace period (V < t_payment ≤ V+G)"),
            new Order("O4", 25, "Payment too late (past V+G)"),
            new Order("O5", 30, "Payment on time, but NO shipment"),
            new Order("O6", 35, "Payment on time, shipment LATE (outside U)"),
            new Order("O7", 40, "Multiple shipments within window"),
            new Order("O8", 45, "Duplicate payments (same order)"),
            new Order("O9", 50, "Missing payment entirely"),
            new Order("O10", 55, "Shipment arrives BEFORE order (edge case)")
        );
    }

    private List<Shipment> createShipments() {
        return Arrays.asList(
            new Shipment("S1", "O1", 12, "Within U=5 of O1 (10+5=15) ✓"),
            new Shipment("S2", "O2", 18, "Within U=5 of O2 (15+5=20) ✓"),
            new Shipment("S3", "O3", 24, "Within U=5 of O3 (20+5=25) ✓"),
            new Shipment("S4", "O4", 28, "Within U=5 of O4 (25+5=30) ✓"),
            new Shipment("S5", "O6", 42, "LATE: Outside U=5 of O6 (35+5=40) ✗"),
            new Shipment("S6", "O7", 42, "Within U=5 of O7 (40+5=45) ✓"),
            new Shipment("S7", "O7", 44, "Within U=5 of O7 (40+5=45) ✓"),
            new Shipment("S8", "O8", 48, "Within U=5 of O8 (45+5=50) ✓"),
            new Shipment("S9", "O10", 54, "BEFORE order O10 at t=55 ✗"),
            new Shipment("S10", "O10", 58, "Within U=5 of O10 (55+5=60) ✓")
        );
    }

    private List<Payment> createPayments() {
        return Arrays.asList(
            new Payment("P1", "O1", 11, "SUCCESS", "Within V=3 of O1 (10+3=13) ✓"),
            new Payment("P2", "O2", 17, "SUCCESS", "Within V=3 of O2 (15+3=18) ✓"),
            new Payment("P3", "O3", 24, "SUCCESS", "In GRACE: V < t ≤ V+G (20+3=23 < 24 ≤ 20+5=25) ✓"),
            new Payment("P4", "O4", 32, "SUCCESS", "TOO LATE: t > V+G (25+5=30 < 32) ✗"),
            new Payment("P5", "O5", 33, "SUCCESS", "Within V+G of O5 (30+5=35) ✓"),
            new Payment("P6", "O6", 37, "SUCCESS", "Within V+G of O6 (35+5=40) ✓"),
            new Payment("P7", "O7", 43, "SUCCESS", "Within V+G of O7 (40+5=45) ✓"),
            new Payment("P8a", "O8", 47, "SUCCESS", "First payment for O8 ✓"),
            new Payment("P8b", "O8", 49, "SUCCESS", "Duplicate payment for O8 (ignored)"),
            new Payment("P9", "O10", 57, "SUCCESS", "Within V+G of O10 (55+5=60) ✓")
        );
    }

    // ==================== Evaluation Logic ====================

    private List<OrderEvaluation> evaluateAllOrders(
            List<Order> orders,
            List<Shipment> shipments,
            List<Payment> payments,
            long U, long V, long G,
            long currentTime) {

        // Group shipments and payments by orderId
        Map<String, List<Shipment>> shipmentsByOrder = shipments.stream()
                .collect(Collectors.groupingBy(s -> s.orderId));
        Map<String, List<Payment>> paymentsByOrder = payments.stream()
                .collect(Collectors.groupingBy(p -> p.orderId));

        List<OrderEvaluation> evaluations = new ArrayList<>();

        for (Order order : orders) {
            long validationDeadline = order.t + V + G;
            long contentDeadline = order.t + U;

            // Find valid payment (first SUCCESS payment within [t_order, t_order + V + G])
            List<Payment> orderPayments = paymentsByOrder.getOrDefault(order.orderId, Collections.emptyList());
            Payment validPayment = orderPayments.stream()
                    .filter(p -> p.isSuccess())
                    .filter(p -> p.t >= order.t && p.t <= validationDeadline)
                    .findFirst()
                    .orElse(null);

            // Find valid shipments within [t_order, t_order + U]
            List<Shipment> orderShipments = shipmentsByOrder.getOrDefault(order.orderId, Collections.emptyList());
            List<Shipment> validShipments = orderShipments.stream()
                    .filter(s -> s.t >= order.t && s.t <= contentDeadline)
                    .collect(Collectors.toList());

            // Determine status and create results
            FinalizationStatus status;
            List<ValidatedResult> results = new ArrayList<>();
            String reason;

            if (validPayment != null) {
                if (!validShipments.isEmpty()) {
                    status = FinalizationStatus.VALID;
                    reason = String.format("Payment P@%d ✓, Shipment(s) within [%d, %d] ✓",
                            validPayment.t, order.t, contentDeadline);
                    for (Shipment s : validShipments) {
                        results.add(new ValidatedResult(order, s, validPayment,
                                Math.max(validPayment.t, s.t)));
                    }
                } else if (currentTime > contentDeadline) {
                    status = FinalizationStatus.NO_CONTENT;
                    reason = String.format("Payment P@%d ✓, but no shipment within [%d, %d]",
                            validPayment.t, order.t, contentDeadline);
                } else {
                    status = FinalizationStatus.PENDING;
                    reason = "Waiting for shipment";
                }
            } else {
                if (currentTime > validationDeadline) {
                    status = FinalizationStatus.INVALID;
                    // Check if there was a late payment
                    Payment latePayment = orderPayments.stream()
                            .filter(Payment::isSuccess)
                            .findFirst()
                            .orElse(null);
                    if (latePayment != null) {
                        reason = String.format("Payment P@%d too late (deadline was %d)",
                                latePayment.t, validationDeadline);
                    } else {
                        reason = "No payment received";
                    }
                } else {
                    status = FinalizationStatus.PENDING;
                    reason = "Waiting for payment";
                }
            }

            evaluations.add(new OrderEvaluation(order, status, results, reason, validPayment, validShipments));
        }

        return evaluations;
    }

    // ==================== Table Generation ====================

    private void printInputTables(List<Order> orders, List<Shipment> shipments, List<Payment> payments) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("                           INPUT TABLES");
        System.out.println("=".repeat(80));

        // Orders table
        System.out.println("\n### Table A: Orders");
        System.out.println("+---------+---------+------------------------------------------------+");
        System.out.println("| orderId | t_order | description                                    |");
        System.out.println("+---------+---------+------------------------------------------------+");
        for (Order o : orders) {
            System.out.printf("| %-7s | %7d | %-46s |\n", o.orderId, o.t, truncate(o.description, 46));
        }
        System.out.println("+---------+---------+------------------------------------------------+");

        // Shipments table
        System.out.println("\n### Table B: Shipments");
        System.out.println("+------------+---------+------------+----------------------------------------+");
        System.out.println("| shipmentId | orderId | t_shipment | description                            |");
        System.out.println("+------------+---------+------------+----------------------------------------+");
        for (Shipment s : shipments) {
            System.out.printf("| %-10s | %-7s | %10d | %-38s |\n", s.shipmentId, s.orderId, s.t, truncate(s.description, 38));
        }
        System.out.println("+------------+---------+------------+----------------------------------------+");

        // Payments table
        System.out.println("\n### Table C: Payments");
        System.out.println("+-----------+---------+-----------+---------+----------------------------------+");
        System.out.println("| paymentId | orderId | t_payment | status  | description                      |");
        System.out.println("+-----------+---------+-----------+---------+----------------------------------+");
        for (Payment p : payments) {
            System.out.printf("| %-9s | %-7s | %9d | %-7s | %-32s |\n",
                    p.paymentId, p.orderId, p.t, p.status, truncate(p.description, 32));
        }
        System.out.println("+-----------+---------+-----------+---------+----------------------------------+");
    }

    private void printEvaluationTable(List<OrderEvaluation> evaluations, long U, long V, long G) {
        System.out.println("\n" + "-".repeat(80));
        System.out.printf("OUTPUT TABLE: U=%d, V=%d, G=%d (V+G=%d)\n", U, V, G, V + G);
        System.out.println("-".repeat(80));

        System.out.println("\n### Validation Analysis Per Order");
        System.out.println("+---------+---------+------------------+------------------+------------+-------------+");
        System.out.println("| orderId | t_order | Payment Window   | Shipment Window  | Status     | # Results   |");
        System.out.println("+---------+---------+------------------+------------------+------------+-------------+");

        int totalValid = 0;
        int totalResults = 0;

        for (OrderEvaluation eval : evaluations) {
            long paymentEnd = eval.order.t + V + G;
            long shipmentEnd = eval.order.t + U;
            String paymentWindow = String.format("[%d, %d]", eval.order.t, paymentEnd);
            String shipmentWindow = String.format("[%d, %d]", eval.order.t, shipmentEnd);

            String statusStr = eval.status.toString();
            int resultCount = eval.results.size();

            System.out.printf("| %-7s | %7d | %-16s | %-16s | %-10s | %11d |\n",
                    eval.order.orderId, eval.order.t, paymentWindow, shipmentWindow, statusStr, resultCount);

            if (eval.status == FinalizationStatus.VALID) {
                totalValid++;
                totalResults += resultCount;
            }
        }
        System.out.println("+---------+---------+------------------+------------------+------------+-------------+");
        System.out.printf("| TOTALS: %d VALID orders, %d output results                                        |\n",
                totalValid, totalResults);
        System.out.println("+---------+---------+------------------+------------------+------------+-------------+");

        // Print detailed results
        System.out.println("\n### Valid Output Results");
        System.out.println("+----------+---------+------------+-----------+---------+------------+----------+");
        System.out.println("| outputId | orderId | shipmentId | paymentId | t_order | t_shipment | t_output |");
        System.out.println("+----------+---------+------------+-----------+---------+------------+----------+");

        int outputId = 1;
        for (OrderEvaluation eval : evaluations) {
            for (ValidatedResult r : eval.results) {
                System.out.printf("| OUT%-5d | %-7s | %-10s | %-9s | %7d | %10d | %8d |\n",
                        outputId++, r.order.orderId, r.shipment.shipmentId, r.payment.paymentId,
                        r.order.t, r.shipment.t, r.outputTime);
            }
        }
        System.out.println("+----------+---------+------------+-----------+---------+------------+----------+");

        // Print rejected orders with reasons
        System.out.println("\n### Rejected Orders");
        System.out.println("+---------+------------+----------------------------------------------------------+");
        System.out.println("| orderId | Status     | Reason                                                   |");
        System.out.println("+---------+------------+----------------------------------------------------------+");

        for (OrderEvaluation eval : evaluations) {
            if (eval.status != FinalizationStatus.VALID) {
                System.out.printf("| %-7s | %-10s | %-56s |\n",
                        eval.order.orderId, eval.status, truncate(eval.reason, 56));
            }
        }
        System.out.println("+---------+------------+----------------------------------------------------------+");
    }

    private void printComparisonTable(
            List<OrderEvaluation> baseline,
            List<OrderEvaluation> extended,
            String changeDescription) {

        System.out.println("\n### Comparison: " + changeDescription);
        System.out.println("+---------+------------+------------+-----------+");
        System.out.println("| orderId | Baseline   | Extended   | Changed?  |");
        System.out.println("+---------+------------+------------+-----------+");

        for (int i = 0; i < baseline.size(); i++) {
            OrderEvaluation base = baseline.get(i);
            OrderEvaluation ext = extended.get(i);
            boolean changed = base.status != ext.status || base.results.size() != ext.results.size();
            String changedStr = changed ? "**YES**" : "no";

            System.out.printf("| %-7s | %-10s | %-10s | %-9s |\n",
                    base.order.orderId, base.status, ext.status, changedStr);
        }
        System.out.println("+---------+------------+------------+-----------+");
    }

    private String truncate(String s, int maxLen) {
        if (s.length() <= maxLen) return s;
        return s.substring(0, maxLen - 3) + "...";
    }

    // ==================== Tests ====================

    @Test
    @DisplayName("Run complete example dataset and generate output tables")
    void runCompleteExampleDataset() {
        List<Order> orders = createOrders();
        List<Shipment> shipments = createShipments();
        List<Payment> payments = createPayments();

        // Print input tables
        printInputTables(orders, shipments, payments);

        // Determine current time (max event time + buffer)
        long currentTime = 100; // Well past all events

        System.out.println("\n" + "=".repeat(80));
        System.out.println("                          OUTPUT TABLES");
        System.out.println("=".repeat(80));

        // Policy 1: Baseline (U=5, V=3, G=2)
        System.out.println("\n" + "#".repeat(80));
        System.out.println("# POLICY 1: BASELINE (U=5, V=3, G=2)");
        System.out.println("#".repeat(80));
        List<OrderEvaluation> baseline = evaluateAllOrders(orders, shipments, payments, 5, 3, 2, currentTime);
        printEvaluationTable(baseline, 5, 3, 2);

        // Policy 2: Extended Validation Window (U=5, V=5, G=2)
        System.out.println("\n" + "#".repeat(80));
        System.out.println("# POLICY 2: EXTENDED VALIDATION WINDOW (U=5, V=5, G=2)");
        System.out.println("#".repeat(80));
        List<OrderEvaluation> extendedV = evaluateAllOrders(orders, shipments, payments, 5, 5, 2, currentTime);
        printEvaluationTable(extendedV, 5, 5, 2);

        // Policy 3: Extended Content Window (U=7, V=3, G=2)
        System.out.println("\n" + "#".repeat(80));
        System.out.println("# POLICY 3: EXTENDED CONTENT WINDOW (U=7, V=3, G=2)");
        System.out.println("#".repeat(80));
        List<OrderEvaluation> extendedU = evaluateAllOrders(orders, shipments, payments, 7, 3, 2, currentTime);
        printEvaluationTable(extendedU, 7, 3, 2);

        // Policy 4: Both Extended (U=7, V=5, G=2)
        System.out.println("\n" + "#".repeat(80));
        System.out.println("# POLICY 4: BOTH EXTENDED (U=7, V=5, G=2)");
        System.out.println("#".repeat(80));
        List<OrderEvaluation> bothExtended = evaluateAllOrders(orders, shipments, payments, 7, 5, 2, currentTime);
        printEvaluationTable(bothExtended, 7, 5, 2);

        // Comparison tables
        System.out.println("\n" + "=".repeat(80));
        System.out.println("                       COMPARISON TABLES");
        System.out.println("=".repeat(80));

        printComparisonTable(baseline, extendedV, "Baseline vs Extended Validation (V: 3→5)");
        printComparisonTable(baseline, extendedU, "Baseline vs Extended Content (U: 5→7)");
        printComparisonTable(baseline, bothExtended, "Baseline vs Both Extended");

        // Summary
        System.out.println("\n" + "=".repeat(80));
        System.out.println("                           SUMMARY");
        System.out.println("=".repeat(80));

        long baselineValid = baseline.stream().filter(e -> e.status == FinalizationStatus.VALID).count();
        long baselineResults = baseline.stream().mapToInt(e -> e.results.size()).sum();

        long extVValid = extendedV.stream().filter(e -> e.status == FinalizationStatus.VALID).count();
        long extVResults = extendedV.stream().mapToInt(e -> e.results.size()).sum();

        long extUValid = extendedU.stream().filter(e -> e.status == FinalizationStatus.VALID).count();
        long extUResults = extendedU.stream().mapToInt(e -> e.results.size()).sum();

        long bothValid = bothExtended.stream().filter(e -> e.status == FinalizationStatus.VALID).count();
        long bothResults = bothExtended.stream().mapToInt(e -> e.results.size()).sum();

        System.out.println("\n+----------------------------------+-------------+--------------+");
        System.out.println("| Policy                           | Valid Orders| Total Results|");
        System.out.println("+----------------------------------+-------------+--------------+");
        System.out.printf("| Baseline (U=5, V=3, G=2)         | %11d | %12d |\n", baselineValid, baselineResults);
        System.out.printf("| Extended V (U=5, V=5, G=2)       | %11d | %12d |\n", extVValid, extVResults);
        System.out.printf("| Extended U (U=7, V=3, G=2)       | %11d | %12d |\n", extUValid, extUResults);
        System.out.printf("| Both Extended (U=7, V=5, G=2)    | %11d | %12d |\n", bothValid, bothResults);
        System.out.println("+----------------------------------+-------------+--------------+");

        System.out.println("\n### Key Observations:");
        System.out.println("• Extending V from 3 to 5: O4 changes from INVALID to VALID (payment at 32 now within [25,32])");
        System.out.println("• Extending U from 5 to 7: O6 changes from NO_CONTENT to VALID (shipment at 42 now within [35,42])");
        System.out.println("• Both extended: O4 AND O6 both become VALID");
        System.out.println("• Orders O5, O9 remain unchanged (O5: no shipment, O9: no payment)");
    }

    @Test
    @DisplayName("Show timeline visualization")
    void showTimelineVisualization() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("                      TIMELINE VISUALIZATION");
        System.out.println("=".repeat(80));
        System.out.println("\nParameters: U=5, V=3, G=2 (V+G=5)");
        System.out.println("\nLegend: O=Order, P=Payment, S=Shipment, |=Window boundary");
        System.out.println("        [...]=Window, ✓=Valid, ✗=Invalid/Late\n");

        System.out.println("Time:  10   11   12   13   14   15   16   17   18   19   20   21   22   23   24   25");
        System.out.println("       │    │    │    │    │    │    │    │    │    │    │    │    │    │    │    │");
        System.out.println("O1:    O────P────S────────────]                                                    → VALID");
        System.out.println("       └─────────────────────┘ Window [10,15]");
        System.out.println();
        System.out.println("O2:                        O─────────P────S────]                                   → VALID");
        System.out.println("                           └─────────────────────┘ Window [15,20]");
        System.out.println();
        System.out.println("O3:                                            O────────────────P/S──]             → VALID (grace)");
        System.out.println("                                               └─────────────────────┘ Window [20,25]");
        System.out.println("                                                      V ends at 23, P@24 in grace period");
        System.out.println();

        System.out.println("\nTime:  25   26   27   28   29   30   31   32   33   34   35   36   37   38   39   40");
        System.out.println("       │    │    │    │    │    │    │    │    │    │    │    │    │    │    │    │");
        System.out.println("O4:    O─────────────S──────────]    P                                             → INVALID");
        System.out.println("       └─────────────────────────┘ Window [25,30], P@32 is OUTSIDE!");
        System.out.println();
        System.out.println("O5:                        O─────────P──────────]    (no shipment)                 → NO_CONTENT");
        System.out.println("                           └─────────────────────┘ Window [30,35]");
        System.out.println();
        System.out.println("O6:                                            O─────────P──────────]    S         → NO_CONTENT");
        System.out.println("                                               └─────────────────────┘ Window [35,40], S@42 OUTSIDE");
        System.out.println();

        System.out.println("\nTime:  40   41   42   43   44   45   46   47   48   49   50   51   52   53   54   55");
        System.out.println("       │    │    │    │    │    │    │    │    │    │    │    │    │    │    │    │");
        System.out.println("O7:    O─────────S────P────S────]                                                  → VALID (2 results)");
        System.out.println("       └─────────────────────────┘ Window [40,45], 2 shipments!");
        System.out.println();
        System.out.println("O8:                            O─────────P────S────P']                             → VALID");
        System.out.println("                               └─────────────────────┘ Window [45,50], P8b ignored");
        System.out.println();
        System.out.println("O9:                                                    O─────────────────]         → INVALID");
        System.out.println("                                                       └─────────────────┘ No payment, no shipment");
        System.out.println();

        System.out.println("\nTime:  54   55   56   57   58   59   60");
        System.out.println("       │    │    │    │    │    │    │");
        System.out.println("O10:   S'   O─────────P────S────────]                                              → VALID");
        System.out.println("            └─────────────────────────┘ Window [55,60], S9@54 before order (ignored)");
    }
}
