package operators.s2r;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnContentChange;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.ElasticWindowingS2ROperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.ElasticWindowingS2ROperator.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ElasticWindowingS2ROperator using the example dataset.
 * 
 * Demonstrates:
 * 1. With FIXED window you miss matches
 * 2. With ELASTIC window (triangle moving) you recover them
 * 3. Output changes as window mode changes
 * 
 * Window Parameters:
 * - V = 50 (validation window)
 * - G = 30 (grace period) → V+G = 80
 * - U = 100 (content window)
 */
public class ElasticWindowingS2ROperatorTest {

    // ==================== Domain Classes ====================

    public static class Order {
        public final String orderId;
        public final String customerId;
        public final double amount;
        public final long timestamp;

        public Order(String orderId, String customerId, double amount, long timestamp) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Order{%s, t=%d}", orderId, timestamp);
        }
    }

    public static class Payment {
        public final String paymentId;
        public final String orderId;
        public final String status;
        public final long timestamp;

        public Payment(String paymentId, String orderId, String status, long timestamp) {
            this.paymentId = paymentId;
            this.orderId = orderId;
            this.status = status;
            this.timestamp = timestamp;
        }

        public boolean isPaid() { return "PAID".equals(status); }

        @Override
        public String toString() {
            return String.format("Payment{%s→%s, %s, t=%d}", paymentId, orderId, status, timestamp);
        }
    }

    public static class Shipment {
        public final String shipmentId;
        public final String orderId;
        public final String carrier;
        public final long timestamp;

        public Shipment(String shipmentId, String orderId, String carrier, long timestamp) {
            this.shipmentId = shipmentId;
            this.orderId = orderId;
            this.carrier = carrier;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Shipment{%s→%s, t=%d}", shipmentId, orderId, timestamp);
        }
    }

    public static class UnifiedEvent {
        public enum Type { ORDER, PAYMENT, SHIPMENT }
        public final String joinKey;
        public final Type type;
        public final Object payload;
        public final long timestamp;

        private UnifiedEvent(String joinKey, Type type, Object payload, long timestamp) {
            this.joinKey = joinKey;
            this.type = type;
            this.payload = payload;
            this.timestamp = timestamp;
        }

        public static UnifiedEvent fromOrder(Order o) {
            return new UnifiedEvent(o.orderId, Type.ORDER, o, o.timestamp);
        }

        public static UnifiedEvent fromPayment(Payment p) {
            return new UnifiedEvent(p.orderId, Type.PAYMENT, p, p.timestamp);
        }

        public static UnifiedEvent fromShipment(Shipment s) {
            return new UnifiedEvent(s.orderId, Type.SHIPMENT, s, s.timestamp);
        }

        public Order asOrder() { return type == Type.ORDER ? (Order) payload : null; }
        public Payment asPayment() { return type == Type.PAYMENT ? (Payment) payload : null; }
        public Shipment asShipment() { return type == Type.SHIPMENT ? (Shipment) payload : null; }

        @Override
        public String toString() {
            return type + "{key=" + joinKey + ", t=" + timestamp + "}";
        }
    }

    public static class ValidatedResult {
        public final Order order;
        public final Payment payment;
        public final Shipment shipment;
        public final long outputTime;

        public ValidatedResult(Order order, Payment payment, Shipment shipment, long outputTime) {
            this.order = order;
            this.payment = payment;
            this.shipment = shipment;
            this.outputTime = outputTime;
        }

        @Override
        public String toString() {
            return String.format("Result{%s + %s + %s}", 
                    order.orderId, payment.paymentId, shipment.shipmentId);
        }
    }

    // ==================== Test Data from Example Tables ====================

    // Window Parameters
    private static final long V = 50;   // Validation window
    private static final long G = 30;   // Grace period
    private static final long U = 100;  // Content window

    // Table A: Orders
    private List<Order> orders;

    // Table C: Payments
    private List<Payment> payments;

    // Table B: Shipments
    private List<Shipment> shipments;

    @BeforeEach
    void setupTestData() {
        // From the example dataset
        orders = Arrays.asList(
            new Order("O1", "C1", 100.00, 0),    // Happy path
            new Order("O2", "C2", 200.00, 10),   // Payment in grace period
            new Order("O3", "C3", 150.00, 20),   // Payment too late
            new Order("O4", "C4", 300.00, 30),   // Shipment outside window
            new Order("O5", "C5", 250.00, 40),   // Missing shipment
            new Order("O6", "C6", 175.00, 50),   // Missing payment
            new Order("O7", "C7", 125.00, 60),   // Multiple shipments
            new Order("O8", "C8", 400.00, 70),   // Duplicate payments
            new Order("O9", "C9", 350.00, 80),   // FAILED payment
            new Order("O10", "C10", 500.00, 90)  // Late payment that works
        );

        payments = Arrays.asList(
            new Payment("P1", "O1", "PAID", 30),    // Within V+G
            new Payment("P2", "O2", "PAID", 75),    // In grace period (65 > V but < V+G)
            new Payment("P3", "O3", "PAID", 120),   // Too late (100 > V+G=80)
            new Payment("P4", "O4", "PAID", 50),    // Within V+G
            new Payment("P5", "O5", "PAID", 60),    // Within V+G (but no shipment)
            // O6: no payment
            new Payment("P7", "O7", "PAID", 90),    // Within V+G
            new Payment("P8a", "O8", "PAID", 100),  // Within V+G
            new Payment("P8b", "O8", "PAID", 120),  // Duplicate (ignored)
            new Payment("P9", "O9", "FAILED", 110), // FAILED status
            new Payment("P10", "O10", "PAID", 140)  // Within V+G (50 ≤ 80)
        );

        shipments = Arrays.asList(
            new Shipment("S1", "O1", "FedEx", 50),   // tO=0, Δ=50 ≤ U
            new Shipment("S2", "O2", "UPS", 80),     // tO=10, Δ=70 ≤ U
            new Shipment("S3", "O3", "DHL", 90),     // tO=20, Δ=70 ≤ U (but payment late)
            new Shipment("S4", "O4", "FedEx", 180),  // tO=30, Δ=150 > U (LATE!)
            // O5: no shipment
            new Shipment("S6", "O6", "UPS", 100),    // Within U (but no payment)
            new Shipment("S7a", "O7", "FedEx", 100), // tO=60, Δ=40 ≤ U
            new Shipment("S7b", "O7", "DHL", 130),   // tO=60, Δ=70 ≤ U
            new Shipment("S8", "O8", "UPS", 120),    // tO=70, Δ=50 ≤ U
            new Shipment("S9", "O9", "FedEx", 130),  // Within U (but FAILED payment)
            new Shipment("S10", "O10", "DHL", 150)   // tO=90, Δ=60 ≤ U
        );
    }

    // ==================== Helper Methods ====================

    private Time createTime() {
        return new TimeImpl(0);
    }

    private Report createReport() {
        Report report = new ReportImpl();
        report.add(new OnContentChange());
        return report;
    }

    private AccumulatorContentFactory<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>> createContentFactory() {
        return new AccumulatorContentFactory<>(
                e -> e,
                e -> Collections.singletonList(e),
                (l1, l2) -> { List<UnifiedEvent> m = new ArrayList<>(l1); m.addAll(l2); return m; },
                new ArrayList<>()
        );
    }

    private ElasticWindowingS2ROperator<UnifiedEvent, String, UnifiedEvent, List<UnifiedEvent>> 
            createOperator(WindowMode mode, long extension) {
        return new ElasticWindowingS2ROperator<>(
                Tick.TIME_DRIVEN,
                createTime(),
                "elastic-" + mode,
                createContentFactory(),
                createReport(),
                V, G, U, extension,
                mode,
                e -> e.joinKey,
                e -> e.timestamp,
                e -> {
                    switch (e.type) {
                        case ORDER: return EventCategory.ORDER;
                        case PAYMENT: return EventCategory.PAYMENT;
                        case SHIPMENT: return EventCategory.SHIPMENT;
                        default: throw new IllegalArgumentException();
                    }
                },
                (order, oT, payment, pT, shipment, sT, outT) -> {
                    // Wrap result in UnifiedEvent
                    ValidatedResult result = new ValidatedResult(
                            ((UnifiedEvent) order).asOrder(),
                            ((UnifiedEvent) payment).asPayment(),
                            ((UnifiedEvent) shipment).asShipment(),
                            outT);
                    return new UnifiedEvent(((UnifiedEvent) order).joinKey, 
                            UnifiedEvent.Type.ORDER, result, outT);
                }
        );
    }

    /**
     * Process all events in timestamp order
     */
    private void processAllEvents(ElasticWindowingS2ROperator<UnifiedEvent, String, UnifiedEvent, List<UnifiedEvent>> operator) {
        // Collect all events with timestamps
        List<UnifiedEvent> allEvents = new ArrayList<>();
        for (Order o : orders) allEvents.add(UnifiedEvent.fromOrder(o));
        for (Payment p : payments) {
            if (p.isPaid()) { // Only process PAID payments
                allEvents.add(UnifiedEvent.fromPayment(p));
            }
        }
        for (Shipment s : shipments) allEvents.add(UnifiedEvent.fromShipment(s));

        // Sort by timestamp
        allEvents.sort(Comparator.comparingLong(e -> e.timestamp));

        // Process in order
        for (UnifiedEvent event : allEvents) {
            operator.compute(event, event.timestamp);
        }

        // Advance time to trigger finalizations
        operator.compute(UnifiedEvent.fromOrder(new Order("DUMMY", "X", 0, 300)), 300);
    }

    /**
     * Extract results and print as table
     */
    private List<ValidatedResult> extractResults(
            ElasticWindowingS2ROperator<UnifiedEvent, String, UnifiedEvent, List<UnifiedEvent>> operator) {
        List<ValidatedResult> results = new ArrayList<>();
        for (var content : operator.getFinalizedWindows().values()) {
            for (UnifiedEvent e : content.coalesce()) {
                if (e.payload instanceof ValidatedResult) {
                    results.add((ValidatedResult) e.payload);
                }
            }
        }
        return results;
    }

    private void printResultTable(String title, List<ValidatedResult> results) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println(title);
        System.out.println("=".repeat(80));
        System.out.printf("%-8s | %-10s | %-10s | %-8s | %-8s | %-8s%n",
                "Order", "Payment", "Shipment", "t_Order", "t_Pay", "t_Ship");
        System.out.println("-".repeat(80));
        
        if (results.isEmpty()) {
            System.out.println("(no results)");
        } else {
            for (ValidatedResult r : results) {
                System.out.printf("%-8s | %-10s | %-10s | %-8d | %-8d | %-8d%n",
                        r.order.orderId, r.payment.paymentId, r.shipment.shipmentId,
                        r.order.timestamp, r.payment.timestamp, r.shipment.timestamp);
            }
        }
        System.out.println("-".repeat(80));
        System.out.println("Total: " + results.size() + " results");
    }

    // ==================== Tests ====================

    @Test
    @DisplayName("Compare FIXED vs ELASTIC vs EXTENDED modes - see output differences")
    void testCompareAllModes() {
        System.out.println("\n" + "#".repeat(80));
        System.out.println("# ELASTIC WINDOWING COMPARISON TEST");
        System.out.println("# V=" + V + " (validation), G=" + G + " (grace), U=" + U + " (content)");
        System.out.println("#".repeat(80));

        // Mode 1: FIXED
        var fixedOp = createOperator(WindowMode.FIXED, 0);
        processAllEvents(fixedOp);
        List<ValidatedResult> fixedResults = extractResults(fixedOp);
        printResultTable("MODE 1: FIXED (shipment window [tO, tO+U])", fixedResults);

        // Mode 2: ELASTIC (triangle moving)
        var elasticOp = createOperator(WindowMode.ELASTIC, 0);
        processAllEvents(elasticOp);
        List<ValidatedResult> elasticResults = extractResults(elasticOp);
        printResultTable("MODE 2: ELASTIC (shipment window [tP, tP+U] - triangle moving)", elasticResults);

        // Mode 3: EXTENDED_FIXED (with 50 extension)
        var extendedOp = createOperator(WindowMode.EXTENDED_FIXED, 50);
        processAllEvents(extendedOp);
        List<ValidatedResult> extendedResults = extractResults(extendedOp);
        printResultTable("MODE 3: EXTENDED_FIXED (shipment window [tO, tO+U+50])", extendedResults);

        // Print comparison summary
        printComparisonSummary(fixedResults, elasticResults, extendedResults);

        // Assertions
        assertTrue(fixedResults.size() >= 4, "FIXED should have at least 4 results");
        assertTrue(elasticResults.size() >= fixedResults.size(), 
                "ELASTIC should have >= FIXED results (triangle moving helps)");
    }

    private void printComparisonSummary(List<ValidatedResult> fixed, 
                                         List<ValidatedResult> elastic,
                                         List<ValidatedResult> extended) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("COMPARISON SUMMARY");
        System.out.println("=".repeat(80));
        
        Set<String> fixedOrders = new HashSet<>();
        Set<String> elasticOrders = new HashSet<>();
        Set<String> extendedOrders = new HashSet<>();
        
        for (ValidatedResult r : fixed) fixedOrders.add(r.order.orderId);
        for (ValidatedResult r : elastic) elasticOrders.add(r.order.orderId);
        for (ValidatedResult r : extended) extendedOrders.add(r.order.orderId);

        System.out.printf("%-20s | %-10s | %-15s%n", "Mode", "Results", "Orders");
        System.out.println("-".repeat(50));
        System.out.printf("%-20s | %-10d | %-15s%n", "FIXED", fixed.size(), fixedOrders);
        System.out.printf("%-20s | %-10d | %-15s%n", "ELASTIC", elastic.size(), elasticOrders);
        System.out.printf("%-20s | %-10d | %-15s%n", "EXTENDED_FIXED", extended.size(), extendedOrders);
        
        // Find orders recovered by elastic but not in fixed
        Set<String> recoveredByElastic = new HashSet<>(elasticOrders);
        recoveredByElastic.removeAll(fixedOrders);
        
        Set<String> recoveredByExtended = new HashSet<>(extendedOrders);
        recoveredByExtended.removeAll(fixedOrders);

        System.out.println("\n" + "-".repeat(50));
        System.out.println("Orders RECOVERED by ELASTIC (not in FIXED): " + recoveredByElastic);
        System.out.println("Orders RECOVERED by EXTENDED (not in FIXED): " + recoveredByExtended);
    }

    @Test
    @DisplayName("O4: Shipment outside FIXED window but inside ELASTIC window")
    void testO4ShipmentRecoveredByElastic() {
        System.out.println("\n--- Test O4: Late Shipment Recovery ---");
        System.out.println("Order O4 at t=30, Payment P4 at t=50, Shipment S4 at t=180");
        System.out.println("FIXED window [30, 130]: 180 > 130 → MISS");
        System.out.println("ELASTIC window [50, 150]: 180 > 150 → STILL MISS (but closer!)");
        System.out.println("EXTENDED window [30, 180]: 180 ≤ 180 → HIT!");

        // Create O4-specific events
        Order o4 = new Order("O4", "C4", 300.00, 30);
        Payment p4 = new Payment("P4", "O4", "PAID", 50);
        Shipment s4 = new Shipment("S4", "O4", "FedEx", 180);

        // FIXED mode
        var fixedOp = createOperator(WindowMode.FIXED, 0);
        fixedOp.compute(UnifiedEvent.fromOrder(o4), o4.timestamp);
        fixedOp.compute(UnifiedEvent.fromPayment(p4), p4.timestamp);
        fixedOp.compute(UnifiedEvent.fromShipment(s4), s4.timestamp);
        fixedOp.compute(UnifiedEvent.fromOrder(new Order("X", "X", 0, 300)), 300);
        
        List<ValidatedResult> fixedResults = extractResults(fixedOp);
        System.out.println("FIXED results: " + fixedResults.size());
        assertEquals(0, fixedResults.size(), "O4 should NOT match in FIXED mode");

        // EXTENDED mode with extension=50 (window becomes [30, 180])
        var extendedOp = createOperator(WindowMode.EXTENDED_FIXED, 50);
        extendedOp.compute(UnifiedEvent.fromOrder(o4), o4.timestamp);
        extendedOp.compute(UnifiedEvent.fromPayment(p4), p4.timestamp);
        extendedOp.compute(UnifiedEvent.fromShipment(s4), s4.timestamp);
        extendedOp.compute(UnifiedEvent.fromOrder(new Order("X", "X", 0, 300)), 300);
        
        List<ValidatedResult> extendedResults = extractResults(extendedOp);
        System.out.println("EXTENDED (ext=50) results: " + extendedResults.size());
        assertEquals(1, extendedResults.size(), "O4 SHOULD match in EXTENDED mode");
    }

    @Test
    @DisplayName("Triangle Moving: Payment time affects shipment window")
    void testTriangleMovingEffect() {
        System.out.println("\n--- Test Triangle Moving Effect ---");
        System.out.println("Same order, different payment times → different shipment windows");

        // Scenario: Order at t=0, Shipment at t=120
        // If payment at t=10: ELASTIC window [10, 110] → 120 > 110 → MISS
        // If payment at t=50: ELASTIC window [50, 150] → 120 ≤ 150 → HIT!

        Order order = new Order("T1", "C1", 100.0, 0);
        Shipment shipment = new Shipment("ST1", "T1", "UPS", 120);

        // Early payment (t=10)
        Payment earlyPayment = new Payment("PE", "T1", "PAID", 10);
        var earlyOp = createOperator(WindowMode.ELASTIC, 0);
        earlyOp.compute(UnifiedEvent.fromOrder(order), order.timestamp);
        earlyOp.compute(UnifiedEvent.fromPayment(earlyPayment), earlyPayment.timestamp);
        earlyOp.compute(UnifiedEvent.fromShipment(shipment), shipment.timestamp);
        earlyOp.compute(UnifiedEvent.fromOrder(new Order("X", "X", 0, 200)), 200);

        List<ValidatedResult> earlyResults = extractResults(earlyOp);
        System.out.println("Early payment (t=10), shipment window [10, 110]: " + earlyResults.size() + " results");
        assertEquals(0, earlyResults.size(), "Shipment at 120 should MISS window [10, 110]");

        // Late payment (t=50)
        Payment latePayment = new Payment("PL", "T1", "PAID", 50);
        var lateOp = createOperator(WindowMode.ELASTIC, 0);
        lateOp.compute(UnifiedEvent.fromOrder(order), order.timestamp);
        lateOp.compute(UnifiedEvent.fromPayment(latePayment), latePayment.timestamp);
        lateOp.compute(UnifiedEvent.fromShipment(shipment), shipment.timestamp);
        lateOp.compute(UnifiedEvent.fromOrder(new Order("X", "X", 0, 200)), 200);

        List<ValidatedResult> lateResults = extractResults(lateOp);
        System.out.println("Late payment (t=50), shipment window [50, 150]: " + lateResults.size() + " results");
        assertEquals(1, lateResults.size(), "Shipment at 120 should HIT window [50, 150]");

        System.out.println("\n→ Triangle moving: later payment = wider effective window for shipment!");
    }

    @Test
    @DisplayName("Generate full output table for all orders")
    void testGenerateFullOutputTable() {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("FULL OUTPUT TABLE FOR ALL ORDERS (V=" + V + ", G=" + G + ", U=" + U + ")");
        System.out.println("=".repeat(100));

        // Process with all three modes
        var fixedOp = createOperator(WindowMode.FIXED, 0);
        var elasticOp = createOperator(WindowMode.ELASTIC, 0);
        var extendedOp = createOperator(WindowMode.EXTENDED_FIXED, 50);

        processAllEvents(fixedOp);
        processAllEvents(elasticOp);
        processAllEvents(extendedOp);

        Set<String> fixedOrders = new HashSet<>();
        Set<String> elasticOrders = new HashSet<>();
        Set<String> extendedOrders = new HashSet<>();

        for (ValidatedResult r : extractResults(fixedOp)) fixedOrders.add(r.order.orderId);
        for (ValidatedResult r : extractResults(elasticOp)) elasticOrders.add(r.order.orderId);
        for (ValidatedResult r : extractResults(extendedOp)) extendedOrders.add(r.order.orderId);

        System.out.printf("%-8s | %-8s | %-8s | %-12s | %-12s | %-10s | %-10s | %-12s%n",
                "Order", "t_Order", "t_Pay", "Pay Status", "t_Ship", "FIXED", "ELASTIC", "EXTENDED");
        System.out.println("-".repeat(100));

        for (Order order : orders) {
            String orderId = order.orderId;
            long tOrder = order.timestamp;
            
            // Find payment for this order
            String payStatus = "MISSING";
            long tPay = -1;
            for (Payment p : payments) {
                if (p.orderId.equals(orderId)) {
                    payStatus = p.status;
                    tPay = p.timestamp;
                    break;
                }
            }
            
            // Find shipment for this order
            long tShip = -1;
            for (Shipment s : shipments) {
                if (s.orderId.equals(orderId)) {
                    tShip = s.timestamp;
                    break;
                }
            }

            String fixedStatus = fixedOrders.contains(orderId) ? "✅ VALID" : "❌ MISS";
            String elasticStatus = elasticOrders.contains(orderId) ? "✅ VALID" : "❌ MISS";
            String extendedStatus = extendedOrders.contains(orderId) ? "✅ VALID" : "❌ MISS";

            System.out.printf("%-8s | %-8d | %-8s | %-12s | %-12s | %-10s | %-10s | %-12s%n",
                    orderId, tOrder, 
                    tPay >= 0 ? String.valueOf(tPay) : "-",
                    payStatus,
                    tShip >= 0 ? String.valueOf(tShip) : "-",
                    fixedStatus, elasticStatus, extendedStatus);
        }

        System.out.println("-".repeat(100));
        System.out.printf("%-8s | %-8s | %-8s | %-12s | %-12s | %-10d | %-10d | %-12d%n",
                "TOTAL", "", "", "", "",
                fixedOrders.size(), elasticOrders.size(), extendedOrders.size());
    }

    @Test
    @DisplayName("Show what changes when window extends")
    void testWindowExtensionImpact() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("IMPACT OF WINDOW EXTENSION");
        System.out.println("=".repeat(80));

        int[] extensions = {0, 25, 50, 75, 100};
        
        System.out.printf("%-15s | %-10s | %-30s%n", "Extension", "Results", "Orders Matched");
        System.out.println("-".repeat(60));

        for (int ext : extensions) {
            var op = createOperator(WindowMode.EXTENDED_FIXED, ext);
            processAllEvents(op);
            List<ValidatedResult> results = extractResults(op);
            
            Set<String> matchedOrders = new HashSet<>();
            for (ValidatedResult r : results) matchedOrders.add(r.order.orderId);

            System.out.printf("%-15s | %-10d | %-30s%n",
                    "U+" + ext + " = " + (U + ext), results.size(), matchedOrders);
        }

        System.out.println("\n→ As extension increases, more late shipments are captured!");
    }
}
