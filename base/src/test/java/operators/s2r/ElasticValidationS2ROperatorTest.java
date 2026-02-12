package operators.s2r;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnContentChange;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.ElasticValidationS2ROperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.ElasticValidationS2ROperator.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ElasticValidationS2ROperator
 * 
 * <p>This operator implements the "One Elastic S2R" pattern that accumulates
 * Orders (A), Shipments (B), and Payments (C) in a single windowed relation
 * per key. It outputs only finalized results based on elastic validation rules.</p>
 * 
 * <h3>Validation Rules:</h3>
 * <ul>
 *   <li>Payment must arrive within [tO, tO+V+G] to validate order</li>
 *   <li>Shipments must arrive within [tO, tO+U] for content join</li>
 *   <li>Output only when BOTH conditions are met (finalized)</li>
 * </ul>
 */
public class ElasticValidationS2ROperatorTest {

    // ==================== Domain Classes ====================

    /**
     * Order event - Primary anchor for validation
     */
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
            return "Order{" + orderId + ", $" + amount + ", t=" + timestamp + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Order order = (Order) o;
            return Objects.equals(orderId, order.orderId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId);
        }
    }

    /**
     * Shipment event - Content join partner
     */
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
            return "Shipment{" + shipmentId + ", order=" + orderId + ", t=" + timestamp + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Shipment s = (Shipment) o;
            return Objects.equals(shipmentId, s.shipmentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shipmentId);
        }
    }

    /**
     * Payment event - Validation evidence
     */
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

        public boolean isPaid() {
            return "PAID".equals(status);
        }

        @Override
        public String toString() {
            return "Payment{" + paymentId + ", order=" + orderId + ", " + status + ", t=" + timestamp + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Payment p = (Payment) o;
            return Objects.equals(paymentId, p.paymentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(paymentId);
        }
    }

    /**
     * Unified event wrapper for all three types
     */
    public static class UnifiedEvent {
        public enum Type { ORDER, SHIPMENT, PAYMENT }

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

        public static UnifiedEvent fromShipment(Shipment s) {
            return new UnifiedEvent(s.orderId, Type.SHIPMENT, s, s.timestamp);
        }

        public static UnifiedEvent fromPayment(Payment p) {
            return new UnifiedEvent(p.orderId, Type.PAYMENT, p, p.timestamp);
        }

        public Order asOrder() { return type == Type.ORDER ? (Order) payload : null; }
        public Shipment asShipment() { return type == Type.SHIPMENT ? (Shipment) payload : null; }
        public Payment asPayment() { return type == Type.PAYMENT ? (Payment) payload : null; }

        @Override
        public String toString() {
            return type + "{key=" + joinKey + ", t=" + timestamp + ", " + payload + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UnifiedEvent that = (UnifiedEvent) o;
            return Objects.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hash(payload);
        }
    }

    /**
     * Validated result - output type
     */
    public static class ValidatedOrderShipment {
        public final Order order;
        public final Shipment shipment;
        public final Payment payment;
        public final long outputTime;

        public ValidatedOrderShipment(Order order, Shipment shipment, Payment payment, long outputTime) {
            this.order = order;
            this.shipment = shipment;
            this.payment = payment;
            this.outputTime = outputTime;
        }

        @Override
        public String toString() {
            return "Validated{order=" + order.orderId + ", shipment=" + shipment.shipmentId + 
                   ", payment=" + payment.paymentId + ", t=" + outputTime + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ValidatedOrderShipment that = (ValidatedOrderShipment) o;
            return Objects.equals(order, that.order) && Objects.equals(shipment, that.shipment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(order, shipment);
        }
    }

    // ==================== Test Setup ====================

    private Time time;
    private Report report;
    private AccumulatorContentFactory<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>> contentFactory;

    // Window parameters
    private static final long CONTENT_WINDOW = 100;    // U: shipment must arrive within 100ms of order
    private static final long VALIDATION_WINDOW = 50;  // V: payment window
    private static final long GRACE_PERIOD = 30;       // G: additional grace period

    @BeforeEach
    void setUp() {
        time = new TimeImpl(0);
        report = new ReportImpl();
        report.add(new OnContentChange());

        contentFactory = new AccumulatorContentFactory<>(
                event -> event,
                event -> Collections.singletonList(event),
                (list1, list2) -> {
                    List<UnifiedEvent> merged = new ArrayList<>(list1);
                    merged.addAll(list2);
                    return merged;
                },
                new ArrayList<>()
        );
    }

    private ElasticValidationS2ROperator<UnifiedEvent, String, UnifiedEvent, List<UnifiedEvent>> createOperator() {
        return new ElasticValidationS2ROperator<>(
                Tick.TIME_DRIVEN,
                time,
                "elastic-validation",
                contentFactory,
                report,
                CONTENT_WINDOW,       // U
                VALIDATION_WINDOW,    // V
                GRACE_PERIOD,         // G
                event -> event.joinKey,
                event -> event.timestamp,
                event -> {
                    switch (event.type) {
                        case ORDER: return EventCategory.ORDER;
                        case SHIPMENT: return EventCategory.SHIPMENT;
                        case PAYMENT: return EventCategory.PAYMENT;
                        default: throw new IllegalArgumentException("Unknown type: " + event.type);
                    }
                },
                (orderEvent, shipmentEvent, paymentEvent, outputTime) -> {
                    // Create validated result wrapped as UnifiedEvent
                    Order order = orderEvent.asOrder();
                    Shipment shipment = shipmentEvent.asShipment();
                    Payment payment = paymentEvent.asPayment();
                    ValidatedOrderShipment result = new ValidatedOrderShipment(order, shipment, payment, outputTime);
                    return new UnifiedEvent(order.orderId, UnifiedEvent.Type.ORDER, result, outputTime);
                }
        );
    }

    // ==================== Test Cases ====================

    @Test
    @DisplayName("Valid scenario: Payment + Shipment arrive within windows")
    void testValidOrderWithPaymentAndShipment() {
        var operator = createOperator();

        // Order at t=0
        Order order = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order), 0);

        // No finalized results yet
        assertEquals(0, operator.getFinalizedWindows().size());

        // Payment at t=30 (within V+G = 80)
        Payment payment = new Payment("P1", "O1", "PAID", 30);
        operator.compute(UnifiedEvent.fromPayment(payment), 30);

        // Still no results (waiting for shipment)
        assertEquals(0, operator.getFinalizedWindows().size());

        // Shipment at t=50 (within U = 100)
        Shipment shipment = new Shipment("S1", "O1", "FedEx", 50);
        operator.compute(UnifiedEvent.fromShipment(shipment), 50);

        // Now we should have finalized results!
        assertEquals(1, operator.getFinalizedWindows().size());

        // Verify content
        Content<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>> content = 
                operator.getFinalizedWindows().values().iterator().next();
        List<UnifiedEvent> results = content.coalesce();
        assertEquals(1, results.size());
    }

    @Test
    @DisplayName("Payment arrives first, then shipment - triggers finalization")
    void testPaymentThenShipment() {
        var operator = createOperator();

        // Payment arrives first at t=0 (before order!)
        // This simulates payment cache or out-of-order events
        Payment payment = new Payment("P1", "O1", "PAID", 0);
        operator.compute(UnifiedEvent.fromPayment(payment), 0);

        // Order at t=10
        Order order = new Order("O1", "C1", 100.0, 10);
        operator.compute(UnifiedEvent.fromOrder(order), 10);

        // No result yet (payment was at t=0, order at t=10, payment within [10, 90])
        // Actually payment at t=0 is NOT within [10, 90], so this should stay pending
        assertEquals(0, operator.getFinalizedWindows().size());

        // New payment within order's window
        Payment payment2 = new Payment("P2", "O1", "PAID", 20);
        operator.compute(UnifiedEvent.fromPayment(payment2), 20);

        // Still pending (no shipment)
        assertEquals(0, operator.getFinalizedWindows().size());

        // Shipment at t=50
        Shipment shipment = new Shipment("S1", "O1", "UPS", 50);
        operator.compute(UnifiedEvent.fromShipment(shipment), 50);

        // Should finalize now
        assertEquals(1, operator.getFinalizedWindows().size());
    }

    @Test
    @DisplayName("Order expires without payment - finalized as INVALID")
    void testOrderExpiresWithoutPayment() {
        var operator = createOperator();

        // Order at t=0
        Order order = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order), 0);

        // Shipment arrives
        Shipment shipment = new Shipment("S1", "O1", "UPS", 30);
        operator.compute(UnifiedEvent.fromShipment(shipment), 30);

        // Still pending (no payment)
        assertEquals(0, operator.getFinalizedWindows().size());

        // Time advances past validation deadline (V+G = 80)
        // Simulate time advancement with a different event
        Order order2 = new Order("O2", "C2", 200.0, 100);
        operator.compute(UnifiedEvent.fromOrder(order2), 100);

        // O1 should be finalized as INVALID (no results emitted)
        // The finalized windows should still be 0 because INVALID doesn't produce output
        assertTrue(operator.getStateForKey("O1").finalizedOrderTimestamps.contains(0L));
    }

    @Test
    @DisplayName("Multiple shipments for same order")
    void testMultipleShipmentsPerOrder() {
        var operator = createOperator();

        // Order at t=0
        Order order = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order), 0);

        // Payment at t=20
        Payment payment = new Payment("P1", "O1", "PAID", 20);
        operator.compute(UnifiedEvent.fromPayment(payment), 20);

        // First shipment at t=30
        Shipment shipment1 = new Shipment("S1", "O1", "UPS", 30);
        operator.compute(UnifiedEvent.fromShipment(shipment1), 30);

        // Should have 1 result
        assertEquals(1, operator.getFinalizedWindows().size());

        // Second shipment at t=40 - but order is already finalized
        Shipment shipment2 = new Shipment("S2", "O1", "FedEx", 40);
        operator.compute(UnifiedEvent.fromShipment(shipment2), 40);

        // Order was already finalized, so second shipment doesn't create new result
        assertEquals(1, operator.getFinalizedWindows().size());
    }

    @Test
    @DisplayName("Late payment outside validation window - order invalid")
    void testLatePaymentInvalid() {
        var operator = createOperator();

        // Order at t=0
        Order order = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order), 0);

        // Shipment within content window
        Shipment shipment = new Shipment("S1", "O1", "UPS", 30);
        operator.compute(UnifiedEvent.fromShipment(shipment), 30);

        // Time advances past validation deadline (V+G = 80)
        Payment latePayment = new Payment("P1", "O1", "PAID", 90);
        operator.compute(UnifiedEvent.fromPayment(latePayment), 90);

        // Order should be finalized as INVALID (payment too late)
        // No results should be emitted
        assertTrue(operator.getStateForKey("O1").finalizedOrderTimestamps.contains(0L));
        
        // Check that no valid results were produced
        // The finalized windows should be empty because INVALID orders don't produce output
        boolean hasValidResults = operator.getFinalizedWindows().values().stream()
                .anyMatch(c -> c.size() > 0);
        assertFalse(hasValidResults);
    }

    @Test
    @DisplayName("Multiple orders with different outcomes")
    void testMultipleOrdersDifferentOutcomes() {
        var operator = createOperator();

        // O1: will be valid
        Order order1 = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order1), 0);

        // O2: will be invalid (no payment)
        Order order2 = new Order("O2", "C2", 200.0, 10);
        operator.compute(UnifiedEvent.fromOrder(order2), 10);

        // O3: will be valid too
        Order order3 = new Order("O3", "C3", 300.0, 20);
        operator.compute(UnifiedEvent.fromOrder(order3), 20);

        // Payments for O1 and O3
        Payment payment1 = new Payment("P1", "O1", "PAID", 25);
        operator.compute(UnifiedEvent.fromPayment(payment1), 25);

        Payment payment3 = new Payment("P3", "O3", "PAID", 35);
        operator.compute(UnifiedEvent.fromPayment(payment3), 35);

        // Shipments for all three
        Shipment shipment1 = new Shipment("S1", "O1", "UPS", 40);
        operator.compute(UnifiedEvent.fromShipment(shipment1), 40);

        Shipment shipment2 = new Shipment("S2", "O2", "FedEx", 45);
        operator.compute(UnifiedEvent.fromShipment(shipment2), 45);

        Shipment shipment3 = new Shipment("S3", "O3", "DHL", 50);
        operator.compute(UnifiedEvent.fromShipment(shipment3), 50);

        // O1 and O3 should have valid results
        assertEquals(2, operator.getFinalizedWindows().size());

        // O2 should still be pending (no payment yet, within grace period)
        assertFalse(operator.getStateForKey("O2").finalizedOrderTimestamps.contains(10L));

        // Advance time past O2's validation deadline (10 + 50 + 30 = 90)
        Order order4 = new Order("O4", "C4", 400.0, 100);
        operator.compute(UnifiedEvent.fromOrder(order4), 100);

        // O2 should now be finalized as INVALID
        assertTrue(operator.getStateForKey("O2").finalizedOrderTimestamps.contains(10L));
    }

    @Test
    @DisplayName("Payment triggers re-evaluation of pending order")
    void testPaymentTriggersReEvaluation() {
        var operator = createOperator();

        // Order at t=0
        Order order = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order), 0);

        // Shipment at t=20
        Shipment shipment = new Shipment("S1", "O1", "UPS", 20);
        operator.compute(UnifiedEvent.fromShipment(shipment), 20);

        // Still pending (no payment)
        assertEquals(0, operator.getFinalizedWindows().size());
        assertFalse(operator.getStateForKey("O1").finalizedOrderTimestamps.contains(0L));

        // Payment at t=40 - this should trigger re-evaluation and finalization
        Payment payment = new Payment("P1", "O1", "PAID", 40);
        operator.compute(UnifiedEvent.fromPayment(payment), 40);

        // Now should be finalized with valid result
        assertEquals(1, operator.getFinalizedWindows().size());
        assertTrue(operator.getStateForKey("O1").finalizedOrderTimestamps.contains(0L));
    }

    @Test
    @DisplayName("State retention and eviction")
    void testStateRetention() {
        var operator = createOperator();

        // Create an order that will be finalized
        Order order = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order), 0);

        Payment payment = new Payment("P1", "O1", "PAID", 10);
        operator.compute(UnifiedEvent.fromPayment(payment), 10);

        Shipment shipment = new Shipment("S1", "O1", "UPS", 20);
        operator.compute(UnifiedEvent.fromShipment(shipment), 20);

        // Verify state exists
        assertEquals(1, operator.getStateKeyCount());

        // Advance time well past retention (retention = max(100, 80) = 100)
        Order laterOrder = new Order("O2", "C2", 200.0, 200);
        operator.compute(UnifiedEvent.fromOrder(laterOrder), 200);

        // O1's events should be evicted (0, 10, 20 are all < 200 - 100 = 100)
        var state1 = operator.getStateForKey("O1");
        if (state1 != null) {
            assertTrue(state1.orders.isEmpty());
            assertTrue(state1.shipments.isEmpty());
            assertTrue(state1.payments.isEmpty());
        }
    }

    @Test
    @DisplayName("Shipment outside content window - no content")
    void testShipmentOutsideContentWindow() {
        var operator = createOperator();

        // Order at t=0
        Order order = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order), 0);

        // Payment within validation window
        Payment payment = new Payment("P1", "O1", "PAID", 30);
        operator.compute(UnifiedEvent.fromPayment(payment), 30);

        // Shipment outside content window (U = 100)
        Shipment shipment = new Shipment("S1", "O1", "UPS", 150);
        operator.compute(UnifiedEvent.fromShipment(shipment), 150);

        // Order should be finalized as NO_CONTENT (payment valid but shipment outside window)
        assertTrue(operator.getStateForKey("O1").finalizedOrderTimestamps.contains(0L));
        
        // No valid results (content window expired)
        assertEquals(0, operator.getFinalizedWindows().size());
    }

    @Test
    @DisplayName("Concurrent orders for same key with different timestamps")
    void testConcurrentOrdersSameKey() {
        var operator = createOperator();

        // Two orders for same orderId at different times
        // This simulates retries or updates
        Order order1 = new Order("O1", "C1", 100.0, 0);
        operator.compute(UnifiedEvent.fromOrder(order1), 0);

        Order order2 = new Order("O1", "C1", 100.0, 50);
        operator.compute(UnifiedEvent.fromOrder(order2), 50);

        // Payment that validates both
        Payment payment = new Payment("P1", "O1", "PAID", 60);
        operator.compute(UnifiedEvent.fromPayment(payment), 60);

        // Shipment that works for both
        Shipment shipment = new Shipment("S1", "O1", "UPS", 70);
        operator.compute(UnifiedEvent.fromShipment(shipment), 70);

        // Both orders should be finalized
        var state = operator.getStateForKey("O1");
        assertTrue(state.finalizedOrderTimestamps.contains(0L));
        assertTrue(state.finalizedOrderTimestamps.contains(50L));
    }
}
