package operators.s2r;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnContentChange;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.DynamicIntervalJoinS2ROperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.DynamicIntervalJoinS2ROperator.TimestampedElement;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for DynamicIntervalJoinS2ROperator
 * <p>
 * This operator uses a grace period to wait for matching foreign keys,
 * respecting referential integrity. The window closes when:
 * 1. A matching foreign key is found (if closeOnFirstMatch=true), OR
 * 2. The grace period expires
 */
public class DynamicIntervalJoinS2ROperatorTest {

    // ==================== Domain Classes ====================

    /**
     * Order event - Primary Key is orderId
     */
    public static class Order {
        public final String orderId;      // Primary Key
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
            return "Order{id=" + orderId + ", customer=" + customerId +
                    ", amount=$" + amount + ", ts=" + timestamp + "}";
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
     * Shipment event - orderId is the Foreign Key referencing Order.orderId
     */
    public static class Shipment {
        public final String shipmentId;
        public final String orderId;      // Foreign Key -> Order.orderId
        public final String carrier;
        public final String status;
        public final long timestamp;

        public Shipment(String shipmentId, String orderId, String carrier, String status, long timestamp) {
            this.shipmentId = shipmentId;
            this.orderId = orderId;
            this.carrier = carrier;
            this.status = status;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Shipment{id=" + shipmentId + ", orderId=" + orderId +
                    ", carrier=" + carrier + ", status=" + status + ", ts=" + timestamp + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Shipment shipment = (Shipment) o;
            return Objects.equals(shipmentId, shipment.shipmentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shipmentId);
        }
    }

    /**
     * Unified event wrapper for both Orders and Shipments
     */
    public static class OrderShipmentEvent {
        public final String joinKey;      // The key used for joining (orderId)
        public final String eventType;    // "ORDER" or "SHIPMENT"
        public final Object payload;      // The actual Order or Shipment object
        public final long timestamp;

        private OrderShipmentEvent(String joinKey, String eventType, Object payload, long timestamp) {
            this.joinKey = joinKey;
            this.eventType = eventType;
            this.payload = payload;
            this.timestamp = timestamp;
        }

        public static OrderShipmentEvent fromOrder(Order order) {
            return new OrderShipmentEvent(order.orderId, "ORDER", order, order.timestamp);
        }

        public static OrderShipmentEvent fromShipment(Shipment shipment) {
            return new OrderShipmentEvent(shipment.orderId, "SHIPMENT", shipment, shipment.timestamp);
        }

        public boolean isOrder() {
            return "ORDER".equals(eventType);
        }

        public boolean isShipment() {
            return "SHIPMENT".equals(eventType);
        }

        public Order asOrder() {
            return isOrder() ? (Order) payload : null;
        }

        public Shipment asShipment() {
            return isShipment() ? (Shipment) payload : null;
        }

        @Override
        public String toString() {
            return eventType + "{key=" + joinKey + ", ts=" + timestamp + ", data=" + payload + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderShipmentEvent that = (OrderShipmentEvent) o;
            return Objects.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hash(payload);
        }
    }

    // ==================== Test Setup ====================

    private Time time;
    private Report report;
    private AccumulatorContentFactory<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> contentFactory;
    private Map<String, TreeMap<Long, List<TimestampedElement<OrderShipmentEvent>>>> shipmentBufferByKey;

    @BeforeEach
    void setUp() {
        time = new TimeImpl(0);
        report = new ReportImpl();
        report.add(new OnContentChange());

        // Content factory that accumulates events into a list
        contentFactory = new AccumulatorContentFactory<>(
                event -> event,                                    // I -> W (identity)
                event -> Collections.singletonList(event),         // W -> R (single element list)
                (list1, list2) -> {                                // Merge function
                    List<OrderShipmentEvent> merged = new ArrayList<>(list1);
                    merged.addAll(list2);
                    return merged;
                },
                new ArrayList<>()                                  // Empty content
        );

        // Key-indexed buffer for shipments: orderId -> (timestamp -> shipments)
        shipmentBufferByKey = new HashMap<>();
    }

    // ==================== Main Test ====================

    @Test
    void testDynamicIntervalJoinWithGracePeriod() {
        /*
         * Scenario: Dynamic Interval Join respecting referential integrity
         * 
         * This test demonstrates:
         * 1. Orders wait for matching shipments within grace period
         * 2. If shipment arrives within grace period -> immediate match
         * 3. If no shipment within grace period -> window closes with order only
         * 4. Foreign key (orderId) must match for a join
         * 5. Shipments with wrong orderId are NOT matched
         * 
         * Timeline:
         * t=1000: ORD-001 arrives (grace period until t=2000)
         * t=1100: ORD-002 arrives (grace period until t=2100)
         * t=1200: SHP-001 for ORD-001 arrives -> matches ORD-001
         * t=1500: ORD-003 arrives (grace period until t=2500)
         * t=2200: (simulated) ORD-002 grace period expired -> closes with no match
         * t=2300: SHP-003 for ORD-003 arrives -> matches ORD-003
         * t=2400: SHP-004 for ORD-999 arrives -> no matching order (orphan shipment)
         */

        System.out.println("=== Test: Dynamic Interval Join with Grace Period ===\n");

        long gracePeriod = 1000;  // Wait up to 1000ms for matching shipment
        long stateRetention = 10000;
        boolean closeOnFirstMatch = false;  // Collect all matches during grace period

        DynamicIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>>
                orderOperator = new DynamicIntervalJoinS2ROperator<>(
                Tick.TIME_DRIVEN,
                time,
                "dynamicOrderShipmentJoin",
                contentFactory,
                report,
                gracePeriod,
                shipmentBufferByKey,
                event -> event.joinKey,        // Key extractor: orderId
                event -> event.timestamp,      // Timestamp extractor
                stateRetention,
                closeOnFirstMatch
        );

        // ========== Step 1: ORD-001 arrives at t=1000 ==========
        System.out.println("t=1000: ORD-001 arrives (grace period until t=2000)");
        Order order1 = new Order("ORD-001", "CUST-ALICE", 99.99, 1000);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order1), 1000);

        assertEquals(1, orderOperator.getPendingWindowCount(), "Should have 1 pending window");
        assertEquals(0, orderOperator.getCompletedWindowCount(), "No windows completed yet");
        System.out.println("  -> Pending windows: " + orderOperator.getPendingWindowCount());

        // ========== Step 2: ORD-002 arrives at t=1100 ==========
        System.out.println("\nt=1100: ORD-002 arrives (grace period until t=2100)");
        Order order2 = new Order("ORD-002", "CUST-BOB", 149.99, 1100);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order2), 1100);

        assertEquals(2, orderOperator.getPendingWindowCount(), "Should have 2 pending windows");
        System.out.println("  -> Pending windows: " + orderOperator.getPendingWindowCount());

        // ========== Step 3: SHP-001 for ORD-001 arrives at t=1200 ==========
        System.out.println("\nt=1200: SHP-001 for ORD-001 arrives");
        Shipment shipment1 = new Shipment("SHP-001", "ORD-001", "FedEx", "SHIPPED", 1200);
        OrderShipmentEvent shipEvent1 = OrderShipmentEvent.fromShipment(shipment1);

        // Add to build buffer AND trigger matching with pending windows
        orderOperator.addToBuildBufferAndMatch(shipEvent1, shipEvent1.joinKey, shipEvent1.timestamp);

        // Verify the match happened
        assertTrue(orderOperator.hasMatchedWindowForKey("ORD-001"), "ORD-001 should be matched now");
        System.out.println("  -> Shipment matched with pending window for key=ORD-001");

        // ========== Step 4: ORD-003 arrives at t=1500 ==========
        System.out.println("\nt=1500: ORD-003 arrives (grace period until t=2500)");
        Order order3 = new Order("ORD-003", "CUST-CHARLIE", 199.99, 1500);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order3), 1500);

        assertEquals(3, orderOperator.getPendingWindowCount(), "Should have 3 pending windows");
        System.out.println("  -> Pending windows: " + orderOperator.getPendingWindowCount());

        // ========== Step 5: Force time advance to t=2200 - ORD-001 and ORD-002 should expire ==========
        System.out.println("\nt=2200: Advancing time - ORD-001 (t=2000) and ORD-002 (t=2100) grace periods expired");
        orderOperator.forceCheckExpiredWindows(2200);

        // ORD-001 and ORD-002 windows should be completed now
        assertEquals(2, orderOperator.getCompletedWindowCount(), "Should have 2 completed windows");
        assertEquals(1, orderOperator.getPendingWindowCount(), "Should have 1 pending window (ORD-003)");
        System.out.println("  -> Completed windows: " + orderOperator.getCompletedWindowCount());
        System.out.println("  -> Pending windows: " + orderOperator.getPendingWindowCount());

        // ========== Step 6: SHP-003 for ORD-003 arrives at t=2300 ==========
        System.out.println("\nt=2300: SHP-003 for ORD-003 arrives");
        Shipment shipment3 = new Shipment("SHP-003", "ORD-003", "UPS", "SHIPPED", 2300);
        OrderShipmentEvent shipEvent3 = OrderShipmentEvent.fromShipment(shipment3);

        // Add to build buffer AND trigger matching
        orderOperator.addToBuildBufferAndMatch(shipEvent3, shipEvent3.joinKey, shipEvent3.timestamp);

        // Verify ORD-003 is now matched
        assertTrue(orderOperator.hasMatchedWindowForKey("ORD-003"), "ORD-003 should be matched now");
        System.out.println("  -> Shipment matched with pending window for key=ORD-003");

        // Process another order
        Order order4 = new Order("ORD-004", "CUST-DIANA", 50.00, 2300);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order4), 2300);

        System.out.println("  -> Pending windows: " + orderOperator.getPendingWindowCount());

        // ========== Step 7: Add orphan shipment for non-existent order ==========
        System.out.println("\nt=2400: SHP-004 for ORD-999 arrives (orphan - no matching order)");
        Shipment shipment4 = new Shipment("SHP-004", "ORD-999", "DHL", "SHIPPED", 2400);
        OrderShipmentEvent shipEvent4 = OrderShipmentEvent.fromShipment(shipment4);

        // Add to build buffer - no pending window exists for ORD-999
        orderOperator.addToBuildBufferAndMatch(shipEvent4, shipEvent4.joinKey, shipEvent4.timestamp);

        // This shipment should not match any order
        assertFalse(orderOperator.getPendingKeys().contains("ORD-999"),
                "No pending window for ORD-999");
        System.out.println("  -> No matching order for SHP-004 (referential integrity maintained)");

        // ========== Step 8: Force expire all remaining windows at t=5000 ==========
        System.out.println("\nt=5000: Force expire all remaining windows");
        orderOperator.forceCheckExpiredWindows(5000);

        int finalCompleted = orderOperator.getCompletedWindowCount();
        System.out.println("  -> Final completed windows: " + finalCompleted);
        assertTrue(finalCompleted >= 4, "Should have at least 4 completed windows");

        // ========== Step 9: Verify content of completed windows ==========
        System.out.println("\n=== Verifying Join Results ===");

        // Check content at t=1000 (ORD-001 should have matched with SHP-001)
        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content1 =
                orderOperator.content(1000);
        List<OrderShipmentEvent> result1 = content1.coalesce();

        System.out.println("\nORD-001 window content:");
        result1.forEach(e -> System.out.println("  - " + e));

        assertTrue(result1.stream().anyMatch(e -> e.isOrder() && "ORD-001".equals(e.joinKey)),
                "Should contain ORD-001");
        assertTrue(result1.stream().anyMatch(e -> e.isShipment() && "SHP-001".equals(e.asShipment().shipmentId)),
                "Should contain SHP-001 (the matching shipment)");

        // Check content at t=1500 (ORD-003 should have matched with SHP-003)
        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content3 =
                orderOperator.content(1500);
        List<OrderShipmentEvent> result3 = content3.coalesce();

        System.out.println("\nORD-003 window content:");
        result3.forEach(e -> System.out.println("  - " + e));

        assertTrue(result3.stream().anyMatch(e -> e.isOrder() && "ORD-003".equals(e.joinKey)),
                "Should contain ORD-003");
        assertTrue(result3.stream().anyMatch(e -> e.isShipment() && "SHP-003".equals(e.asShipment().shipmentId)),
                "Should contain SHP-003 (the matching shipment)");

        // Check content at t=1100 (ORD-002 had no matching shipment)
        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content2 =
                orderOperator.content(1100);
        List<OrderShipmentEvent> result2 = content2.coalesce();

        System.out.println("\nORD-002 window content (no matching shipment):");
        result2.forEach(e -> System.out.println("  - " + e));

        assertTrue(result2.stream().anyMatch(e -> e.isOrder() && "ORD-002".equals(e.joinKey)),
                "Should contain ORD-002");
        assertEquals(1, result2.size(), "ORD-002 should only have 1 element (no matching shipment)");

        // ========== Summary ==========
        System.out.println("\n=== Test Summary ===");
        System.out.println("✓ ORD-001 matched with SHP-001 (same key, within grace period)");
        System.out.println("✓ ORD-002 had no matching shipment (grace period expired)");
        System.out.println("✓ ORD-003 matched with SHP-003 (same key, within grace period)");
        System.out.println("✓ Orphan shipment SHP-004 (ORD-999) did not match any order");
        System.out.println("✓ Referential integrity maintained through key-based matching");
        System.out.println("\n✓ Dynamic Interval Join test PASSED!\n");
    }

    // ==================== Helper Methods ====================

    private void addShipmentToBuffer(String shipmentId, String orderId, String carrier, long timestamp) {
        Shipment shipment = new Shipment(shipmentId, orderId, carrier, "SHIPPED", timestamp);
        OrderShipmentEvent event = OrderShipmentEvent.fromShipment(shipment);
        DynamicIntervalJoinS2ROperator.addToBuildBuffer(
                shipmentBufferByKey, event, event.joinKey, event.timestamp);
    }

    private int countShipments(List<OrderShipmentEvent> events) {
        return (int) events.stream().filter(OrderShipmentEvent::isShipment).count();
    }

    private boolean hasShipmentId(List<OrderShipmentEvent> events, String shipmentId) {
        return events.stream()
                .filter(OrderShipmentEvent::isShipment)
                .anyMatch(e -> e.asShipment().shipmentId.equals(shipmentId));
    }

    private boolean hasOrderId(List<OrderShipmentEvent> events, String orderId) {
        return events.stream()
                .filter(OrderShipmentEvent::isOrder)
                .anyMatch(e -> e.asOrder().orderId.equals(orderId));
    }
}
