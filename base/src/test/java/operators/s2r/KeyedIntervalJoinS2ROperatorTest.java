package operators.s2r;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnContentChange;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.sds.timevarying.TimeVarying;
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.KeyedIntervalJoinS2ROperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.KeyedIntervalJoinS2ROperator.TimestampedElement;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;


public class KeyedIntervalJoinS2ROperatorTest {


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
     * Shipment event
     * orderId is the FOREIGN KEY referencing Order.orderId
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

        // Suppress verbose logging during tests
        java.util.logging.Logger.getLogger("org.streamreasoning.polyflow").setLevel(java.util.logging.Level.SEVERE);
    }

    // ==================== Test Cases ====================

/*    @Test
    void testBasicKeyedIntervalJoin() {
        *//*
         * Scenario:
         * - Order ORD-001 placed at t=1000
         * - Shipment for ORD-001 at t=1500 (within interval)
         * - Shipment for ORD-002 at t=1200 (WRONG KEY - should NOT match)
         * 
         * Interval: [0, +1000] (shipment within 1 second after order)
         *//*
        // System.out.println("=== Test: Basic Keyed Interval Join ===");



        long lowerBound = 0;
        long upperBound = 1000;
        long stateRetention = 10000;

        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            orderOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    time,
                    "orderShipmentJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    shipmentBufferByKey,
                    event -> event.joinKey,        // Key extractor: orderId
                    event -> event.timestamp,      // Timestamp extractor
                    stateRetention
            );

        // Add shipments to the build buffer FIRST
        Shipment shipment1 = new Shipment("SHP-001", "ORD-001", "FedEx", "SHIPPED", 1500);
        Shipment shipment2 = new Shipment("SHP-002", "ORD-002", "UPS", "SHIPPED", 1200);

        OrderShipmentEvent shipEvent1 = OrderShipmentEvent.fromShipment(shipment1);
        OrderShipmentEvent shipEvent2 = OrderShipmentEvent.fromShipment(shipment2);

        // Add to buffer with KEY indexing
        KeyedIntervalJoinS2ROperator.addToBuildBuffer(
                shipmentBufferByKey, shipEvent1, shipEvent1.joinKey, shipEvent1.timestamp);
        KeyedIntervalJoinS2ROperator.addToBuildBuffer(
                shipmentBufferByKey, shipEvent2, shipEvent2.joinKey, shipEvent2.timestamp);

        // Reduced logging - shipments populated
        // System.out.println("Shipments in buffer:");
        // System.out.println("  - " + shipment1 + " (key=ORD-001)");
        // System.out.println("  - " + shipment2 + " (key=ORD-002)");

        // Now process an order
        Order order1 = new Order("ORD-001", "CUST-100", 99.99, 1000);
        OrderShipmentEvent orderEvent1 = OrderShipmentEvent.fromOrder(order1);

        // System.out.println("\nProcessing order: " + order1);
        // System.out.println("Interval window: [" + (1000 + lowerBound) + ", " + (1000 + upperBound) + "]");

        orderOperator.compute(orderEvent1, orderEvent1.timestamp);

        // Get results
        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content = 
                orderOperator.content(1000);
        List<OrderShipmentEvent> result = content.coalesce();

        // System.out.println("\nResults:");
        // for (OrderShipmentEvent event : result) {
        //     System.out.println("  - " + event);
        // }

        // Assertions
        assertEquals(2, result.size(), "Should have order + 1 matching shipment");
        assertTrue(result.contains(orderEvent1), "Should contain the order");
        assertTrue(result.contains(shipEvent1), "Should contain shipment for ORD-001");
        assertFalse(result.contains(shipEvent2), "Should NOT contain shipment for ORD-002 (wrong key)");

        System.out.println("\n✓ Test passed: Only shipments with matching orderId are joined\n");
    }*/

    @Test
    void testMultipleOrdersDifferentKeys() {
        /*
         * Scenario: Multiple orders, each should only match their own shipments
         * 
         * ORD-001 at t=1000 -> should match SHP-001 (ORD-001, t=1200)
         * ORD-002 at t=1100 -> should match SHP-002 (ORD-002, t=1300), SHP-003 (ORD-002, t=1400)
         * ORD-003 at t=1200 -> no matching shipments
         */
        // System.out.println("=== Test: Multiple Orders with Different Keys ===");

        long lowerBound = 0;
        long upperBound = 500;
        long stateRetention = 10000;

        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            orderOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    time,
                    "orderShipmentJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    shipmentBufferByKey,
                    event -> event.joinKey,
                    event -> event.timestamp,
                    stateRetention
            );

        // Add shipments
        addShipmentToBuffer("SHP-001", "ORD-001", "FedEx", 1200);
        addShipmentToBuffer("SHP-002", "ORD-002", "UPS", 1300);
        addShipmentToBuffer("SHP-003", "ORD-002", "DHL", 1400);

        // Process ORD-001 at t=1000, interval [1000, 1500]
        Order order1 = new Order("ORD-001", "CUST-A", 50.00, 1000);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order1), 1000);

        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content1 = 
                orderOperator.content(1000);
        List<OrderShipmentEvent> result1 = content1.coalesce();

        System.out.println("ORD-001 at t=1000, interval [1000, 1500]:");
        System.out.println("  Matches: " + countShipments(result1) + " shipment(s)");
        assertEquals(2, result1.size()); // order + 1 shipment
        assertTrue(hasShipmentId(result1, "SHP-001"));
        assertFalse(hasShipmentId(result1, "SHP-002")); // Wrong key
        assertFalse(hasShipmentId(result1, "SHP-003")); // Wrong key

        // Process ORD-002 at t=1100, interval [1100, 1600]
        Order order2 = new Order("ORD-002", "CUST-B", 75.00, 1100);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order2), 1100);

        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content2 = 
                orderOperator.content(1100);
        List<OrderShipmentEvent> result2 = content2.coalesce();

        System.out.println("ORD-002 at t=1100, interval [1100, 1600]:");
        System.out.println("  Matches: " + countShipments(result2) + " shipment(s)");
        assertEquals(3, result2.size()); // order + 2 shipments
        assertTrue(hasShipmentId(result2, "SHP-002"));
        assertTrue(hasShipmentId(result2, "SHP-003"));
        assertFalse(hasShipmentId(result2, "SHP-001")); // Wrong key

        // Process ORD-003 at t=1200, interval [1200, 1700] - no matching shipments
        Order order3 = new Order("ORD-003", "CUST-C", 100.00, 1200);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order3), 1200);

        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content3 = 
                orderOperator.content(1200);
        List<OrderShipmentEvent> result3 = content3.coalesce();

        System.out.println("ORD-003 at t=1200, interval [1200, 1700]:");
        System.out.println("  Matches: " + countShipments(result3) + " shipment(s)");
        assertEquals(1, result3.size()); // only the order itself

        System.out.println("\n✓ Test passed: Each order matches only its own shipments\n");
    }

   /* @Test
    void testShipmentOutsideTimeInterval() {
        *//*
         * Scenario: Shipment has correct key but is outside the time interval
         * 
         * Order ORD-001 at t=1000, interval [0, +500] = [1000, 1500]
         * Shipment for ORD-001 at t=2000 (correct key, but too late)
         *//*
        System.out.println("=== Test: Shipment Outside Time Interval ===");

        long lowerBound = 0;
        long upperBound = 500;
        long stateRetention = 10000;

        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            orderOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    time,
                    "orderShipmentJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    shipmentBufferByKey,
                    event -> event.joinKey,
                    event -> event.timestamp,
                    stateRetention
            );

        // Shipment at t=2000 (outside interval)
        addShipmentToBuffer("SHP-001", "ORD-001", "FedEx", 2000);

        // Process order at t=1000, interval [1000, 1500]
        Order order1 = new Order("ORD-001", "CUST-A", 50.00, 1000);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order1), 1000);

        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content = 
                orderOperator.content(1000);
        List<OrderShipmentEvent> result = content.coalesce();

        System.out.println("Order at t=1000, interval [1000, 1500]");
        System.out.println("Shipment at t=2000 (same key but outside interval)");
        System.out.println("Result: " + countShipments(result) + " shipment matches");

        assertEquals(1, result.size()); // Only the order, no shipment match
        assertFalse(hasShipmentId(result, "SHP-001"));

        System.out.println("\n✓ Test passed: Shipment with correct key but wrong time is excluded\n");
    }*/

   /* @Test
    void testNegativeLowerBoundWithKey() {
        *//*
         * Scenario: Order can match shipments that arrived BEFORE the order
         * (e.g., pre-shipment or inventory movement)
         * 
         * Order ORD-001 at t=1000, interval [-500, +500] = [500, 1500]
         * Shipment at t=700 (before order, but within interval)
         *//*
        System.out.println("=== Test: Negative Lower Bound (Pre-Order Shipments) ===");

        long lowerBound = -500;
        long upperBound = 500;
        long stateRetention = 10000;

        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            orderOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    time,
                    "orderShipmentJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    shipmentBufferByKey,
                    event -> event.joinKey,
                    event -> event.timestamp,
                    stateRetention
            );

        // Shipments
        addShipmentToBuffer("SHP-001", "ORD-001", "FedEx", 700);   // Before order, in interval
        addShipmentToBuffer("SHP-002", "ORD-001", "UPS", 1200);    // After order, in interval
        addShipmentToBuffer("SHP-003", "ORD-001", "DHL", 400);     // Before interval

        // Process order at t=1000, interval [500, 1500]
        Order order1 = new Order("ORD-001", "CUST-A", 50.00, 1000);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order1), 1000);

        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content = 
                orderOperator.content(1000);
        List<OrderShipmentEvent> result = content.coalesce();

        System.out.println("Order at t=1000, interval [500, 1500]");
        System.out.println("Shipments: t=400 (outside), t=700 (inside), t=1200 (inside)");
        System.out.println("Result: " + countShipments(result) + " shipment matches");

        assertEquals(3, result.size()); // order + 2 shipments
        assertTrue(hasShipmentId(result, "SHP-001"), "Should include pre-order shipment at t=700");
        assertTrue(hasShipmentId(result, "SHP-002"), "Should include post-order shipment at t=1200");
        assertFalse(hasShipmentId(result, "SHP-003"), "Should exclude shipment at t=400 (before interval)");

        System.out.println("\n✓ Test passed: Pre-order shipments within interval are matched\n");
    }*/

/*    @Test
    void testBidirectionalKeyedJoin() {
        *//*
         * Scenario: Both streams can trigger joins
         * - When an order arrives, find matching shipments
         * - When a shipment arrives, find matching orders
         *//*
        System.out.println("=== Test: Bidirectional Keyed Join ===");

        long lowerBound = -200;
        long upperBound = 200;
        long stateRetention = 10000;

        // Buffer for orders (from shipment's perspective)
        Map<String, TreeMap<Long, List<TimestampedElement<OrderShipmentEvent>>>> orderBufferByKey = 
                new HashMap<>();

        // Operator for ORDER stream (probes into shipment buffer)
        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            orderOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    time,
                    "orderJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    shipmentBufferByKey,
                    event -> event.joinKey,
                    event -> event.timestamp,
                    stateRetention
            );

        // Operator for SHIPMENT stream (probes into order buffer)
        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            shipmentOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    new TimeImpl(0),
                    "shipmentJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    orderBufferByKey,
                    event -> event.joinKey,
                    event -> event.timestamp,
                    stateRetention
            );

        // Step 1: Order arrives at t=1000
        Order order = new Order("ORD-001", "CUST-A", 100.00, 1000);
        OrderShipmentEvent orderEvent = OrderShipmentEvent.fromOrder(order);
        
        orderOperator.compute(orderEvent, 1000);
        // Also add to order buffer for shipment operator
        KeyedIntervalJoinS2ROperator.addToBuildBuffer(
                orderBufferByKey, orderEvent, orderEvent.joinKey, orderEvent.timestamp);

        System.out.println("Step 1: Order arrives at t=1000");
        System.out.println("  Order operator finds 0 shipments (none in buffer yet)");

        // Step 2: Shipment arrives at t=1100
        Shipment shipment = new Shipment("SHP-001", "ORD-001", "FedEx", "SHIPPED", 1100);
        OrderShipmentEvent shipEvent = OrderShipmentEvent.fromShipment(shipment);
        
        // Add to shipment buffer
        KeyedIntervalJoinS2ROperator.addToBuildBuffer(
                shipmentBufferByKey, shipEvent, shipEvent.joinKey, shipEvent.timestamp);
        shipmentOperator.compute(shipEvent, 1100);

        System.out.println("Step 2: Shipment arrives at t=1100");

        // Check: Shipment operator should find the order
        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> shipmentContent = 
                shipmentOperator.content(1100);
        List<OrderShipmentEvent> shipmentResult = shipmentContent.coalesce();

        System.out.println("  Shipment interval: [" + (1100 + lowerBound) + ", " + (1100 + upperBound) + "]");
        System.out.println("  Found order at t=1000? " + hasOrderId(shipmentResult, "ORD-001"));

        assertTrue(shipmentResult.contains(shipEvent), "Should contain the shipment itself");
        assertTrue(shipmentResult.contains(orderEvent), "Shipment should find the order");

        System.out.println("\n✓ Test passed: Bidirectional join works correctly\n");
    }*/

    @Test
    void testMultipleShipmentsPerOrder() {
        /*
         * Scenario: One order has multiple shipments (e.g., split shipment)
         * 
         * Order ORD-001 at t=1000
         * Shipments: SHP-001 at t=1100, SHP-002 at t=1200, SHP-003 at t=1300
         * All should match
         */
        System.out.println("=== Test: Multiple Shipments Per Order ===");

        long lowerBound = 0;
        long upperBound = 500;
        long stateRetention = 10000;

        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            orderOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    time,
                    "orderShipmentJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    shipmentBufferByKey,
                    event -> event.joinKey,
                    event -> event.timestamp,
                    stateRetention
            );

        // Add multiple shipments for same order
        addShipmentToBuffer("SHP-001", "ORD-001", "FedEx", 1100);
        addShipmentToBuffer("SHP-002", "ORD-001", "UPS", 1200);
        addShipmentToBuffer("SHP-003", "ORD-001", "DHL", 1300);

        // Process order
        Order order = new Order("ORD-001", "CUST-A", 150.00, 1000);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order), 1000);

        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content = 
                orderOperator.content(1000);
        List<OrderShipmentEvent> result = content.coalesce();

        System.out.println("Order ORD-001 at t=1000, interval [1000, 1500]");
        System.out.println("Found " + countShipments(result) + " shipments:");
        for (OrderShipmentEvent e : result) {
            if (e.isShipment()) {
                System.out.println("  - " + e.asShipment().shipmentId + " at t=" + e.timestamp);
            }
        }

        assertEquals(4, result.size()); // 1 order + 3 shipments
        assertTrue(hasShipmentId(result, "SHP-001"));
        assertTrue(hasShipmentId(result, "SHP-002"));
        assertTrue(hasShipmentId(result, "SHP-003"));

        System.out.println("\n✓ Test passed: All shipments for the order are matched\n");
    }
/*
    @Test
    void testStateEvictionByKey() {
        *//*
         * Scenario: Test that old events are properly evicted from keyed buffers
         *//*
        System.out.println("=== Test: State Eviction ===");

        long lowerBound = 0;
        long upperBound = 100;
        long stateRetention = 500; // Short retention for testing

        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            orderOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    time,
                    "orderShipmentJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    shipmentBufferByKey,
                    event -> event.joinKey,
                    event -> event.timestamp,
                    stateRetention
            );

        // Add shipment at t=100
        addShipmentToBuffer("SHP-001", "ORD-001", "FedEx", 100);

        // Process order at t=1000 (t=100 should be evicted: 1000 - 100 = 900 > 500 retention)
        Order order = new Order("ORD-001", "CUST-A", 50.00, 1000);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order), 1000);

        System.out.println("State retention: " + stateRetention);
        System.out.println("Shipment at t=100, Order at t=1000");
        System.out.println("Eviction threshold: " + (1000 - stateRetention) + " = 500");
        System.out.println("Build buffer keys after eviction: " + orderOperator.getBuildKeyCount());
        System.out.println("Build buffer size after eviction: " + orderOperator.getBuildBufferSize());

        // The old shipment should have been evicted
        assertEquals(0, orderOperator.getBuildBufferSize(), "Old events should be evicted");

        System.out.println("\n✓ Test passed: Old state is properly evicted\n");
    }*/

/*
    @Test
    void testRealWorldScenario() {
        */
/*
         * Real-world e-commerce scenario:
         * 
         * Timeline:
         * t=0:    Order ORD-001 placed ($99.99)
         * t=100:  Order ORD-002 placed ($149.99)
         * t=200:  Shipment SHP-001 for ORD-001 (PROCESSING)
         * t=300:  Shipment SHP-002 for ORD-002 (PROCESSING)
         * t=500:  Shipment SHP-003 for ORD-001 (SHIPPED)
         * t=1500: Shipment SHP-004 for ORD-001 (late, outside interval)
         * 
         * Interval: [0, +1000] (shipments within 1 second of order)
         *//*

        System.out.println("=== Test: Real-World E-Commerce Scenario ===\n");

        long lowerBound = 0;
        long upperBound = 1000;
        long stateRetention = 10000;

        KeyedIntervalJoinS2ROperator<OrderShipmentEvent, String, OrderShipmentEvent, List<OrderShipmentEvent>> 
            orderOperator = new KeyedIntervalJoinS2ROperator<>(
                    Tick.TIME_DRIVEN,
                    time,
                    "orderShipmentJoin",
                    contentFactory,
                    report,
                    lowerBound,
                    upperBound,
                    shipmentBufferByKey,
                    event -> event.joinKey,
                    event -> event.timestamp,
                    stateRetention
            );

        // Pre-populate shipments (simulating batch load or late-arriving stream)
        System.out.println("Shipment Stream:");
        addShipmentToBuffer("SHP-001", "ORD-001", "FedEx", 200);
        System.out.println("  t=200: SHP-001 for ORD-001 (PROCESSING)");
        addShipmentToBuffer("SHP-002", "ORD-002", "UPS", 300);
        System.out.println("  t=300: SHP-002 for ORD-002 (PROCESSING)");
        addShipmentToBuffer("SHP-003", "ORD-001", "FedEx", 500);
        System.out.println("  t=500: SHP-003 for ORD-001 (SHIPPED)");
        addShipmentToBuffer("SHP-004", "ORD-001", "FedEx", 1500);
        System.out.println("  t=1500: SHP-004 for ORD-001 (LATE)");

        System.out.println("\nOrder Stream:");

        // Process ORD-001 at t=0
        System.out.println("  t=0: Processing ORD-001 ($99.99)");
        Order order1 = new Order("ORD-001", "CUST-ALICE", 99.99, 0);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order1), 0);

        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content1 = 
                orderOperator.content(0);
        List<OrderShipmentEvent> result1 = content1.coalesce();

        System.out.println("    Interval: [0, 1000]");
        System.out.println("    Matched shipments: " + countShipments(result1));
        printShipmentMatches(result1);

        assertEquals(3, result1.size()); // order + SHP-001 + SHP-003
        assertTrue(hasShipmentId(result1, "SHP-001"));
        assertTrue(hasShipmentId(result1, "SHP-003"));
        assertFalse(hasShipmentId(result1, "SHP-002")); // Wrong key
        assertFalse(hasShipmentId(result1, "SHP-004")); // Outside interval

        // Process ORD-002 at t=100
        System.out.println("\n  t=100: Processing ORD-002 ($149.99)");
        Order order2 = new Order("ORD-002", "CUST-BOB", 149.99, 100);
        orderOperator.compute(OrderShipmentEvent.fromOrder(order2), 100);

        Content<OrderShipmentEvent, OrderShipmentEvent, List<OrderShipmentEvent>> content2 = 
                orderOperator.content(100);
        List<OrderShipmentEvent> result2 = content2.coalesce();

        System.out.println("    Interval: [100, 1100]");
        System.out.println("    Matched shipments: " + countShipments(result2));
        printShipmentMatches(result2);

        assertEquals(2, result2.size()); // order + SHP-002
        assertTrue(hasShipmentId(result2, "SHP-002"));
        assertFalse(hasShipmentId(result2, "SHP-001")); // Wrong key
        assertFalse(hasShipmentId(result2, "SHP-003")); // Wrong key

        System.out.println("\n✓ Real-world scenario test passed!\n");
        System.out.println("Summary:");
        System.out.println("  - ORD-001 matched with SHP-001, SHP-003 (same key, in interval)");
        System.out.println("  - ORD-001 did NOT match SHP-004 (same key, but late)");
        System.out.println("  - ORD-002 matched with SHP-002 only (correct key)");
    }
*/

    // ==================== Helper Methods ====================

    private void addShipmentToBuffer(String shipmentId, String orderId, String carrier, long timestamp) {
        Shipment shipment = new Shipment(shipmentId, orderId, carrier, "SHIPPED", timestamp);
        OrderShipmentEvent event = OrderShipmentEvent.fromShipment(shipment);
        KeyedIntervalJoinS2ROperator.addToBuildBuffer(
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

    private void printShipmentMatches(List<OrderShipmentEvent> events) {
        events.stream()
                .filter(OrderShipmentEvent::isShipment)
                .forEach(e -> {
                    Shipment s = e.asShipment();
                    System.out.println("      - " + s.shipmentId + " (orderId=" + s.orderId + ", t=" + s.timestamp + ")");
                });
    }

    private void addShipmentToBufferByKey(String shipmentId, String orderId, String carrier, long timestamp) {
        Shipment shipment = new Shipment(shipmentId, orderId, carrier, "SHIPPED", timestamp);
        OrderShipmentEvent event = OrderShipmentEvent.fromShipment(shipment);
        KeyedIntervalJoinS2ROperator.addToBuildBuffer(
                shipmentBufferByKey, event, event.joinKey, event.timestamp);
    }
}
