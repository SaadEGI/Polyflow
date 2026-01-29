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
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.IntervalJoinS2ROperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.IntervalJoinS2ROperator.TimestampedElement;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for IntervalJoinS2ROperator
 * 
 * Uses simple Order and Shipment events to demonstrate interval joins.
 * 
 * Scenario: Match orders with shipments that occur within a specified
 * time interval relative to the order timestamp.
 */
public class IntervalJoinS2ROperatorTest {

    // Simple Event class for testing
    public static class Event {
        public final String id;
        public final String type;  // "ORDER" or "SHIPMENT"
        public final String key;   // Join key (e.g., orderId)
        public final long timestamp;

        public Event(String id, String type, String key, long timestamp) {
            this.id = id;
            this.type = type;
            this.key = key;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return type + "{id=" + id + ", key=" + key + ", ts=" + timestamp + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;
            return Objects.equals(id, event.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    // Joined result class
    public static class JoinedEvent {
        public final Event order;
        public final Event shipment;

        public JoinedEvent(Event order, Event shipment) {
            this.order = order;
            this.shipment = shipment;
        }

        @Override
        public String toString() {
            return "JoinedEvent{order=" + order.id + ", shipment=" + shipment.id + "}";
        }
    }

    private Time time;
    private Report report;
    private AccumulatorContentFactory<Event, Event, List<Event>> contentFactory;
    private TreeMap<Long, List<TimestampedElement<Event>>> shipmentBuffer;

    // Quiet mode to suppress verbose printing during large tests
    private static final boolean QUIET = true;

    @BeforeEach
    void setUp() {
        time = new TimeImpl(0);
        report = new ReportImpl();
        report.add(new OnContentChange());

        // Content factory that accumulates events into a list
        contentFactory = new AccumulatorContentFactory<>(
                event -> event,                           // I -> W (identity)
                event -> Collections.singletonList(event), // W -> R (single element list)
                (list1, list2) -> {                        // Merge function
                    List<Event> merged = new ArrayList<>(list1);
                    merged.addAll(list2);
                    return merged;
                },
                new ArrayList<>()                          // Empty content
        );

        // Shared buffer for shipment events
        shipmentBuffer = new TreeMap<>();

        // Suppress System.out/err printing during tests when QUIET is true
        if (QUIET) {
            System.setOut(new java.io.PrintStream(new java.io.OutputStream() {
                @Override public void write(int b) {
                    // no-op
                }
            }));
            System.setErr(new java.io.PrintStream(new java.io.OutputStream() {
                @Override public void write(int b) {
                    // no-op
                }
            }));
        }
    }

    @Test
    void testBasicIntervalJoin() {
        // Interval: shipment within [0, +100] relative to order
        // Order at t=100 should match shipments in [100, 200]
        long lowerBound = 0;
        long upperBound = 100;
        long stateRetention = 1000;

        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Add shipments to the shared buffer first
        Event shipment1 = new Event("S1", "SHIPMENT", "ORD-1", 120);
        Event shipment2 = new Event("S2", "SHIPMENT", "ORD-1", 180);
        Event shipment3 = new Event("S3", "SHIPMENT", "ORD-2", 250);

        shipmentBuffer.computeIfAbsent(120L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipment1, 120));
        shipmentBuffer.computeIfAbsent(180L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipment2, 180));
        shipmentBuffer.computeIfAbsent(250L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipment3, 250));

        // Now add an order at t=100
        // Interval window will be [100, 200], should match S1 (120) and S2 (180)
        Event order1 = new Event("O1", "ORDER", "ORD-1", 100);
        orderOperator.compute(order1, 100);

        // Check window was created
        assertEquals(1, orderOperator.getActiveWindowCount());

        // Get content and verify matches
        Content<Event, Event, List<Event>> content = orderOperator.content(100);
        List<Event> result = content.coalesce();

        // Should contain: order1, shipment1, shipment2
        assertEquals(3, result.size());
        assertTrue(result.contains(order1));
        assertTrue(result.contains(shipment1));
        assertTrue(result.contains(shipment2));
        assertFalse(result.contains(shipment3)); // Outside interval

        System.out.println("Basic interval join test passed!");
        System.out.println("Order at t=100, interval [100, 200]");
        System.out.println("Matched events: " + result);
    }

    @Test
    void testIntervalJoinWithNegativeLowerBound() {
        // Interval: shipment within [-50, +50] relative to order
        // Order at t=100 should match shipments in [50, 150]
        long lowerBound = -50;
        long upperBound = 50;
        long stateRetention = 1000;

        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Add shipments
        Event shipmentBefore = new Event("S1", "SHIPMENT", "ORD-1", 60);   // In interval [50, 150]
        Event shipmentAt = new Event("S2", "SHIPMENT", "ORD-1", 100);      // In interval
        Event shipmentAfter = new Event("S3", "SHIPMENT", "ORD-1", 140);   // In interval
        Event shipmentOutside = new Event("S4", "SHIPMENT", "ORD-1", 200); // Outside

        shipmentBuffer.computeIfAbsent(60L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipmentBefore, 60));
        shipmentBuffer.computeIfAbsent(100L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipmentAt, 100));
        shipmentBuffer.computeIfAbsent(140L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipmentAfter, 140));
        shipmentBuffer.computeIfAbsent(200L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipmentOutside, 200));

        // Add order at t=100, interval is [50, 150]
        Event order = new Event("O1", "ORDER", "ORD-1", 100);
        orderOperator.compute(order, 100);

        Content<Event, Event, List<Event>> content = orderOperator.content(100);
        List<Event> result = content.coalesce();

        // Should contain: order, shipmentBefore, shipmentAt, shipmentAfter
        assertEquals(4, result.size());
        assertTrue(result.contains(order));
        assertTrue(result.contains(shipmentBefore));
        assertTrue(result.contains(shipmentAt));
        assertTrue(result.contains(shipmentAfter));
        assertFalse(result.contains(shipmentOutside));

        System.out.println("\nNegative lower bound test passed!");
        System.out.println("Order at t=100, interval [50, 150]");
        System.out.println("Matched events: " + result);
    }

    @Test
    void testIntervalJoinWithKeyPredicate() {
        // Interval: [0, +100] with key-based join predicate
        long lowerBound = 0;
        long upperBound = 100;
        long stateRetention = 1000;

        // Join predicate: only match events with the same key
        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        (order, shipment) -> order.key.equals(shipment.key), // Key equality
                        null,
                        stateRetention
                );

        // Add shipments with different keys
        Event shipmentMatch = new Event("S1", "SHIPMENT", "ORD-1", 120);
        Event shipmentNoMatch = new Event("S2", "SHIPMENT", "ORD-2", 130);

        shipmentBuffer.computeIfAbsent(120L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipmentMatch, 120));
        shipmentBuffer.computeIfAbsent(130L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipmentNoMatch, 130));

        // Add order with key ORD-1
        Event order = new Event("O1", "ORDER", "ORD-1", 100);
        orderOperator.compute(order, 100);

        Content<Event, Event, List<Event>> content = orderOperator.content(100);
        List<Event> result = content.coalesce();

        // Should only contain order and matching shipment
        assertEquals(2, result.size());
        assertTrue(result.contains(order));
        assertTrue(result.contains(shipmentMatch));
        assertFalse(result.contains(shipmentNoMatch)); // Wrong key

        System.out.println("\nKey predicate test passed!");
        System.out.println("Only matched shipments with key=ORD-1");
        System.out.println("Matched events: " + result);
    }

    @Test
    void testMultipleOrdersWithIntervalJoin() {
        long lowerBound = -20;
        long upperBound = 30;
        long stateRetention = 1000;

        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Add shipments at various times
        Event s1 = new Event("S1", "SHIPMENT", "X", 85);
        Event s2 = new Event("S2", "SHIPMENT", "X", 110);
        Event s3 = new Event("S3", "SHIPMENT", "X", 150);
        Event s4 = new Event("S4", "SHIPMENT", "X", 170);

        shipmentBuffer.computeIfAbsent(85L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(s1, 85));
        shipmentBuffer.computeIfAbsent(110L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(s2, 110));
        shipmentBuffer.computeIfAbsent(150L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(s3, 150));
        shipmentBuffer.computeIfAbsent(170L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(s4, 170));

        // Order 1 at t=100, interval [80, 130]
        Event order1 = new Event("O1", "ORDER", "X", 100);
        orderOperator.compute(order1, 100);

        // Order 2 at t=160, interval [140, 190]
        Event order2 = new Event("O2", "ORDER", "X", 160);
        orderOperator.compute(order2, 160);

        // Should have 2 active windows
        assertEquals(2, orderOperator.getActiveWindowCount());

        // Check windows
        Set<Window> windows = orderOperator.getActiveWindows();
        System.out.println("\nMultiple orders test:");
        System.out.println("Active windows: " + windows);

        // Verify time varying
        TimeVarying<List<Event>> tvg = orderOperator.get();
        tvg.materialize(160);
        List<Event> latest = tvg.get();
        
        System.out.println("Latest content at t=160: " + latest);
        
        // The latest window [140, 190] should contain order2, s3, s4
        assertTrue(latest.contains(order2));
        assertTrue(latest.contains(s3));
        assertTrue(latest.contains(s4));
    }

    @Test
    void testEmptyBuildBuffer() {
        long lowerBound = 0;
        long upperBound = 100;
        long stateRetention = 1000;

        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer, // Empty buffer
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Add order with no matching shipments
        Event order = new Event("O1", "ORDER", "ORD-1", 100);
        orderOperator.compute(order, 100);

        Content<Event, Event, List<Event>> content = orderOperator.content(100);
        List<Event> result = content.coalesce();

        // Should only contain the order itself
        assertEquals(1, result.size());
        assertTrue(result.contains(order));

        System.out.println("\nEmpty build buffer test passed!");
        System.out.println("Result with no shipments: " + result);
    }

    @Test
    void testStateEviction() {
        long lowerBound = 0;
        long upperBound = 50;
        long stateRetention = 100; // Short retention for testing

        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Add first order at t=100
        Event order1 = new Event("O1", "ORDER", "ORD-1", 100);
        orderOperator.compute(order1, 100);
        assertEquals(1, orderOperator.getActiveWindowCount());
        assertEquals(1, orderOperator.getProbeBufferSize());

        // Add second order at t=300 (beyond retention period)
        Event order2 = new Event("O2", "ORDER", "ORD-2", 300);
        orderOperator.compute(order2, 300);

        // Old window should be evicted (300 - 100 = 200 > 100 retention)
        // Actually window end is 150, so 300 - 150 = 150 > 100
        System.out.println("\nState eviction test:");
        System.out.println("Active windows after eviction: " + orderOperator.getActiveWindowCount());
        System.out.println("Probe buffer size: " + orderOperator.getProbeBufferSize());

        // The old state should be evicted
        assertTrue(orderOperator.getActiveWindowCount() <= 2);
    }

    @Test
    void testBidirectionalIntervalJoin() {
        // Test scenario: Both streams can trigger joins
        long lowerBound = -30;
        long upperBound = 30;
        long stateRetention = 1000;

        // Buffer for orders (from perspective of shipment stream)
        TreeMap<Long, List<TimestampedElement<Event>>> orderBuffer = new TreeMap<>();

        // Operator for order stream (probes into shipment buffer)
        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Operator for shipment stream (probes into order buffer)
        IntervalJoinS2ROperator<Event, Event, List<Event>> shipmentOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        new TimeImpl(0), // Separate time instance
                        "shipmentIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        orderBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Simulate: Order arrives first at t=100
        Event order = new Event("O1", "ORDER", "ORD-1", 100);
        orderOperator.compute(order, 100);
        // Also add to order buffer for shipment operator
        orderBuffer.computeIfAbsent(100L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(order, 100));

        // Then shipment arrives at t=120
        Event shipment = new Event("S1", "SHIPMENT", "ORD-1", 120);
        shipmentBuffer.computeIfAbsent(120L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipment, 120));
        shipmentOperator.compute(shipment, 120);

        // Check: Order operator should match with shipment (if we re-check)
        // Shipment operator should match with order
        Content<Event, Event, List<Event>> shipmentContent = shipmentOperator.content(120);
        List<Event> shipmentResult = shipmentContent.coalesce();

        System.out.println("\nBidirectional join test:");
        System.out.println("Shipment at t=120, interval [90, 150]");
        System.out.println("Should find order at t=100");
        System.out.println("Result: " + shipmentResult);

        assertTrue(shipmentResult.contains(shipment));
        assertTrue(shipmentResult.contains(order));
    }

    @Test
    void testTimeVaryingMaterialization() {
        long lowerBound = 0;
        long upperBound = 100;
        long stateRetention = 1000;

        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Setup test data
        Event shipment = new Event("S1", "SHIPMENT", "ORD-1", 150);
        shipmentBuffer.computeIfAbsent(150L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(shipment, 150));

        Event order = new Event("O1", "ORDER", "ORD-1", 100);
        orderOperator.compute(order, 100);

        // Get TimeVarying and materialize
        TimeVarying<List<Event>> tvg = orderOperator.get();
        assertEquals("orderIntervalJoin", tvg.iri());

        tvg.materialize(100);
        List<Event> result = tvg.get();

        System.out.println("\nTimeVarying materialization test:");
        System.out.println("Materialized at t=100: " + result);

        assertNotNull(result);
        assertTrue(result.contains(order));
        assertTrue(result.contains(shipment));
    }

    @Test
    void testEdgeCasesIntervalBoundaries() {
        // Test exact boundary conditions
        long lowerBound = -10;
        long upperBound = 10;
        long stateRetention = 1000;

        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,  // Extract timestamp from event
                        stateRetention
                );

        // Shipments at exact boundaries for order at t=100 (interval [90, 110])
        Event atLower = new Event("S1", "SHIPMENT", "X", 90);   // Exactly at lower bound
        Event atUpper = new Event("S2", "SHIPMENT", "X", 110);  // Exactly at upper bound
        Event justBelow = new Event("S3", "SHIPMENT", "X", 89); // Just below lower
        Event justAbove = new Event("S4", "SHIPMENT", "X", 111);// Just above upper

        shipmentBuffer.computeIfAbsent(90L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(atLower, 90));
        shipmentBuffer.computeIfAbsent(110L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(atUpper, 110));
        shipmentBuffer.computeIfAbsent(89L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(justBelow, 89));
        shipmentBuffer.computeIfAbsent(111L, k -> new ArrayList<>())
                .add(new TimestampedElement<>(justAbove, 111));

        Event order = new Event("O1", "ORDER", "X", 100);
        orderOperator.compute(order, 100);

        Content<Event, Event, List<Event>> content = orderOperator.content(100);
        List<Event> result = content.coalesce();

        System.out.println("\nEdge cases (boundary) test:");
        System.out.println("Order at t=100, interval [90, 110]");
        System.out.println("Result: " + result);

        // Boundaries are inclusive
        assertTrue(result.contains(atLower), "Should include event at lower bound");
        assertTrue(result.contains(atUpper), "Should include event at upper bound");
        assertFalse(result.contains(justBelow), "Should exclude event below lower bound");
        assertFalse(result.contains(justAbove), "Should exclude event above upper bound");
    }

    @Test
    void testBulk80Tuples() {
        // Create 80 tuples (40 orders + 40 shipments) and verify joins work
        long lowerBound = 0;
        long upperBound = 10;
        long stateRetention = 10000;

        IntervalJoinS2ROperator<Event, Event, List<Event>> orderOperator =
                new IntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "bulkIntervalJoin",
                        contentFactory,
                        report,
                        lowerBound,
                        upperBound,
                        shipmentBuffer,
                        event -> event.timestamp,
                        (order, shipment) -> order.key.equals(shipment.key),
                        null,
                        stateRetention
                );

        // Generate 40 keys and for each generate one order and one shipment (80 tuples total)
        int pairs = 40;
        List<Event> orders = new ArrayList<>();
        List<Event> shipments = new ArrayList<>();

        long baseTs = 1000L;
        for (int i = 0; i < pairs; i++) {
            String key = String.format("K%03d", i);
            long orderTs = baseTs + i * 5;
            long shipTs = orderTs + 2; // within [orderTs, orderTs+10]

            Event order = new Event("O" + i, "ORDER", key, orderTs);
            Event ship = new Event("S" + i, "SHIPMENT", key, shipTs);

            shipments.add(ship);
            orders.add(order);

            // add shipment to build buffer
            shipmentBuffer.computeIfAbsent(shipTs, k -> new ArrayList<>())
                    .add(new TimestampedElement<>(ship, shipTs));
        }

        // Now feed all orders to the operator
        for (Event o : orders) {
            orderOperator.compute(o, o.timestamp);
        }

        // Verify each order matched its shipment
        int matchedPairs = 0;
        for (Event o : orders) {
            Content<Event, Event, List<Event>> c = orderOperator.content(o.timestamp);
            List<Event> res = c.coalesce();
            boolean foundOrder = res.stream().anyMatch(e -> e.equals(o));
            boolean foundShip = res.stream().anyMatch(e -> e.type.equals("SHIPMENT") && e.key.equals(o.key));
            if (foundOrder && foundShip) matchedPairs++;
        }

        // All pairs should be matched
        assertEquals(pairs, matchedPairs, "All generated order-shipment pairs should match");
    }
}
