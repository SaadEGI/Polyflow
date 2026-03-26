package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin.*;

import java.util.List;

/**
 * Runnable example: Orders ⋈ Shipments interval join.
 * <p>
 * Join condition:
 * <pre>
 *   order.orderId == shipment.orderId
 *   AND order_time BETWEEN ship_time − 4h AND ship_time
 * </pre>
 * Rewritten to the canonical form (right timestamp relative to left):
 * <pre>
 *   tR ∈ [tL + 0, tL + 4h]   (lowerBound=0, upperBound=4h in ms)
 * </pre>
 * That is: the shipment may happen between 0 and 4 hours after the order.
 */
public class IntervalJoinExample {

    // ── domain objects ──

    public static class Order {
        public final String orderId;
        public final String customer;
        public final double amount;
        public final long timestamp;

        public Order(String orderId, String customer, double amount, long ts) {
            this.orderId = orderId;
            this.customer = customer;
            this.amount = amount;
            this.timestamp = ts;
        }

        @Override
        public String toString() {
            return "Order{" + orderId + ", $" + amount + " @" + timestamp + '}';
        }
    }

    public static class Shipment {
        public final String shipmentId;
        public final String orderId;
        public final String carrier;
        public final long timestamp;

        public Shipment(String shipmentId, String orderId, String carrier, long ts) {
            this.shipmentId = shipmentId;
            this.orderId = orderId;
            this.carrier = carrier;
            this.timestamp = ts;
        }

        @Override
        public String toString() {
            return "Shipment{" + shipmentId + ", orderId=" + orderId + " @" + timestamp + '}';
        }
    }

    public static class OrderShipment {
        public final Order order;
        public final Shipment shipment;
        public final long joinTimestamp;

        public OrderShipment(Order o, Shipment s, long ts) {
            this.order = o;
            this.shipment = s;
            this.joinTimestamp = ts;
        }

        @Override
        public String toString() {
            return "Joined{order=" + order.orderId + ", shipment=" + shipment.shipmentId +
                    ", outTs=" + joinTimestamp + '}';
        }
    }

    // ── main ──

    public static void main(String[] args) {

        final long HOUR = 3_600_000L; // 1 hour in milliseconds

        // Build the operator:  tR ∈ [tL + 0, tL + 4h]
        FlinkIntervalJoinOperator<String, Order, Shipment, OrderShipment> op =
                FlinkIntervalJoinOperator.<String, Order, Shipment, OrderShipment>builder()
                        .lowerBound(0)
                        .upperBound(4 * HOUR)
                        .leftKeyExtractor(o -> o.orderId)
                        .rightKeyExtractor(s -> s.orderId)
                        .processJoinFunction((order, shipment, ctx, collector) ->
                                collector.collect(
                                        new OrderShipment(order, shipment, ctx.getTimestamp()),
                                        ctx.getTimestamp()))
                        .build();

        // ── Simulate two streams ──

        long t0 = 1_000_000L;

        // Orders
        Order o1 = new Order("ORD-001", "Alice", 250.0, t0);
        Order o2 = new Order("ORD-002", "Bob",   180.0, t0 + HOUR);
        Order o3 = new Order("ORD-003", "Carol", 320.0, t0 + 2 * HOUR);

        // Shipments
        Shipment s1 = new Shipment("SHP-A", "ORD-001", "FedEx",  t0 + 2 * HOUR);  // within 4h of o1
        Shipment s2 = new Shipment("SHP-B", "ORD-001", "UPS",    t0 + 5 * HOUR);  // outside 4h of o1
        Shipment s3 = new Shipment("SHP-C", "ORD-002", "DHL",    t0 + 2 * HOUR);  // within 4h of o2
        Shipment s4 = new Shipment("SHP-D", "ORD-003", "FedEx",  t0 + 3 * HOUR);  // within 4h of o3

        // Process left records (orders)
        System.out.println("=== Processing Orders ===");
        op.processLeft(o1, o1.timestamp);
        op.processLeft(o2, o2.timestamp);
        op.processLeft(o3, o3.timestamp);

        // Process right records (shipments)
        System.out.println("\n=== Processing Shipments ===");
        op.processRight(s1, s1.timestamp);
        op.processRight(s2, s2.timestamp);
        op.processRight(s3, s3.timestamp);
        op.processRight(s4, s4.timestamp);

        // Drain and print results
        System.out.println("\n=== Join Results ===");
        List<FlinkIntervalJoinOperator.TimestampedOutput<OrderShipment>> results = op.drainOutputs();
        for (var r : results) {
            System.out.println("  " + r);
        }

        // Late data: advance watermark, then send a late order
        System.out.println("\n=== Advancing watermark to t0 + 6h ===");
        op.advanceWatermark(t0 + 6 * HOUR);

        Order latOrder = new Order("ORD-004", "Dave", 99.0, t0);  // way behind watermark
        op.processLeft(latOrder, latOrder.timestamp);

        System.out.println("\n=== Late Side Outputs ===");
        for (var so : op.drainSideOutputs()) {
            System.out.println("  " + so);
        }

        // Show state after cleanup
        System.out.println("\n=== Buffer sizes after cleanup ===");
        System.out.println("  leftBuffer : " + op.getLeftBufferSize());
        System.out.println("  rightBuffer: " + op.getRightBufferSize());
    }
}
