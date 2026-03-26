package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin.*;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.SymmetricExpandingIntervalJoinOperator.*;

import java.util.List;

/**
 * Runnable example demonstrating the Symmetric Expanding Interval Join.
 * <p>
 * Scenario: Orders ⋈ Shipments on {@code orderId}.<br>
 * Initial window:  [tL − 1h , tL + 1h]  (α₀ = β₀ = 1 hour).<br>
 * Expansion step:  Δ = 1 hour, max = 4 hours.<br>
 * <p>
 * Some shipments are delayed and would be missed by the fixed 1-hour window.
 * The operator detects a "no match" and expands the window to recover them.
 */
public class SymmetricExpandingIntervalJoinExample {

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

        @Override public String toString() {
            return "Order{" + orderId + ", " + customer + ", $" + amount + " @" + timestamp + '}';
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

        @Override public String toString() {
            return "Shipment{" + shipmentId + " → " + orderId + ", " + carrier + " @" + timestamp + '}';
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

        @Override public String toString() {
            return "Matched{" + order.orderId + " ↔ " + shipment.shipmentId +
                    ", outTs=" + joinTimestamp + '}';
        }
    }

    // ── main ──

    public static void main(String[] args) {

        final long H = 3_600_000L; // 1 hour in ms

        // Build the operator
        var op = SymmetricExpandingIntervalJoinOperator
                .<String, Order, Shipment, OrderShipment>builder()
                .alpha0(H)               // initial future bound
                .beta0(H)                // initial past bound
                .alphaMax(4 * H)         // max future bound
                .betaMax(4 * H)          // max past bound
                .delta(H)               // expand by 1h each time
                .leftKeyExtractor(o -> o.orderId)
                .rightKeyExtractor(s -> s.orderId)
                .processJoinFunction((order, shipment, ctx, collector) ->
                        collector.collect(
                                new OrderShipment(order, shipment, ctx.getTimestamp()),
                                ctx.getTimestamp()))
                .build();

        long t0 = 1_000_000L;

        // ── Scenario 1: Normal match within initial window ──
        System.out.println("=== Scenario 1: Normal match ===");
        Order o1 = new Order("ORD-001", "Alice", 250.0, t0);
        Shipment s1 = new Shipment("SHP-A", "ORD-001", "FedEx", t0 + 30 * 60_000L); // 30 min later

        op.processLeft(o1, o1.timestamp);
        op.processRight(s1, s1.timestamp);

        for (var r : op.drainOutputs()) System.out.println("  " + r);
        System.out.println("  Bounds for ORD-001: α=" + op.getAlpha("ORD-001") +
                           ", β=" + op.getBeta("ORD-001"));

        // ── Scenario 2: Delayed shipment triggers expansion ──
        System.out.println("\n=== Scenario 2: Delayed shipment triggers expansion ===");
        Order o2 = new Order("ORD-002", "Bob", 180.0, t0);
        // Shipment arrives 2.5h later — outside the initial 1h window
        Shipment s2 = new Shipment("SHP-B", "ORD-002", "UPS", t0 + (long)(2.5 * H));

        op.processLeft(o2, o2.timestamp);
        // At this point no right-side match exists → expansion triggered
        System.out.println("  After order (no shipment yet):");
        System.out.println("    outputs: " + op.drainOutputs().size());
        System.out.println("    Bounds for ORD-002: α=" + op.getAlpha("ORD-002") +
                           ", β=" + op.getBeta("ORD-002"));

        // Now the shipment arrives at 2.5h — right side probes left with expanded bounds
        op.processRight(s2, s2.timestamp);
        System.out.println("  After shipment arrives:");
        var out2 = op.drainOutputs();
        for (var r : out2) System.out.println("    " + r);
        System.out.println("    Bounds for ORD-002: α=" + op.getAlpha("ORD-002") +
                           ", β=" + op.getBeta("ORD-002"));

        // ── Scenario 3: Expansion up to max ──
        System.out.println("\n=== Scenario 3: Multiple expansions to max ===");
        Order o3 = new Order("ORD-003", "Carol", 320.0, t0);
        op.processLeft(o3, o3.timestamp);   // no right match → expand

        // Still no match for ORD-003; send a right for a DIFFERENT key to trigger more processing
        op.processRight(new Shipment("SHP-X", "ORD-003", "DHL", t0 + 5 * H), t0 + 5 * H);
        // 5h is beyond even the first expansion (2h); right-side arrival with no left match → expand again

        System.out.println("  Bounds for ORD-003: α=" + op.getAlpha("ORD-003") +
                           ", β=" + op.getBeta("ORD-003"));

        // ── Show expansion log ──
        System.out.println("\n=== Expansion Log ===");
        for (var ev : op.drainExpansionLog()) {
            System.out.println("  " + ev);
        }

        // ── Late data ──
        System.out.println("\n=== Late data after watermark advance ===");
        op.advanceWatermark(t0 + 10 * H);
        op.processLeft(new Order("ORD-005", "Eve", 50.0, t0), t0); // way behind wm
        for (var so : op.drainSideOutputs()) System.out.println("  " + so);

        // ── State after cleanup ──
        System.out.println("\n=== Buffer sizes after cleanup ===");
        System.out.println("  leftBuffer : " + op.getLeftBufferSize());
        System.out.println("  rightBuffer: " + op.getRightBufferSize());
    }
}
