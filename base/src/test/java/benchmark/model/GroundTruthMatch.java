package benchmark.model;

import java.util.Objects;

/**
 * A single ground truth match — the "perfect world" result.
 * 
 * <p>Each entry represents one correct (Order, Payment, Shipment) triple
 * as if we had full future knowledge. Built backward: first we define
 * the correct output, then we construct input streams that lead to it.</p>
 * 
 * <p>Note: An order CAN have multiple ground truth entries (one per valid shipment).
 * This is intentional — we do NOT assume 1-to-1 matching.</p>
 */
public class GroundTruthMatch {
    public final String orderId;
    public final String paymentId;
    public final String shipmentId;
    public final long orderTime;
    public final long paymentTime;
    public final long shipmentTime;

    /** Why this match is interesting for evaluation */
    public final MatchCategory category;

    public enum MatchCategory {
        /** Normal case: all within standard interval window */
        NORMAL,
        /** Payment arrives late (outside base window, within grace) */
        LATE_PAYMENT,
        /** Shipment arrives late (near or beyond content window boundary) */
        LATE_SHIPMENT,
        /** Multiple payments exist for this order — correct one selected */
        DUPLICATE_PAYMENT,
        /** Multiple shipments exist for this order — each is a valid match */
        MULTIPLE_SHIPMENTS,
        /** Both duplicate payments AND multiple shipments */
        COMPLEX,
        /** Order has payment but no shipment — should NOT appear in output */
        NO_SHIPMENT,
        /** Order has shipment but no payment — should NOT appear in output */
        NO_PAYMENT
    }

    public GroundTruthMatch(String orderId, String paymentId, String shipmentId,
                            long orderTime, long paymentTime, long shipmentTime,
                            MatchCategory category) {
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.shipmentId = shipmentId;
        this.orderTime = orderTime;
        this.paymentTime = paymentTime;
        this.shipmentTime = shipmentTime;
        this.category = category;
    }

    /**
     * Returns a composite key for this match triple.
     * Used to compare operator output against ground truth.
     */
    public String matchKey() {
        return orderId + "|" + paymentId + "|" + shipmentId;
    }

    /**
     * Returns a pair key (order, shipment) — ignoring which payment was used.
     * Useful for measuring content accuracy independently of validation.
     */
    public String contentKey() {
        return orderId + "|" + shipmentId;
    }

    @Override
    public String toString() {
        return "GT{" + orderId + ", " + paymentId + ", " + shipmentId +
               " | tO=" + orderTime + ", tP=" + paymentTime + ", tS=" + shipmentTime +
               " | " + category + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroundTruthMatch that = (GroundTruthMatch) o;
        return Objects.equals(orderId, that.orderId) &&
               Objects.equals(paymentId, that.paymentId) &&
               Objects.equals(shipmentId, that.shipmentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, paymentId, shipmentId);
    }
}
