package benchmark.model;

import java.util.Objects;

/**
 * Shipment event — content join partner.
 * 
 * <p>Multiple shipments per order are allowed (no 1-to-1 assumption).
 * This is realistic: partial shipments, re-shipments, etc.</p>
 */
public class ShipmentEvent {
    public final String shipmentId;
    public final String orderId;   // FK → OrderEvent.orderId
    public final String carrier;
    public final long timestamp;

    public ShipmentEvent(String shipmentId, String orderId, String carrier, long timestamp) {
        this.shipmentId = shipmentId;
        this.orderId = orderId;
        this.carrier = carrier;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Shipment{" + shipmentId + ", order=" + orderId + ", " + carrier + ", t=" + timestamp + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShipmentEvent that = (ShipmentEvent) o;
        return Objects.equals(shipmentId, that.shipmentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shipmentId);
    }
}
