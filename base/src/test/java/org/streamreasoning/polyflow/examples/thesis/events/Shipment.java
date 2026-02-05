package org.streamreasoning.polyflow.examples.thesis.events;

import java.util.Objects;

/**
 * Stream B: Shipment events (Business join partner)
 * 
 * Content query uses Order ⋈ Shipment based on orderId.
 * Join key: orderId (foreign key)
 */
public class Shipment {
    public final String shipmentId;
    public final String orderId;       // Foreign Key -> Order.orderId
    public final String carrier;
    public final long eventTime;
    
    public Shipment(String shipmentId, String orderId, String carrier, long eventTime) {
        this.shipmentId = shipmentId;
        this.orderId = orderId;
        this.carrier = carrier;
        this.eventTime = eventTime;
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
    
    @Override
    public String toString() {
        return String.format("Shipment{id=%s, orderId=%s, carrier=%s, time=%d}",
                shipmentId, orderId, carrier, eventTime);
    }
}
