package org.streamreasoning.polyflow.examples.thesis.events;

/**
 * Represents a validated join result: (Order, Shipment) pair
 * 
 * This is the output type of the three-stream pipeline.
 * A ValidatedOrderShipment is only produced when:
 * 1. The shipment is within the content interval window of the order
 * 2. The order has been validated (paid) within the validation window
 */
public class ValidatedOrderShipment {
    public final Order order;
    public final Shipment shipment;
    public final long outputTime;
    
    public ValidatedOrderShipment(Order order, Shipment shipment, long outputTime) {
        this.order = order;
        this.shipment = shipment;
        this.outputTime = outputTime;
    }
    
    @Override
    public String toString() {
        return String.format("ValidatedJoin{order=%s, shipment=%s, time=%d}",
                order.orderId, shipment.shipmentId, outputTime);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidatedOrderShipment that = (ValidatedOrderShipment) o;
        return order.equals(that.order) && shipment.equals(that.shipment);
    }
    
    @Override
    public int hashCode() {
        int result = order.hashCode();
        result = 31 * result + shipment.hashCode();
        return result;
    }
}
