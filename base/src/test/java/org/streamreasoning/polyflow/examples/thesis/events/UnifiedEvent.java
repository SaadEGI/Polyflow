package org.streamreasoning.polyflow.examples.thesis.events;

import java.util.Objects;

/**
 * Unified Event Wrapper
 * 
 * Wraps Order, Shipment, and Payment events into a single type
 * to allow using Polyflow's KeyedIntervalJoinS2ROperator which
 * expects a uniform event type.
 * 
 * The joinKey (orderId) is the shared key across all three streams.
 */
public class UnifiedEvent {
    
    public enum EventType { 
        ORDER,      // From Stream A (pivot)
        SHIPMENT,   // From Stream B (business join partner)
        PAYMENT     // From Stream C (validation)
    }
    
    public final String joinKey;      // orderId (shared key for joins)
    public final EventType eventType;
    public final Object payload;      // Order, Shipment, or Payment
    public final long timestamp;
    
    private UnifiedEvent(String joinKey, EventType eventType, Object payload, long timestamp) {
        this.joinKey = joinKey;
        this.eventType = eventType;
        this.payload = payload;
        this.timestamp = timestamp;
    }
    
    // Factory methods
    
    public static UnifiedEvent fromOrder(Order order) {
        return new UnifiedEvent(order.orderId, EventType.ORDER, order, order.eventTime);
    }
    
    public static UnifiedEvent fromShipment(Shipment shipment) {
        return new UnifiedEvent(shipment.orderId, EventType.SHIPMENT, shipment, shipment.eventTime);
    }
    
    public static UnifiedEvent fromPayment(Payment payment) {
        return new UnifiedEvent(payment.orderId, EventType.PAYMENT, payment, payment.eventTime);
    }
    
    // Type checking methods
    
    public boolean isOrder() { 
        return eventType == EventType.ORDER; 
    }
    
    public boolean isShipment() { 
        return eventType == EventType.SHIPMENT; 
    }
    
    public boolean isPayment() { 
        return eventType == EventType.PAYMENT; 
    }
    
    // Type casting methods
    
    public Order asOrder() { 
        return isOrder() ? (Order) payload : null; 
    }
    
    public Shipment asShipment() { 
        return isShipment() ? (Shipment) payload : null; 
    }
    
    public Payment asPayment() { 
        return isPayment() ? (Payment) payload : null; 
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnifiedEvent that = (UnifiedEvent) o;
        return timestamp == that.timestamp &&
               eventType == that.eventType &&
               Objects.equals(joinKey, that.joinKey) &&
               Objects.equals(payload, that.payload);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(joinKey, eventType, payload, timestamp);
    }
    
    @Override
    public String toString() {
        return String.format("%s{key=%s, time=%d, data=%s}", 
                eventType, joinKey, timestamp, payload);
    }
}
