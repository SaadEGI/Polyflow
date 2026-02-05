package org.streamreasoning.polyflow.examples.thesis.events;

import java.util.Objects;

/**
 * Stream A: Order events (Primary pivot stream)
 * 
 * This is the "anchor" around which windows are defined.
 * Join key: orderId
 */
public class Order {
    public final String orderId;       // Primary Key
    public final String customerId;
    public final double amount;
    public final long eventTime;       // Event timestamp
    
    public Order(String orderId, String customerId, double amount, long eventTime) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.eventTime = eventTime;
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
    
    @Override
    public String toString() {
        return String.format("Order{id=%s, customer=%s, amount=%.2f, time=%d}",
                orderId, customerId, amount, eventTime);
    }
}
