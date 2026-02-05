package org.streamreasoning.polyflow.examples.thesis.events;

import java.util.Objects;

/**
 * Stream C: Payment events (Validation stream)
 * 
 * Validation query checks whether an order is valid (paid).
 * An order should only participate in the content join if it is validated by a payment.
 * 
 * Join key: orderId (foreign key)
 */
public class Payment {
    public final String paymentId;
    public final String orderId;       // Foreign Key -> Order.orderId
    public final String status;        // "PAID", "PENDING", "FAILED"
    public final long eventTime;
    
    public Payment(String paymentId, String orderId, String status, long eventTime) {
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.status = status;
        this.eventTime = eventTime;
    }
    
    public boolean isPaid() {
        return "PAID".equals(status);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payment p = (Payment) o;
        return Objects.equals(paymentId, p.paymentId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(paymentId);
    }
    
    @Override
    public String toString() {
        return String.format("Payment{id=%s, orderId=%s, status=%s, time=%d}",
                paymentId, orderId, status, eventTime);
    }
}
