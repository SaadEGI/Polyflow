package org.streamreasoning.polyflow.examples.thesis.validation;

import org.streamreasoning.polyflow.examples.thesis.events.Payment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages validation state: tracks which orders have been paid.
 * 
 * This is a simple "static" implementation that only considers
 * payments within a fixed validation window. This demonstrates
 * the incompleteness problem when payments arrive late.
 * 
 * Key insight from supervisor:
 * "An element from A (Order) should only participate in the content join 
 * if it is validated by C (Payment)."
 */
public class ValidationStateManager {
    
    // orderId -> PaymentInfo
    private final Map<String, PaymentInfo> validatedOrders = new ConcurrentHashMap<>();
    
    // How long payment can arrive after order to be considered valid
    private final long validationWindowSize;
    
    /**
     * Create a validation state manager.
     * 
     * @param validationWindowSize The maximum time after an order that a payment
     *                             can arrive and still validate the order.
     *                             E.g., 3600000 = 1 hour
     */
    public ValidationStateManager(long validationWindowSize) {
        this.validationWindowSize = validationWindowSize;
    }
    
    /**
     * Record a payment event.
     * Only PAID payments are recorded as validation.
     */
    public void recordPayment(Payment payment) {
        if (payment.isPaid()) {
            validatedOrders.put(payment.orderId, 
                new PaymentInfo(payment.eventTime, true));
        }
    }
    
    /**
     * Check if an order is validated (paid) within the validation window.
     * 
     * This is the STRICT check that enforces the validation window.
     * If payment arrives late (outside the window), this returns false,
     * demonstrating the incompleteness problem.
     * 
     * @param orderId The order to check
     * @param orderTime The timestamp of the order
     * @return true if a PAID payment exists within [orderTime, orderTime + validationWindow]
     */
    public boolean isValidated(String orderId, long orderTime) {
        PaymentInfo info = validatedOrders.get(orderId);
        if (info == null || !info.isPaid) {
            return false;
        }
        // Check if payment is within validation window
        // Payment must be: orderTime <= paymentTime <= orderTime + validationWindow
        return info.paymentTime >= orderTime && 
               info.paymentTime <= orderTime + validationWindowSize;
    }
    
    /**
     * Check if order is validated without time constraints.
     * 
     * This is a RELAXED check useful for comparison:
     * - Shows what the "correct" output would be if we could wait forever
     * - Demonstrates the gap between static (incomplete) and ideal behavior
     */
    public boolean isValidatedRelaxed(String orderId) {
        PaymentInfo info = validatedOrders.get(orderId);
        return info != null && info.isPaid;
    }
    
    /**
     * Check if a payment has been recorded for this order (regardless of timing).
     */
    public boolean hasPayment(String orderId) {
        return validatedOrders.containsKey(orderId);
    }
    
    /**
     * Get the payment time for an order, or -1 if no payment exists.
     */
    public long getPaymentTime(String orderId) {
        PaymentInfo info = validatedOrders.get(orderId);
        return info != null ? info.paymentTime : -1;
    }
    
    /**
     * Clear old state for memory efficiency.
     * Removes payments that are older than 2x the validation window.
     */
    public void evictBefore(long timestamp) {
        long threshold = timestamp - validationWindowSize * 2;
        validatedOrders.entrySet().removeIf(
            entry -> entry.getValue().paymentTime < threshold
        );
    }
    
    /**
     * Get the number of validated orders currently tracked.
     */
    public int size() {
        return validatedOrders.size();
    }
    
    /**
     * Clear all validation state (for testing).
     */
    public void clear() {
        validatedOrders.clear();
    }
    
    /**
     * Internal class to store payment information.
     */
    private static class PaymentInfo {
        final long paymentTime;
        final boolean isPaid;
        
        PaymentInfo(long paymentTime, boolean isPaid) {
            this.paymentTime = paymentTime;
            this.isPaid = isPaid;
        }
    }
}
