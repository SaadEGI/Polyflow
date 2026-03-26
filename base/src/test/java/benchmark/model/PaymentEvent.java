package benchmark.model;

import java.util.Objects;

/**
 * Payment event — validation evidence.
 * 
 * <p>Multiple payments per order are allowed (no 1-to-1 assumption).
 * Status can be PAID or FAILED.</p>
 */
public class PaymentEvent {
    public final String paymentId;
    public final String orderId;   // FK → OrderEvent.orderId
    public final String status;    // PAID or FAILED
    public final double amount;
    public final long timestamp;

    public PaymentEvent(String paymentId, String orderId, String status, double amount, long timestamp) {
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.status = status;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public boolean isPaid() {
        return "PAID".equals(status);
    }

    @Override
    public String toString() {
        return "Payment{" + paymentId + ", order=" + orderId + ", " + status + ", $" + amount + ", t=" + timestamp + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaymentEvent that = (PaymentEvent) o;
        return Objects.equals(paymentId, that.paymentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(paymentId);
    }
}
