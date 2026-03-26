package benchmark.model;

import java.util.Objects;

/**
 * Order event — primary anchor for the three-stream join.
 * 
 * <p>The order defines the temporal reference point.
 * For classic interval join: window = [t - β, t + α]
 * For elastic operators: window may adapt based on data.</p>
 */
public class OrderEvent {
    public final String orderId;
    public final String customerId;
    public final double amount;
    public final long timestamp;

    public OrderEvent(String orderId, String customerId, double amount, long timestamp) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Order{" + orderId + ", customer=" + customerId + ", $" + amount + ", t=" + timestamp + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderEvent that = (OrderEvent) o;
        return Objects.equals(orderId, that.orderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId);
    }
}
