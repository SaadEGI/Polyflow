# Three-Stream Validation

This guide explains how to implement a **validation-dependent interval join** using the Polyflow framework, where:
- **Stream A (Orders)** - The main (PK) stream
- **Stream B (Shipments)** - The business join partner
- **Stream C (Payments)** - The validation stream

**Key insight**: An order from A should only participate in the content join (A ⋈ B) if it is validated by C (payment received).

---

## Phase 0: Semantic Specification

### Event Schemas

```java
// Stream A: Order events (Primary pivot stream)
public class Order {
    public final String orderId;       // Primary Key
    public final String customerId;
    public final double amount;
    public final long eventTime;       // Event timestamp
}

// Stream B: Shipment events (Business join partner)
public class Shipment {
    public final String shipmentId;
    public final String orderId;       // Foreign Key -> Order.orderId
    public final String carrier;
    public final long eventTime;
}

// Stream C: Payment events (Validation stream)
public class Payment {
    public final String paymentId;
    public final String orderId;       // Foreign Key -> Order.orderId
    public final String status;        // "PAID", "PENDING", "FAILED"
    public final long eventTime;
}
```

### Query Semantics

1. **Content Query**: Interval join between Orders and Shipments
   ```
   A ⋈ B WHERE shipment.eventTime ∈ [order.eventTime - L, order.eventTime + U]
            AND shipment.orderId = order.orderId
   ```

2. **Validation Predicate**:
   ```
   valid(order) ≡ ∃ payment ∈ C : 
       payment.orderId = order.orderId AND 
       payment.status = "PAID" AND
       payment.eventTime ∈ [order.eventTime, order.eventTime + ValidationWindow]
   ```

3. **Correct Output**: `(order, shipment)` only if:
   - shipment lies in content interval window of order
   - `valid(order)` is true

---

## Phase 1: Implementation Architecture

### Approach 1: Two-Task Design for Phase 1

```
                    ┌──────────────────┐
                    │  Validation Task │
Stream C ──────────►│  (Payment Check) │──────► Validation State
(Payments)          │  S2R: Window     │        (Set<orderId>)
                    └──────────────────┘
                            │
                            ▼
Stream A ───────┐   ┌──────────────────┐
(Orders)        │   │   Content Task   │
                ├──►│ (Order-Shipment) │──────► Output Stream
Stream B ───────┘   │ + Validation R2R │        (Valid Joins)
(Shipments)         └──────────────────┘
```



---

## Phase 1: Static Pipeline Implementation

### Step 1: Define Event Classes

```java
package thesis.events;

import java.util.Objects;

public class Order {
    public final String orderId;
    public final String customerId;
    public final double amount;
    public final long eventTime;
    
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

public class Shipment {
    public final String shipmentId;
    public final String orderId;
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

public class Payment {
    public final String paymentId;
    public final String orderId;
    public final String status;  // "PAID", "PENDING", "FAILED"
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
```

### Step 2: Create a Unified Event Wrapper

```java
package thesis.events;

/**
 * Wrapper to unify all three event types into a single stream type.
 * This allows using Polyflow's KeyedIntervalJoinS2ROperator.
 */
public class UnifiedEvent {
    public enum EventType { ORDER, SHIPMENT, PAYMENT }
    
    public final String joinKey;      // orderId in this case
    public final EventType eventType;
    public final Object payload;      // Order, Shipment or Payment
    public final long timestamp;
    
    private UnifiedEvent(String joinKey, EventType eventType, Object payload, long timestamp) {
        this.joinKey = joinKey;
        this.eventType = eventType;
        this.payload = payload;
        this.timestamp = timestamp;
    }
    
    public static UnifiedEvent fromOrder(Order order) {
        return new UnifiedEvent(order.orderId, EventType.ORDER, order, order.eventTime);
    }
    
    public static UnifiedEvent fromShipment(Shipment shipment) {
        return new UnifiedEvent(shipment.orderId, EventType.SHIPMENT, shipment, shipment.eventTime);
    }
    
    public static UnifiedEvent fromPayment(Payment payment) {
        return new UnifiedEvent(payment.orderId, EventType.PAYMENT, payment, payment.eventTime);
    }
    
    public boolean isOrder() { return eventType == EventType.ORDER; }
    public boolean isShipment() { return eventType == EventType.SHIPMENT; }
    public boolean isPayment() { return eventType == EventType.PAYMENT; }
    
    public Order asOrder() { return isOrder() ? (Order) payload : null; }
    public Shipment asShipment() { return isShipment() ? (Shipment) payload : null; }
    public Payment asPayment() { return isPayment() ? (Payment) payload : null; }
    
    @Override
    public String toString() {
        return String.format("%s{key=%s, time=%d, data=%s}", eventType, joinKey, timestamp, payload);
    }
}
```

### Step 3: Validation State Manager

```java
package thesis.validation;

import thesis.events.Payment;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages validation state: tracks which orders have been paid.
 * 
 * This is a simple "static" implementation that only considers
 * payments within a fixed validation window.
 * ONLY for the basic case
 */
public class ValidationStateManager {
    
    // orderId -> (paymentTime, isPaid)
    private final Map<String, PaymentInfo> validatedOrders = new ConcurrentHashMap<>();
    private final long validationWindowSize;  // How long payment can arrive after order, grace period possible to implement here?
    
    public ValidationStateManager(long validationWindowSize) {
        this.validationWindowSize = validationWindowSize;
    }
    
    /**
     * Record a payment event
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
        return info.paymentTime >= orderTime && 
               info.paymentTime <= orderTime + validationWindowSize;
    }
    
    /**
     * Check if order is validated, allowing late payments (for comparison)
     */
    public boolean isValidatedRelaxed(String orderId) {
        PaymentInfo info = validatedOrders.get(orderId);
        return info != null && info.isPaid;
    }
    
    /**
     * Clear old state for memory efficiency
     */
    public void evictBefore(long timestamp) {
        validatedOrders.entrySet().removeIf(
            entry -> entry.getValue().paymentTime < timestamp - validationWindowSize * 2
        );
    }
    
    private static class PaymentInfo {
        final long paymentTime;
        final boolean isPaid;
        
        PaymentInfo(long paymentTime, boolean isPaid) {
            this.paymentTime = paymentTime;
            this.isPaid = isPaid;
        }
    }
}
```

### Step 4: Validation-Aware R2R Operator

```java
package thesis.operators;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import thesis.events.UnifiedEvent;
import thesis.validation.ValidationStateManager;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * R2R Operator that filters join results based on validation state.
 * 
 * Input: List of UnifiedEvents (from interval join: orders + shipments)
 * Output: Filtered list containing only (order, shipment) pairs where order is validated
 */
public class ValidationFilterR2R implements RelationToRelationOperator<List<UnifiedEvent>> {
    
    private final ValidationStateManager validationManager;
    private final List<String> operandNames;
    private final String resultName;
    
    public ValidationFilterR2R(ValidationStateManager validationManager,
                               List<String> operandNames,
                               String resultName) {
        this.validationManager = validationManager;
        this.operandNames = operandNames;
        this.resultName = resultName;
    }
    
    @Override
    public List<UnifiedEvent> eval(List<List<UnifiedEvent>> operands) {
        if (operands.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<UnifiedEvent> joinResult = operands.get(0);
        
        // Separate orders and shipments
        List<UnifiedEvent> orders = joinResult.stream()
                .filter(UnifiedEvent::isOrder)
                .collect(Collectors.toList());
        
        List<UnifiedEvent> shipments = joinResult.stream()
                .filter(UnifiedEvent::isShipment)
                .collect(Collectors.toList());
        
        // Filter: only keep events for VALIDATED orders
        Set<String> validatedOrderIds = orders.stream()
                .filter(e -> validationManager.isValidated(e.joinKey, e.timestamp))
                .map(e -> e.joinKey)
                .collect(Collectors.toSet());
        
        // Return only events (orders + shipments) for validated orders
        return joinResult.stream()
                .filter(e -> validatedOrderIds.contains(e.joinKey))
                .collect(Collectors.toList());
    }
    
    @Override
    public List<String> getOperandNames() {
        return operandNames;
    }
    
    @Override
    public String getResultName() {
        return resultName;
    }
}
```

### Step 5: Output Result Class

```java
package thesis.events;

/**
 * Represents a validated join result: (Order, Shipment) pair
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
}
```

### Step 6: R2S Operator for Output

```java
package thesis.operators;

import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import thesis.events.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Converts the validated join results to output stream elements.
 */
public class ValidatedJoinR2S implements RelationToStreamOperator<List<UnifiedEvent>, ValidatedOrderShipment> {
    
    @Override
    public Collection<ValidatedOrderShipment> eval(List<UnifiedEvent> relation, long ts) {
        if (relation == null || relation.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Group by orderId
        Map<String, List<UnifiedEvent>> byOrderId = relation.stream()
                .collect(Collectors.groupingBy(e -> e.joinKey));
        
        List<ValidatedOrderShipment> results = new ArrayList<>();
        
        for (Map.Entry<String, List<UnifiedEvent>> entry : byOrderId.entrySet()) {
            List<UnifiedEvent> events = entry.getValue();
            
            // Find order and shipments for this orderId
            UnifiedEvent orderEvent = events.stream()
                    .filter(UnifiedEvent::isOrder)
                    .findFirst()
                    .orElse(null);
            
            if (orderEvent == null) continue;
            
            Order order = orderEvent.asOrder();
            
            // Create output for each shipment matched with this order
            for (UnifiedEvent e : events) {
                if (e.isShipment()) {
                    results.add(new ValidatedOrderShipment(
                            order, 
                            e.asShipment(), 
                            ts
                    ));
                }
            }
        }
        
        return results;
    }
}
```

### Step 7: Full Pipeline Setup

```java
package thesis;

import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.api.processing.ContinuousProgram;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnContentChange;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.KeyedIntervalJoinS2ROperator;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import org.streamreasoning.polyflow.base.sds.SDSDefault;
import org.streamreasoning.polyflow.base.stream.defaultDataStream;

import thesis.events.*;
import thesis.operators.*;
import thesis.validation.ValidationStateManager;

import java.util.*;

/**
 * Three-Stream Validation-Dependent Join Pipeline
 * 
 * Demonstrates the "static" baseline that shows incompleteness
 * when validation (payment) arrives late.
 */
public class ThreeStreamPipeline {
    
    // Configuration
    private static final long CONTENT_JOIN_LOWER_BOUND = 0;      // Shipment after order
    private static final long CONTENT_JOIN_UPPER_BOUND = 3600000; // Within 1 hour
    private static final long VALIDATION_WINDOW = 3600000;        // Payment within 1 hour
    private static final long STATE_RETENTION = 7200000;          // 2 hours
    
    // Components
    private final ContinuousProgram<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>, ValidatedOrderShipment> program;
    private final DataStream<UnifiedEvent> orderStream;
    private final DataStream<UnifiedEvent> shipmentStream;
    private final DataStream<UnifiedEvent> paymentStream;
    private final DataStream<ValidatedOrderShipment> outputStream;
    private final ValidationStateManager validationManager;
    
    // Shared buffer for shipments (used by interval join)
    private final Map<String, TreeMap<Long, List<KeyedIntervalJoinS2ROperator.TimestampedElement<UnifiedEvent>>>> 
            shipmentBuffer = new HashMap<>();
    
    // Results tracking for evaluation
    private final List<ValidatedOrderShipment> outputResults = Collections.synchronizedList(new ArrayList<>());
    
    public ThreeStreamPipeline() {
        // Initialize validation manager
        validationManager = new ValidationStateManager(VALIDATION_WINDOW);
        
        // Create streams
        orderStream = new defaultDataStream<>("orders");
        shipmentStream = new defaultDataStream<>("shipments");
        paymentStream = new defaultDataStream<>("payments");
        outputStream = new defaultDataStream<>("output");
        
        // Track outputs
        outputStream.addConsumer((stream, element, ts) -> outputResults.add(element));
        
        // Build the program
        program = buildProgram();
    }
    
    private ContinuousProgram<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>, ValidatedOrderShipment> buildProgram() {
        
        // Shared time
        Time time = new TimeImpl(0);
        
        // Report strategy: emit on content change
        Report report = new ReportImpl();
        report.add(new OnContentChange());
        
        // Content factory for accumulating events
        AccumulatorContentFactory<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>> contentFactory =
                new AccumulatorContentFactory<>(
                        e -> e,                                    // I -> W (identity)
                        e -> Collections.singletonList(e),         // W -> R
                        (l1, l2) -> {                              // Merge
                            List<UnifiedEvent> merged = new ArrayList<>(l1);
                            merged.addAll(l2);
                            return merged;
                        },
                        new ArrayList<>()                          // Empty
                );
        
        // S2R Operator: Keyed Interval Join (Orders ⋈ Shipments)
        KeyedIntervalJoinS2ROperator<UnifiedEvent, String, UnifiedEvent, List<UnifiedEvent>> 
                orderShipmentJoin = new KeyedIntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderShipmentJoin",
                        contentFactory,
                        report,
                        CONTENT_JOIN_LOWER_BOUND,
                        CONTENT_JOIN_UPPER_BOUND,
                        shipmentBuffer,
                        e -> e.joinKey,          // Key extractor (orderId)
                        e -> e.timestamp,        // Timestamp extractor
                        STATE_RETENTION
                );
        
        // R2R Operator: Filter by validation state
        RelationToRelationOperator<List<UnifiedEvent>> validationFilter = 
                new ValidationFilterR2R(
                        validationManager,
                        Collections.singletonList("orderShipmentJoin"),
                        "validatedJoin"
                );
        
        // R2S Operator: Convert to output
        RelationToStreamOperator<List<UnifiedEvent>, ValidatedOrderShipment> r2s = 
                new ValidatedJoinR2S();
        
        // Build Task
        Task<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>, ValidatedOrderShipment> task = 
                new TaskImpl<>("validatedJoinTask");
        
        task.addS2ROperator(orderShipmentJoin, orderStream)
            .addR2ROperator(validationFilter)
            .addR2SOperator(r2s)
            .addDAG(new DAGImpl<>())
            .addSDS(new SDSDefault<>())
            .addTime(time);
        
        task.initialize();
        
        // Build Continuous Program
        ContinuousProgramImpl<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>, ValidatedOrderShipment> 
                cp = new ContinuousProgramImpl<>();
        
        cp.buildTask(task, 
                Arrays.asList(orderStream), 
                Arrays.asList(outputStream));
        
        return cp;
    }
    
    // ============ Event Injection Methods ============
    
    public void injectOrder(Order order) {
        UnifiedEvent event = UnifiedEvent.fromOrder(order);
        orderStream.put(event, order.eventTime);
    }
    
    public void injectShipment(Shipment shipment) {
        UnifiedEvent event = UnifiedEvent.fromShipment(shipment);
        // Add to shared buffer for interval join
        KeyedIntervalJoinS2ROperator.addToBuildBuffer(
                shipmentBuffer, event, event.joinKey, event.timestamp);
        shipmentStream.put(event, shipment.eventTime);
    }
    
    public void injectPayment(Payment payment) {
        // Update validation state
        validationManager.recordPayment(payment);
        UnifiedEvent event = UnifiedEvent.fromPayment(payment);
        paymentStream.put(event, payment.eventTime);
    }
    
    // ============ Results Access ============
    
    public List<ValidatedOrderShipment> getOutputResults() {
        return new ArrayList<>(outputResults);
    }
    
    public void clearResults() {
        outputResults.clear();
    }
}
```

---

## Phase 1: Test Data Generator

```java
package thesis.testdata;

import thesis.events.*;
import thesis.ThreeStreamPipeline;

import java.util.*;

/**
 * Generates test cases to demonstrate incompleteness.
 */
public class TestDataGenerator {
    
    /**
     * Case S1: Normal case - payment arrives in time
     * Expected: Join output produced
     */
    public static void generateCaseS1_Normal(ThreeStreamPipeline pipeline) {
        long baseTime = 1000;
        
        // Order at t=1000
        Order order = new Order("ORD-S1", "CUST-1", 100.0, baseTime);
        
        // Payment at t=1100 (100ms after order - within window)
        Payment payment = new Payment("PAY-S1", "ORD-S1", "PAID", baseTime + 100);
        
        // Shipment at t=1500 (within join window)
        Shipment shipment = new Shipment("SHP-S1", "ORD-S1", "FedEx", baseTime + 500);
        
        // Inject in order: payment first (so validation state is ready)
        pipeline.injectPayment(payment);
        pipeline.injectShipment(shipment);
        pipeline.injectOrder(order);
        
        System.out.println("Case S1 (Normal): Order with quick payment");
        System.out.println("  Order: " + order);
        System.out.println("  Payment: " + payment);
        System.out.println("  Shipment: " + shipment);
        System.out.println("  Expected: JOIN OUTPUT PRODUCED");
    }
    
    /**
     * Case S2: Late validation - payment arrives after validation window
     * Expected: Join output MISSING (thiss demonstrates incompleteness)
     */
    public static void generateCaseS2_LateValidation(ThreeStreamPipeline pipeline) {
        long baseTime = 10000;
        long validationWindow = 3600000; // 1 hour
        
        // Order at t=10000
        Order order = new Order("ORD-S2", "CUST-2", 200.0, baseTime);
        
        // Payment at t=10000 + 2 hours (LATE - outside validation window!)
        Payment payment = new Payment("PAY-S2", "ORD-S2", "PAID", baseTime + 2 * validationWindow);
        
        // Shipment at t=10500 (within join window)
        Shipment shipment = new Shipment("SHP-S2", "ORD-S2", "UPS", baseTime + 500);
        
        // Inject shipment first, then order (payment comes late)
        pipeline.injectShipment(shipment);
        pipeline.injectOrder(order);
        // Payment arrives much later
        pipeline.injectPayment(payment);
        
        System.out.println("\nCase S2 (Late Validation): Payment arrives after validation window");
        System.out.println("  Order: " + order);
        System.out.println("  Payment: " + payment + " (LATE!)");
        System.out.println("  Shipment: " + shipment);
        System.out.println("  Expected: JOIN OUTPUT MISSING (demonstrates incompleteness)");
    }
    
    /**
     * Case S3: Late shipment - shipment arrives after join window
     * Expected: No join output (correct behavior - soundness check)
     */
    public static void generateCaseS3_LateShipment(ThreeStreamPipeline pipeline) {
        long baseTime = 20000;
        long joinUpperBound = 3600000; // 1 hour
        
        // Order at t=20000
        Order order = new Order("ORD-S3", "CUST-3", 150.0, baseTime);
        
        // Payment at t=20100 (within validation window)
        Payment payment = new Payment("PAY-S3", "ORD-S3", "PAID", baseTime + 100);
        
        // Shipment at t=20000 + 2 hours (OUTSIDE join window!)
        Shipment shipment = new Shipment("SHP-S3", "ORD-S3", "DHL", baseTime + 2 * joinUpperBound);
        
        pipeline.injectPayment(payment);
        pipeline.injectShipment(shipment);
        pipeline.injectOrder(order);
        
        System.out.println("\nCase S3 (Late Shipment): Shipment outside join window");
        System.out.println("  Order: " + order);
        System.out.println("  Payment: " + payment);
        System.out.println("  Shipment: " + shipment + " (LATE!)");
        System.out.println("  Expected: NO JOIN OUTPUT (correct - soundness)");
    }
    
    /**
     * Case S4: Missing payment - no payment ever arrives
     * Expected: No join output (correct behavior)
     */
    public static void generateCaseS4_MissingPayment(ThreeStreamPipeline pipeline) {
        long baseTime = 30000;
        
        // Order at t=30000
        Order order = new Order("ORD-S4", "CUST-4", 250.0, baseTime);
        
        // NO payment!
        
        // Shipment at t=30500 (within join window)
        Shipment shipment = new Shipment("SHP-S4", "ORD-S4", "USPS", baseTime + 500);
        
        pipeline.injectShipment(shipment);
        pipeline.injectOrder(order);
        
        System.out.println("\nCase S4 (Missing Payment): No payment arrives");
        System.out.println("  Order: " + order);
        System.out.println("  Payment: NONE");
        System.out.println("  Shipment: " + shipment);
        System.out.println("  Expected: NO JOIN OUTPUT (correct - no validation)");
    }
}
```

---

## Phase 1: Evaluation Framework

```java
package thesis.evaluation;

import thesis.events.*;
import thesis.ThreeStreamPipeline;
import thesis.testdata.TestDataGenerator;

import java.util.*;

/**
 * Evaluates the static pipeline to demonstrate incompleteness.
 */
public class PipelineEvaluator {
    
    public static class EvaluationResult {
        public final int expectedJoins;
        public final int actualJoins;
        public final double precision;  // correct outputs / actual outputs
        public final double recall;     // correct outputs / expected outputs
        public final List<String> missedJoins = new ArrayList<>();
        public final List<String> incorrectJoins = new ArrayList<>();
        
        public EvaluationResult(int expected, int actual, Set<String> expectedIds, Set<String> actualIds) {
            this.expectedJoins = expected;
            this.actualJoins = actual;
            
            // Calculate precision and recall
            Set<String> correct = new HashSet<>(expectedIds);
            correct.retainAll(actualIds);
            
            this.precision = actualJoins == 0 ? 1.0 : (double) correct.size() / actualJoins;
            this.recall = expectedJoins == 0 ? 1.0 : (double) correct.size() / expectedJoins;
            
            // Track missed and incorrect
            for (String id : expectedIds) {
                if (!actualIds.contains(id)) {
                    missedJoins.add(id);
                }
            }
            for (String id : actualIds) {
                if (!expectedIds.contains(id)) {
                    incorrectJoins.add(id);
                }
            }
        }
        
        @Override
        public String toString() {
            return String.format(
                "Evaluation Results:\n" +
                "  Expected joins: %d\n" +
                "  Actual joins:   %d\n" +
                "  Precision:      %.2f\n" +
                "  Recall:         %.2f\n" +
                "  Missed:         %s\n" +
                "  Incorrect:      %s",
                expectedJoins, actualJoins, precision, recall, missedJoins, incorrectJoins
            );
        }
    }
    
    public static void main(String[] args) {
        System.out.println("=== Three-Stream Validation-Dependent Join Evaluation ===\n");
        
        ThreeStreamPipeline pipeline = new ThreeStreamPipeline();
        
        // Run all test cases
        TestDataGenerator.generateCaseS1_Normal(pipeline);
        TestDataGenerator.generateCaseS2_LateValidation(pipeline);
        TestDataGenerator.generateCaseS3_LateShipment(pipeline);
        TestDataGenerator.generateCaseS4_MissingPayment(pipeline);
        
        // Get results
        List<ValidatedOrderShipment> outputs = pipeline.getOutputResults();
        
        System.out.println("\n=== Actual Outputs ===");
        for (ValidatedOrderShipment result : outputs) {
            System.out.println("  " + result);
        }
        
        // Expected outputs (ground truth)
        Set<String> expectedOrderIds = new HashSet<>(Arrays.asList(
            "ORD-S1"  // Only S1 should produce output
            // S2 is late payment (should produce but won't - incompleteness!)
            // S3 is late shipment (correctly no output)
            // S4 is missing payment (correctly no output)
        ));
        
        // With perfect system, S2 would also produce output
        Set<String> expectedWithLateHandling = new HashSet<>(Arrays.asList(
            "ORD-S1", "ORD-S2"
        ));
        
        Set<String> actualOrderIds = new HashSet<>();
        for (ValidatedOrderShipment out : outputs) {
            actualOrderIds.add(out.order.orderId);
        }
        
        System.out.println("\n=== Evaluation (Static Baseline) ===");
        EvaluationResult result = new EvaluationResult(
            expectedWithLateHandling.size(),
            actualOrderIds.size(),
            expectedWithLateHandling,
            actualOrderIds
        );
        System.out.println(result);
        
        System.out.println("\n=== Key Insight ===");
        System.out.println("Precision should be ~1.0 (sound - no incorrect outputs)");
        System.out.println("Recall < 1.0 when late payments exist (incomplete)");
        System.out.println("This demonstrates the need for dynamic adaptation!");
    }
}
```

---

## Summary: What You've Built

### Phase 0 Deliverable
- Clear semantic specification (schemas, queries, correctness definition)

### Phase 1 Deliverables

1. **Event Classes**: `Order`, `Shipment`, `Payment`, `UnifiedEvent`
2. **Validation State**: `ValidationStateManager` tracks paid orders
3. **S2R Operator**: Uses `KeyedIntervalJoinS2ROperator` for A ⋈ B
4. **R2R Operator**: `ValidationFilterR2R` filters by validation state
5. **R2S Operator**: `ValidatedJoinR2S` produces output
6. **Pipeline**: `ThreeStreamPipeline` wires everything together
7. **Test Data**: Four cases (S1-S4) demonstrating normal, late, and missing scenarios
8. **Evaluation**: Measures precision (soundness) and recall (completeness)

### Expected Results

| Case | Payment | Shipment | Expected Output | Static Baseline |
|------|---------|----------|-----------------|-----------------|
| S1   | On time | On time  | ✓ Join          | ✓ Join          |
| S2   | **LATE**| On time  | ✓ Join          | ✗ **MISSED**    |
| S3   | On time | **LATE** | ✗ No join       | ✗ No join       |
| S4   | Missing | On time  | ✗ No join       | ✗ No join       |


---

## Next Steps (Phase 2)
### Approach 2: Single-Task with Custom R2R Operator (NOT done YET)

```
                ┌─────────────────────────────────────────┐
Stream A ──────►│                                         │
(Orders)        │         Three-Way Task                  │
                │                                         │
Stream B ──────►│  S2R_OrderShipment: Interval Join       │──► Output
(Shipments)     │  S2R_Payment: Validation Window         │
                │  R2R: Filter by validated orders        │
Stream C ──────►│                                         │
(Payments)      └─────────────────────────────────────────┘
```

