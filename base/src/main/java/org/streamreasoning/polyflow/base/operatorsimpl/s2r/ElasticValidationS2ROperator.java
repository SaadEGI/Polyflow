package org.streamreasoning.polyflow.base.operatorsimpl.s2r;

import org.apache.log4j.Logger;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.WindowImpl;
import org.streamreasoning.polyflow.api.sds.timevarying.TimeVarying;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeInstant;
import org.streamreasoning.polyflow.base.sds.TimeVaryingObject;

import java.util.*;
import java.util.function.Function;

/**
 * One Elastic S2R Operator for Three-Stream Validation Pattern
 * 
 * <p>This operator accumulates events from three streams (A: Orders, B: Shipments, C: Payments)
 * in a single windowed relation per key. It outputs only finalized results based on elastic
 * validation rules.</p>
 * 
 * <h3>Pipeline Concept:</h3>
 * <ul>
 *   <li>S2R: Accumulates UnifiedEvents in time-based key-retention buckets</li>
 *   <li>Internal R2R: Computes output using elastic validation rules:
 *       <ul>
 *         <li>For each order, check if payment exists within [tO, tO+V+G]</li>
 *         <li>If yes and shipments exist within [tO, tO+U] → emit validated results</li>
 *         <li>If no and time ≤ tO+V+G → keep pending (no output yet)</li>
 *         <li>If no and time > tO+V+G → finalize as invalid (emit nothing for this order)</li>
 *       </ul>
 *   </li>
 *   <li>Output: ValidatedOrderShipment events</li>
 * </ul>
 * 
 * <h3>Key Benefits:</h3>
 * <ul>
 *   <li>Payments naturally trigger re-evaluation because they change the relation</li>
 *   <li>No manual callbacks needed when payment arrives</li>
 *   <li>Clean finalization semantics: orders are finalized as valid or invalid</li>
 * </ul>
 * 
 * @param <I> Input event type (UnifiedEvent containing Order/Shipment/Payment)
 * @param <K> Key type for joining (typically orderId)
 * @param <W> Internal working type
 * @param <R> Result type (Iterable)
 */
public class ElasticValidationS2ROperator<I, K, W, R extends Iterable<?>>
        implements StreamToRelationOperator<I, W, R> {

    private static final Logger log = Logger.getLogger(ElasticValidationS2ROperator.class);

    /**
     * Finalization status for an order
     */
    public enum FinalizationStatus {
        /** Still waiting for validation or content events */
        PENDING,
        /** Order is validated and has shipments - ready to emit */
        VALID,
        /** Order validation window expired without payment - invalid */
        INVALID,
        /** Order content window expired without shipments - no content */
        NO_CONTENT
    }

    /**
     * Result of validation evaluation
     */
    public static class ValidationResult<I> {
        public final FinalizationStatus status;
        public final List<I> results;  // Non-empty only if VALID
        
        public ValidationResult(FinalizationStatus status, List<I> results) {
            this.status = status;
            this.results = results != null ? results : Collections.emptyList();
        }
        
        public static <I> ValidationResult<I> pending() {
            return new ValidationResult<>(FinalizationStatus.PENDING, null);
        }
        
        public static <I> ValidationResult<I> invalid() {
            return new ValidationResult<>(FinalizationStatus.INVALID, null);
        }
        
        public static <I> ValidationResult<I> noContent() {
            return new ValidationResult<>(FinalizationStatus.NO_CONTENT, null);
        }
        
        public static <I> ValidationResult<I> valid(List<I> results) {
            return new ValidationResult<>(FinalizationStatus.VALID, results);
        }
    }

    // Core Polyflow components
    private final Tick tick;
    private final Time time;
    private final String name;
    private final ContentFactory<I, W, R> cf;
    private final Report report;

    // Elastic window parameters
    private final long contentWindowSize;      // U: Max time for shipment after order
    private final long validationWindowSize;   // V: Validation window for payment
    private final long gracePeriod;            // G: Additional grace period for late arrivals

    // Retention: max(U, V+G) to ensure all relevant events coexist
    private final long stateRetentionTime;

    // Key extractors
    private final Function<I, K> keyExtractor;
    private final Function<I, Long> timestampExtractor;
    private final Function<I, EventCategory> categoryExtractor;

    // Event categorization
    public enum EventCategory { ORDER, SHIPMENT, PAYMENT }

    // State per key
    private final Map<K, KeyedEventState<I>> stateByKey = new HashMap<>();

    // Completed/finalized windows with their content
    private final Map<Window, Content<I, W, R>> finalizedWindows = new LinkedHashMap<>();

    // Windows that have been reported
    private final List<Window> reportedWindows = new ArrayList<>();

    /**
     * Holds all events for a single key (orderId)
     */
    public static class KeyedEventState<I> {
        public final List<TimestampedEvent<I>> orders = new ArrayList<>();
        public final List<TimestampedEvent<I>> shipments = new ArrayList<>();
        public final List<TimestampedEvent<I>> payments = new ArrayList<>();
        public final Set<Long> finalizedOrderTimestamps = new HashSet<>();
        public long lastUpdateTime = 0;

        public boolean isEmpty() {
            return orders.isEmpty() && shipments.isEmpty() && payments.isEmpty();
        }

        @Override
        public String toString() {
            return String.format("State{orders=%d, shipments=%d, payments=%d}", 
                orders.size(), shipments.size(), payments.size());
        }
    }

    /**
     * Wrapper for timestamped events
     */
    public static class TimestampedEvent<T> {
        public final T event;
        public final long timestamp;

        public TimestampedEvent(T event, long timestamp) {
            this.event = event;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "TS{" + event + "@" + timestamp + "}";
        }
    }

    /**
     * Factory for creating validated join results
     */
    @FunctionalInterface
    public interface ValidatedResultFactory<I> {
        /**
         * Create a validated result from an order and matching shipment
         */
        I createValidatedResult(I orderEvent, I shipmentEvent, I paymentEvent, long outputTime);
    }

    private final ValidatedResultFactory<I> resultFactory;

    /**
     * Full constructor
     *
     * @param tick                 Tick strategy
     * @param time                 Shared time object
     * @param name                 Operator name
     * @param cf                   Content factory
     * @param report               Report strategy
     * @param contentWindowSize    U: Max time for shipment after order
     * @param validationWindowSize V: Validation window for payment
     * @param gracePeriod          G: Additional grace period
     * @param keyExtractor         Extracts join key from event
     * @param timestampExtractor   Extracts timestamp from event
     * @param categoryExtractor    Determines if event is ORDER, SHIPMENT, or PAYMENT
     * @param resultFactory        Factory to create validated results
     */
    public ElasticValidationS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long contentWindowSize,
            long validationWindowSize,
            long gracePeriod,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            Function<I, EventCategory> categoryExtractor,
            ValidatedResultFactory<I> resultFactory) {

        this.tick = tick;
        this.time = time;
        this.name = name;
        this.cf = cf;
        this.report = report;
        this.contentWindowSize = contentWindowSize;
        this.validationWindowSize = validationWindowSize;
        this.gracePeriod = gracePeriod;
        this.keyExtractor = keyExtractor;
        this.timestampExtractor = timestampExtractor;
        this.categoryExtractor = categoryExtractor;
        this.resultFactory = resultFactory;

        // Retention must cover max(U, V+G) to ensure all relevant events coexist
        this.stateRetentionTime = Math.max(contentWindowSize, validationWindowSize + gracePeriod);

        log.info(String.format("ElasticValidationS2R created: contentWindow=%d, validationWindow=%d, grace=%d, retention=%d",
                contentWindowSize, validationWindowSize, gracePeriod, stateRetentionTime));
    }

    @Override
    public void compute(I element, long ts) {
        K key = keyExtractor.apply(element);
        EventCategory category = categoryExtractor.apply(element);
        
        log.debug(String.format("Received %s for key=%s at ts=%d", category, key, ts));

        // Step 1: Add event to appropriate state bucket
        KeyedEventState<I> state = stateByKey.computeIfAbsent(key, k -> new KeyedEventState<>());
        TimestampedEvent<I> tsEvent = new TimestampedEvent<>(element, ts);

        switch (category) {
            case ORDER:
                state.orders.add(tsEvent);
                break;
            case SHIPMENT:
                state.shipments.add(tsEvent);
                break;
            case PAYMENT:
                state.payments.add(tsEvent);
                break;
        }
        state.lastUpdateTime = ts;

        // Step 2: Re-evaluate all pending orders for this key
        // Payments trigger re-evaluation naturally
        evaluateAndFinalizeOrders(key, state, ts);

        // Step 3: Check for globally expired orders (across all keys)
        checkGlobalExpiredOrders(ts);

        // Step 4: Update application time
        time.setAppTime(ts);

        // Step 5: Evict old state
        evictOldState(ts);
    }

    /**
     * Evaluate all orders for a key and finalize those that are ready
     */
    private void evaluateAndFinalizeOrders(K key, KeyedEventState<I> state, long currentTime) {
        List<TimestampedEvent<I>> ordersToProcess = new ArrayList<>(state.orders);

        for (TimestampedEvent<I> orderEvent : ordersToProcess) {
            long orderTime = orderEvent.timestamp;

            // Skip already finalized orders
            if (state.finalizedOrderTimestamps.contains(orderTime)) {
                continue;
            }

            ValidationResult<I> result = evaluateOrder(orderEvent, state, currentTime);

            switch (result.status) {
                case VALID:
                    // Emit validated results
                    finalizeWithResults(key, orderEvent, result.results, currentTime);
                    state.finalizedOrderTimestamps.add(orderTime);
                    log.debug(String.format("Order finalized as VALID: key=%s, orderTime=%d, results=%d",
                            key, orderTime, result.results.size()));
                    break;

                case INVALID:
                    // Finalize as invalid (no output)
                    state.finalizedOrderTimestamps.add(orderTime);
                    log.debug(String.format("Order finalized as INVALID: key=%s, orderTime=%d", key, orderTime));
                    break;

                case NO_CONTENT:
                    // Finalize with no content (no shipments within window)
                    state.finalizedOrderTimestamps.add(orderTime);
                    log.debug(String.format("Order finalized as NO_CONTENT: key=%s, orderTime=%d", key, orderTime));
                    break;

                case PENDING:
                    // Keep waiting
                    log.trace(String.format("Order still PENDING: key=%s, orderTime=%d", key, orderTime));
                    break;
            }
        }
    }

    /**
     * Evaluate a single order against the elastic validation rules
     */
    private ValidationResult<I> evaluateOrder(TimestampedEvent<I> orderEvent, 
                                               KeyedEventState<I> state, 
                                               long currentTime) {
        long orderTime = orderEvent.timestamp;
        long validationDeadline = orderTime + validationWindowSize + gracePeriod;  // tO + V + G
        long contentDeadline = orderTime + contentWindowSize;  // tO + U

        // Step 1: Check for valid payment within [tO, tO + V + G]
        TimestampedEvent<I> validPayment = findValidPayment(state.payments, orderTime, validationDeadline);

        // Step 2: Check for shipments within [tO, tO + U]
        List<TimestampedEvent<I>> validShipments = findValidShipments(state.shipments, orderTime, contentDeadline);

        // Step 3: Apply elastic rules
        if (validPayment != null) {
            // Payment exists
            if (!validShipments.isEmpty()) {
                // Payment + Shipments → VALID, emit results
                List<I> results = new ArrayList<>();
                for (TimestampedEvent<I> shipment : validShipments) {
                    I result = resultFactory.createValidatedResult(
                            orderEvent.event, shipment.event, validPayment.event, currentTime);
                    results.add(result);
                }
                return ValidationResult.valid(results);
            } else if (currentTime > contentDeadline) {
                // Payment exists but content window expired → NO_CONTENT
                return ValidationResult.noContent();
            } else {
                // Payment exists but still waiting for shipments
                return ValidationResult.pending();
            }
        } else {
            // No payment yet
            if (currentTime > validationDeadline) {
                // Validation window + grace expired → INVALID
                return ValidationResult.invalid();
            } else {
                // Still within validation window
                return ValidationResult.pending();
            }
        }
    }

    /**
     * Find a valid payment within the validation window
     */
    private TimestampedEvent<I> findValidPayment(List<TimestampedEvent<I>> payments, 
                                                   long orderTime, 
                                                   long validationDeadline) {
        for (TimestampedEvent<I> payment : payments) {
            // Payment must be within [orderTime, validationDeadline]
            if (payment.timestamp >= orderTime && payment.timestamp <= validationDeadline) {
                return payment;
            }
        }
        return null;
    }

    /**
     * Find all valid shipments within the content window
     */
    private List<TimestampedEvent<I>> findValidShipments(List<TimestampedEvent<I>> shipments,
                                                          long orderTime,
                                                          long contentDeadline) {
        List<TimestampedEvent<I>> valid = new ArrayList<>();
        for (TimestampedEvent<I> shipment : shipments) {
            // Shipment must be within [orderTime, contentDeadline]
            if (shipment.timestamp >= orderTime && shipment.timestamp <= contentDeadline) {
                valid.add(shipment);
            }
        }
        return valid;
    }

    /**
     * Finalize an order with validated results
     */
    private void finalizeWithResults(K key, TimestampedEvent<I> orderEvent, 
                                       List<I> results, long currentTime) {
        Window window = new WindowImpl(orderEvent.timestamp, currentTime);
        Content<I, W, R> content = cf.create();

        for (I result : results) {
            content.add(result);
        }

        finalizedWindows.put(window, content);

        // Report if conditions are met
        if (report.report(window, content, currentTime, System.currentTimeMillis())) {
            reportedWindows.add(window);
            time.addEvaluationTimeInstants(new TimeInstant(currentTime));
            log.debug(String.format("Reported finalized window for key=%s with %d results", key, results.size()));
        }
    }

    /**
     * Check for expired orders across all keys
     */
    private void checkGlobalExpiredOrders(long currentTime) {
        for (Map.Entry<K, KeyedEventState<I>> entry : stateByKey.entrySet()) {
            K key = entry.getKey();
            KeyedEventState<I> state = entry.getValue();
            evaluateAndFinalizeOrders(key, state, currentTime);
        }
    }

    /**
     * Evict old state to prevent memory growth
     */
    private void evictOldState(long currentTime) {
        long threshold = currentTime - stateRetentionTime;

        for (Iterator<Map.Entry<K, KeyedEventState<I>>> it = stateByKey.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K, KeyedEventState<I>> entry = it.next();
            KeyedEventState<I> state = entry.getValue();

            // Remove old events
            state.orders.removeIf(e -> e.timestamp < threshold);
            state.shipments.removeIf(e -> e.timestamp < threshold);
            state.payments.removeIf(e -> e.timestamp < threshold);

            // Remove state if completely empty
            if (state.isEmpty()) {
                it.remove();
            }
        }
    }

    // ==================== StreamToRelationOperator Interface ====================

    @Override
    public Report report() {
        return report;
    }

    @Override
    public Tick tick() {
        return tick;
    }

    @Override
    public Time time() {
        return time;
    }

    @Override
    public Content<I, W, R> content(long t_e) {
        // Return the most recently finalized content
        if (finalizedWindows.isEmpty()) {
            return cf.createEmpty();
        }
        // Return the latest content
        Content<I, W, R> latest = null;
        for (Content<I, W, R> content : finalizedWindows.values()) {
            latest = content;
        }
        return latest != null ? latest : cf.createEmpty();
    }

    @Override
    public List<Content<I, W, R>> getContents(long t_e) {
        return new ArrayList<>(finalizedWindows.values());
    }

    @Override
    public TimeVarying<R> get() {
        return new TimeVaryingObject<>(this, name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void evict() {
        // Clear all finalized windows that have been reported
        for (Window w : reportedWindows) {
            finalizedWindows.remove(w);
        }
        reportedWindows.clear();
    }

    @Override
    public void evict(long ts) {
        // Remove finalized windows older than ts
        finalizedWindows.entrySet().removeIf(entry -> entry.getKey().getC() < ts);
        evictOldState(ts);
    }

    // ==================== Accessors for Testing ====================

    /**
     * Get the current state for a key (for testing)
     */
    public KeyedEventState<I> getStateForKey(K key) {
        return stateByKey.get(key);
    }

    /**
     * Get all finalized windows (for testing)
     */
    public Map<Window, Content<I, W, R>> getFinalizedWindows() {
        return Collections.unmodifiableMap(finalizedWindows);
    }

    /**
     * Get the number of keys currently in state (for testing)
     */
    public int getStateKeyCount() {
        return stateByKey.size();
    }
}
