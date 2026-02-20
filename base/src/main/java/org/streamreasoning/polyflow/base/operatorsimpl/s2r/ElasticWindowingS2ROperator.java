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
 * Elastic Windowing S2R Operator - Triangle Moving Windows
 * 
 * <h2>Key Concept: Chained Two-Stream Joins with Elastic Boundaries</h2>
 * 
 * <p>Instead of fixed windows anchored on order time, this operator implements
 * "elastic windowing" where window boundaries can extend based on event arrivals:</p>
 * 
 * <h3>Chain of Two-Stream Joins:</h3>
 * <ol>
 *   <li><b>Stage 1: Order ↔ Payment</b> (elastic validation window)
 *       <ul>
 *         <li>Base window: [tO, tO + V]</li>
 *         <li>Can extend to: [tO, tO + V + G] (grace period)</li>
 *         <li>Window "moves" as time progresses</li>
 *       </ul>
 *   </li>
 *   <li><b>Stage 2: Payment ↔ Shipment</b> (content window anchored on PAYMENT)
 *       <ul>
 *         <li>Window starts from validated payment timestamp: [tP, tP + U]</li>
 *         <li>This creates the "triangle moving" effect</li>
 *       </ul>
 *   </li>
 * </ol>
 * 
 * <h3>Triangle Moving Visualization:</h3>
 * <pre>
 *     Fixed Window (baseline):
 *     Order────────[────────────U────────────]──────▶
 *          tO                              tO+U
 *     
 *     Elastic Window (triangle moving):
 *     Order────────[──V──]──────────────────────────▶
 *                       ↓ payment arrives at tP
 *                  Payment────[────U────]───────────▶
 *                        tP         tP+U
 * </pre>
 * 
 * <h3>Benefits:</h3>
 * <ul>
 *   <li>Late payments can still "rescue" matches</li>
 *   <li>Shipment window moves with payment (more flexible)</li>
 *   <li>Feels like composable two-stream joins</li>
 *   <li>Easier to generalize to N-stream chains</li>
 * </ul>
 *
 * @param <I> Input event type
 * @param <K> Key type for joining
 * @param <W> Internal working type
 * @param <R> Result type (Iterable)
 */
public class ElasticWindowingS2ROperator<I, K, W, R extends Iterable<?>>
        implements StreamToRelationOperator<I, W, R> {

    private static final Logger log = Logger.getLogger(ElasticWindowingS2ROperator.class);

    /**
     * Window mode for comparison
     */
    public enum WindowMode {
        /** Fixed window: shipment must be in [tO, tO+U] */
        FIXED,
        /** Elastic window: shipment must be in [tP, tP+U] where tP is payment time */
        ELASTIC,
        /** Extended fixed: shipment in [tO, tO+U+extension] */
        EXTENDED_FIXED
    }

    // Event categorization
    public enum EventCategory { ORDER, SHIPMENT, PAYMENT }

    // Core Polyflow components
    private final Tick tick;
    private final Time time;
    private final String name;
    private final ContentFactory<I, W, R> cf;
    private final Report report;

    // Window parameters
    private final long validationWindow;       // V: base validation window for payment
    private final long gracePeriod;            // G: grace period extension
    private final long contentWindow;          // U: content window for shipment
    private final long windowExtension;        // Extension for EXTENDED_FIXED mode

    // Window mode
    private final WindowMode windowMode;

    // Key extractors
    private final Function<I, K> keyExtractor;
    private final Function<I, Long> timestampExtractor;
    private final Function<I, EventCategory> categoryExtractor;

    // Result factory
    private final ChainedResultFactory<I> resultFactory;

    // State per key
    private final Map<K, KeyedChainState<I>> stateByKey = new HashMap<>();

    // Finalized results
    private final Map<Window, Content<I, W, R>> finalizedWindows = new LinkedHashMap<>();
    private final List<Window> reportedWindows = new ArrayList<>();

    /**
     * State for chained join per key
     */
    public static class KeyedChainState<I> {
        public final List<TimestampedEvent<I>> orders = new ArrayList<>();
        public final List<TimestampedEvent<I>> payments = new ArrayList<>();
        public final List<TimestampedEvent<I>> shipments = new ArrayList<>();
        
        // Track validated (Order, Payment) pairs
        public final Map<Long, ValidatedPair<I>> validatedPairs = new HashMap<>();
        
        // Track finalized orders
        public final Set<Long> finalizedOrders = new HashSet<>();
        
        public long lastUpdateTime = 0;
    }

    /**
     * Represents a validated Order-Payment pair
     */
    public static class ValidatedPair<I> {
        public final I order;
        public final long orderTime;
        public final I payment;
        public final long paymentTime;
        public final long shipmentWindowEnd; // tP + U (elastic) or tO + U (fixed)

        public ValidatedPair(I order, long orderTime, I payment, long paymentTime, long shipmentWindowEnd) {
            this.order = order;
            this.orderTime = orderTime;
            this.payment = payment;
            this.paymentTime = paymentTime;
            this.shipmentWindowEnd = shipmentWindowEnd;
        }

        @Override
        public String toString() {
            return String.format("ValidatedPair{orderT=%d, paymentT=%d, shipEnd=%d}", 
                    orderTime, paymentTime, shipmentWindowEnd);
        }
    }

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
     * Factory for creating chained join results
     */
    @FunctionalInterface
    public interface ChainedResultFactory<I> {
        I createResult(I order, long orderTime, I payment, long paymentTime, 
                       I shipment, long shipmentTime, long outputTime);
    }

    /**
     * Full constructor
     */
    public ElasticWindowingS2ROperator(
            Tick tick,
            Time time,
            String name,
            ContentFactory<I, W, R> cf,
            Report report,
            long validationWindow,
            long gracePeriod,
            long contentWindow,
            long windowExtension,
            WindowMode windowMode,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            Function<I, EventCategory> categoryExtractor,
            ChainedResultFactory<I> resultFactory) {

        this.tick = tick;
        this.time = time;
        this.name = name;
        this.cf = cf;
        this.report = report;
        this.validationWindow = validationWindow;
        this.gracePeriod = gracePeriod;
        this.contentWindow = contentWindow;
        this.windowExtension = windowExtension;
        this.windowMode = windowMode;
        this.keyExtractor = keyExtractor;
        this.timestampExtractor = timestampExtractor;
        this.categoryExtractor = categoryExtractor;
        this.resultFactory = resultFactory;

        log.info(String.format("ElasticWindowingS2R created: mode=%s, V=%d, G=%d, U=%d, ext=%d",
                windowMode, validationWindow, gracePeriod, contentWindow, windowExtension));
    }

    /**
     * Simplified constructor defaulting to ELASTIC mode
     */
    public ElasticWindowingS2ROperator(
            Tick tick, Time time, String name,
            ContentFactory<I, W, R> cf, Report report,
            long validationWindow, long gracePeriod, long contentWindow,
            Function<I, K> keyExtractor,
            Function<I, Long> timestampExtractor,
            Function<I, EventCategory> categoryExtractor,
            ChainedResultFactory<I> resultFactory) {
        this(tick, time, name, cf, report, validationWindow, gracePeriod, contentWindow, 0,
                WindowMode.ELASTIC, keyExtractor, timestampExtractor, categoryExtractor, resultFactory);
    }

    @Override
    public void compute(I element, long ts) {
        K key = keyExtractor.apply(element);
        EventCategory category = categoryExtractor.apply(element);

        log.debug(String.format("[%s] Received %s for key=%s at ts=%d", windowMode, category, key, ts));

        KeyedChainState<I> state = stateByKey.computeIfAbsent(key, k -> new KeyedChainState<>());
        TimestampedEvent<I> tsEvent = new TimestampedEvent<>(element, ts);

        // Add to appropriate bucket
        switch (category) {
            case ORDER:
                state.orders.add(tsEvent);
                break;
            case PAYMENT:
                state.payments.add(tsEvent);
                // Payment arrival triggers Stage 1 evaluation
                evaluateStage1(key, state, ts);
                break;
            case SHIPMENT:
                state.shipments.add(tsEvent);
                // Shipment arrival triggers Stage 2 evaluation
                evaluateStage2(key, state, ts);
                break;
        }
        state.lastUpdateTime = ts;

        // Check for expired orders
        checkExpiredOrders(key, state, ts);

        time.setAppTime(ts);
        evictOldState(ts);
    }

    /**
     * Stage 1: Order ↔ Payment (elastic validation)
     * Creates ValidatedPair when payment arrives within [tO, tO + V + G]
     */
    private void evaluateStage1(K key, KeyedChainState<I> state, long currentTime) {
        for (TimestampedEvent<I> paymentEvent : state.payments) {
            long paymentTime = paymentEvent.timestamp;

            for (TimestampedEvent<I> orderEvent : state.orders) {
                long orderTime = orderEvent.timestamp;

                // Skip already finalized or already validated
                if (state.finalizedOrders.contains(orderTime) || 
                    state.validatedPairs.containsKey(orderTime)) {
                    continue;
                }

                // Check if payment is within validation window [tO, tO + V + G]
                long validationDeadline = orderTime + validationWindow + gracePeriod;
                if (paymentTime >= orderTime && paymentTime <= validationDeadline) {
                    // Create validated pair with appropriate shipment window end
                    long shipmentWindowEnd = calculateShipmentWindowEnd(orderTime, paymentTime);
                    
                    ValidatedPair<I> pair = new ValidatedPair<>(
                            orderEvent.event, orderTime,
                            paymentEvent.event, paymentTime,
                            shipmentWindowEnd);
                    
                    state.validatedPairs.put(orderTime, pair);
                    log.debug(String.format("[%s] Stage1 validated: order@%d, payment@%d, shipEnd=%d",
                            windowMode, orderTime, paymentTime, shipmentWindowEnd));

                    // Immediately check Stage 2 for this newly validated pair
                    evaluateStage2ForPair(key, state, pair, currentTime);
                }
            }
        }
    }

    /**
     * Calculate shipment window end based on mode
     */
    private long calculateShipmentWindowEnd(long orderTime, long paymentTime) {
        switch (windowMode) {
            case FIXED:
                // Shipment window anchored on order: [tO, tO + U]
                return orderTime + contentWindow;
            case ELASTIC:
                // Shipment window anchored on payment: [tP, tP + U]
                // This is the "triangle moving" effect!
                return paymentTime + contentWindow;
            case EXTENDED_FIXED:
                // Extended fixed window: [tO, tO + U + extension]
                return orderTime + contentWindow + windowExtension;
            default:
                return orderTime + contentWindow;
        }
    }

    /**
     * Stage 2: Payment ↔ Shipment (content join)
     * Matches shipments against validated pairs
     */
    private void evaluateStage2(K key, KeyedChainState<I> state, long currentTime) {
        for (ValidatedPair<I> pair : state.validatedPairs.values()) {
            if (state.finalizedOrders.contains(pair.orderTime)) {
                continue;
            }
            evaluateStage2ForPair(key, state, pair, currentTime);
        }
    }

    /**
     * Evaluate Stage 2 for a specific validated pair
     */
    private void evaluateStage2ForPair(K key, KeyedChainState<I> state, 
                                        ValidatedPair<I> pair, long currentTime) {
        long shipmentWindowStart = getShipmentWindowStart(pair);
        long shipmentWindowEnd = pair.shipmentWindowEnd;

        List<TimestampedEvent<I>> matchingShipments = new ArrayList<>();
        for (TimestampedEvent<I> shipmentEvent : state.shipments) {
            long shipmentTime = shipmentEvent.timestamp;
            if (shipmentTime >= shipmentWindowStart && shipmentTime <= shipmentWindowEnd) {
                matchingShipments.add(shipmentEvent);
            }
        }

        if (!matchingShipments.isEmpty()) {
            // Finalize with results
            finalizeWithResults(key, pair, matchingShipments, currentTime);
            state.finalizedOrders.add(pair.orderTime);
        }
    }

    /**
     * Get shipment window start based on mode
     */
    private long getShipmentWindowStart(ValidatedPair<I> pair) {
        switch (windowMode) {
            case ELASTIC:
                // Elastic: shipment window starts from payment
                return pair.paymentTime;
            case FIXED:
            case EXTENDED_FIXED:
            default:
                // Fixed: shipment window starts from order
                return pair.orderTime;
        }
    }

    /**
     * Check for orders that have expired without being finalized
     */
    private void checkExpiredOrders(K key, KeyedChainState<I> state, long currentTime) {
        for (TimestampedEvent<I> orderEvent : state.orders) {
            long orderTime = orderEvent.timestamp;
            if (state.finalizedOrders.contains(orderTime)) {
                continue;
            }

            long validationDeadline = orderTime + validationWindow + gracePeriod;
            ValidatedPair<I> pair = state.validatedPairs.get(orderTime);

            if (pair == null && currentTime > validationDeadline) {
                // No payment within validation window → INVALID
                state.finalizedOrders.add(orderTime);
                log.debug(String.format("[%s] Order@%d finalized as INVALID (no payment by %d)",
                        windowMode, orderTime, validationDeadline));
            } else if (pair != null && currentTime > pair.shipmentWindowEnd) {
                // Payment exists but no shipment within window → NO_CONTENT
                state.finalizedOrders.add(orderTime);
                log.debug(String.format("[%s] Order@%d finalized as NO_CONTENT (no shipment by %d)",
                        windowMode, orderTime, pair.shipmentWindowEnd));
            }
        }
    }

    /**
     * Finalize order with results
     */
    private void finalizeWithResults(K key, ValidatedPair<I> pair,
                                      List<TimestampedEvent<I>> shipments, long currentTime) {
        Window window = new WindowImpl(pair.orderTime, currentTime);
        Content<I, W, R> content = cf.create();

        for (TimestampedEvent<I> shipment : shipments) {
            I result = resultFactory.createResult(
                    pair.order, pair.orderTime,
                    pair.payment, pair.paymentTime,
                    shipment.event, shipment.timestamp,
                    currentTime);
            content.add(result);
        }

        finalizedWindows.put(window, content);

        if (report.report(window, content, currentTime, System.currentTimeMillis())) {
            reportedWindows.add(window);
            time.addEvaluationTimeInstants(new TimeInstant(currentTime));
        }

        log.debug(String.format("[%s] Finalized VALID: order@%d, payment@%d, %d shipments",
                windowMode, pair.orderTime, pair.paymentTime, shipments.size()));
    }

    /**
     * Evict old state
     */
    private void evictOldState(long currentTime) {
        long maxWindow = Math.max(contentWindow, validationWindow + gracePeriod);
        if (windowMode == WindowMode.EXTENDED_FIXED) {
            maxWindow = Math.max(maxWindow, contentWindow + windowExtension);
        }
        long threshold = currentTime - maxWindow * 2;

        for (Iterator<Map.Entry<K, KeyedChainState<I>>> it = stateByKey.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K, KeyedChainState<I>> entry = it.next();
            KeyedChainState<I> state = entry.getValue();

            state.orders.removeIf(e -> e.timestamp < threshold);
            state.payments.removeIf(e -> e.timestamp < threshold);
            state.shipments.removeIf(e -> e.timestamp < threshold);
            state.validatedPairs.entrySet().removeIf(e -> e.getValue().orderTime < threshold);

            if (state.orders.isEmpty() && state.payments.isEmpty() && state.shipments.isEmpty()) {
                it.remove();
            }
        }
    }

    // ==================== StreamToRelationOperator Interface ====================

    @Override
    public Report report() { return report; }

    @Override
    public Tick tick() { return tick; }

    @Override
    public Time time() { return time; }

    @Override
    public Content<I, W, R> content(long t_e) {
        if (finalizedWindows.isEmpty()) return cf.createEmpty();
        Content<I, W, R> latest = null;
        for (Content<I, W, R> c : finalizedWindows.values()) latest = c;
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
    public String getName() { return name; }

    @Override
    public void evict() {
        for (Window w : reportedWindows) finalizedWindows.remove(w);
        reportedWindows.clear();
    }

    @Override
    public void evict(long ts) {
        finalizedWindows.entrySet().removeIf(e -> e.getKey().getC() < ts);
        evictOldState(ts);
    }

    // ==================== Testing Accessors ====================

    public Map<Window, Content<I, W, R>> getFinalizedWindows() {
        return Collections.unmodifiableMap(finalizedWindows);
    }

    public KeyedChainState<I> getStateForKey(K key) {
        return stateByKey.get(key);
    }

    public WindowMode getWindowMode() {
        return windowMode;
    }
}
