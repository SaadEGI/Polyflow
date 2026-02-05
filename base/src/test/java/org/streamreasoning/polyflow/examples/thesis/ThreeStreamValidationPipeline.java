package org.streamreasoning.polyflow.examples.thesis;

import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
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

import org.streamreasoning.polyflow.examples.thesis.events.*;
import org.streamreasoning.polyflow.examples.thesis.operators.*;
import org.streamreasoning.polyflow.examples.thesis.validation.ValidationStateManager;

import java.util.*;

/**
 * Three-Stream Validation-Dependent Join Pipeline
 * 
 * This pipeline demonstrates a validation-dependent interval join where:
 * - Stream A (Orders): The pivot/anchor stream
 * - Stream B (Shipments): The business join partner  
 * - Stream C (Payments): The validation stream
 * 
 * Key semantic:
 * An order from A should only participate in the content join (A ⋈ B) 
 * if it is validated by C (payment received within validation window).
 * 
 * This is the "static" baseline implementation that demonstrates INCOMPLETENESS
 * when validation (payment) arrives late.
 * 
 * Architecture:
 * <pre>
 * Stream A (Orders) ────┐
 *                       ├──► S2R: KeyedIntervalJoin ──► R2R: ValidationFilter ──► R2S ──► Output
 * Stream B (Shipments) ─┘                                       ▲
 *                                                               │
 * Stream C (Payments) ──────► ValidationStateManager ───────────┘
 * </pre>
 */
public class ThreeStreamValidationPipeline {
    
    // ============ Configuration ============
    
    /** 
     * Content join: shipment within [orderTime + lowerBound, orderTime + upperBound]
     * Default: [0, 1 hour] - shipment must arrive within 1 hour after order
     */
    private final long contentJoinLowerBound;
    private final long contentJoinUpperBound;
    
    /**
     * Validation window: payment must arrive within [orderTime, orderTime + validationWindow]
     * Default: 1 hour - payment must arrive within 1 hour after order
     */
    private final long validationWindow;
    
    /**
     * State retention: how long to keep state for late events
     * Default: 2 hours
     */
    private final long stateRetention;
    
    // ============ Components ============
    
    private final ContinuousProgramImpl<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>, ValidatedOrderShipment> program;
    private final DataStream<UnifiedEvent> orderStream;
    private final DataStream<UnifiedEvent> shipmentStream;
    private final DataStream<UnifiedEvent> paymentStream;
    private final DataStream<ValidatedOrderShipment> outputStream;
    private final ValidationStateManager validationManager;
    
    // Shared buffer for shipments (used by interval join operator)
    private final Map<String, TreeMap<Long, List<KeyedIntervalJoinS2ROperator.TimestampedElement<UnifiedEvent>>>> 
            shipmentBuffer = new HashMap<>();
    
    // Results tracking for evaluation
    private final List<ValidatedOrderShipment> outputResults = Collections.synchronizedList(new ArrayList<>());
    
    // ============ Constructors ============
    
    /**
     * Create pipeline with default configuration.
     * - Content join window: [0, 1 hour]
     * - Validation window: 1 hour
     * - State retention: 2 hours
     */
    public ThreeStreamValidationPipeline() {
        this(0, 3600000, 3600000, 7200000);
    }
    
    /**
     * Create pipeline with custom configuration.
     * 
     * @param contentJoinLowerBound Lower bound for content join (relative to order time)
     * @param contentJoinUpperBound Upper bound for content join (relative to order time)
     * @param validationWindow How long after order that payment can arrive
     * @param stateRetention How long to retain state for late events
     */
    public ThreeStreamValidationPipeline(long contentJoinLowerBound, 
                                          long contentJoinUpperBound,
                                          long validationWindow,
                                          long stateRetention) {
        this.contentJoinLowerBound = contentJoinLowerBound;
        this.contentJoinUpperBound = contentJoinUpperBound;
        this.validationWindow = validationWindow;
        this.stateRetention = stateRetention;
        
        // Initialize validation manager
        this.validationManager = new ValidationStateManager(validationWindow);
        
        // Create streams
        this.orderStream = new defaultDataStream<>("orders");
        this.shipmentStream = new defaultDataStream<>("shipments");
        this.paymentStream = new defaultDataStream<>("payments");
        this.outputStream = new defaultDataStream<>("output");
        
        // Track outputs
        this.outputStream.addConsumer((stream, element, ts) -> outputResults.add(element));
        
        // Build the program
        this.program = buildProgram();
    }
    
    // ============ Pipeline Construction ============
    
    private ContinuousProgramImpl<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>, ValidatedOrderShipment> buildProgram() {
        
        // Shared time instance
        Time time = new TimeImpl(0);
        
        // Report strategy: emit on content change
        Report report = new ReportImpl();
        report.add(new OnContentChange());
        
        // Content factory for accumulating events into lists
        AccumulatorContentFactory<UnifiedEvent, UnifiedEvent, List<UnifiedEvent>> contentFactory =
                new AccumulatorContentFactory<>(
                        e -> e,                                    // I -> W (identity)
                        e -> Collections.singletonList(e),         // W -> R (single element list)
                        (l1, l2) -> {                              // Merge function
                            List<UnifiedEvent> merged = new ArrayList<>(l1);
                            merged.addAll(l2);
                            return merged;
                        },
                        new ArrayList<>()                          // Empty content
                );
        
        // S2R Operator: Keyed Interval Join (Orders ⋈ Shipments)
        // This creates windows around each order and matches with shipments
        KeyedIntervalJoinS2ROperator<UnifiedEvent, String, UnifiedEvent, List<UnifiedEvent>> 
                orderShipmentJoin = new KeyedIntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "orderShipmentJoin",
                        contentFactory,
                        report,
                        contentJoinLowerBound,
                        contentJoinUpperBound,
                        shipmentBuffer,
                        e -> e.joinKey,          // Key extractor (orderId)
                        e -> e.timestamp,        // Timestamp extractor
                        stateRetention
                );
        
        // R2R Operator: Filter by validation state
        // Only keeps order-shipment pairs where order is validated (paid)
        RelationToRelationOperator<List<UnifiedEvent>> validationFilter = 
                new ValidationFilterR2R(
                        validationManager,
                        Collections.singletonList("orderShipmentJoin"),
                        "validatedJoin"
                );
        
        // R2S Operator: Convert to output stream elements
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
    
    /**
     * Inject an order event.
     * This is the pivot event that triggers window creation and join computation.
     */
    public void injectOrder(Order order) {
        UnifiedEvent event = UnifiedEvent.fromOrder(order);
        orderStream.put(event, order.eventTime);
    }
    
    /**
     * Inject a shipment event.
     * Shipments are added to the shared buffer for interval join matching.
     */
    public void injectShipment(Shipment shipment) {
        UnifiedEvent event = UnifiedEvent.fromShipment(shipment);
        // Add to shared buffer for interval join
        KeyedIntervalJoinS2ROperator.addToBuildBuffer(
                shipmentBuffer, event, event.joinKey, event.timestamp);
        shipmentStream.put(event, shipment.eventTime);
    }
    
    /**
     * Inject a payment event.
     * Payments update the validation state.
     */
    public void injectPayment(Payment payment) {
        // Update validation state
        validationManager.recordPayment(payment);
        UnifiedEvent event = UnifiedEvent.fromPayment(payment);
        paymentStream.put(event, payment.eventTime);
    }
    
    // ============ Results Access ============
    
    /**
     * Get all output results produced so far.
     */
    public List<ValidatedOrderShipment> getOutputResults() {
        return new ArrayList<>(outputResults);
    }
    
    /**
     * Clear output results (for testing).
     */
    public void clearResults() {
        outputResults.clear();
    }
    
    /**
     * Get the validation manager (for inspection/testing).
     */
    public ValidationStateManager getValidationManager() {
        return validationManager;
    }
    
    // ============ Configuration Getters ============
    
    public long getContentJoinLowerBound() {
        return contentJoinLowerBound;
    }
    
    public long getContentJoinUpperBound() {
        return contentJoinUpperBound;
    }
    
    public long getValidationWindow() {
        return validationWindow;
    }
    
    public long getStateRetention() {
        return stateRetention;
    }
}
