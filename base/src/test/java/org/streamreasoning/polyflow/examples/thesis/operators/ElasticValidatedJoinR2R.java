package org.streamreasoning.polyflow.examples.thesis.operators;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.examples.thesis.events.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Elastic Validated Join R2R Operator
 * 
 * <p>This R2R operator computes validated join results from a relation containing
 * all three event types (Orders, Shipments, Payments). It applies elastic validation
 * rules to produce finalized results.</p>
 * 
 * <h3>Elastic Validation Rules:</h3>
 * <ul>
 *   <li>For each order at time tO:</li>
 *   <li>Check if payment exists within [tO, tO+V+G] (validation + grace)</li>
 *   <li>If yes and shipments within [tO, tO+U] → emit ValidatedOrderShipment</li>
 *   <li>If no and time ≤ tO+V+G → keep pending (return empty)</li>
 *   <li>If no and time > tO+V+G → finalize as invalid (return empty)</li>
 * </ul>
 * 
 * <h3>Key Insight:</h3>
 * <p>Payments naturally trigger re-evaluation because they change the input relation.
 * No manual callbacks are needed.</p>
 */
public class ElasticValidatedJoinR2R implements RelationToRelationOperator<List<UnifiedEvent>> {

    private final long contentWindowSize;      // U: Max time for shipment after order
    private final long validationWindowSize;   // V: Validation window for payment
    private final long gracePeriod;            // G: Additional grace period

    private final List<String> operandNames;
    private final String resultName;

    // Track which orders have been finalized to avoid re-processing
    private final Set<String> finalizedOrders = new HashSet<>();

    // Current evaluation time (set externally or derived from latest event)
    private long currentEvaluationTime = 0;

    /**
     * Create an elastic validated join operator.
     *
     * @param contentWindowSize    U: Max time for shipment after order
     * @param validationWindowSize V: Validation window for payment
     * @param gracePeriod          G: Additional grace period for late arrivals
     * @param operandNames         Names of input TVGs
     * @param resultName           Name of this operator's result
     */
    public ElasticValidatedJoinR2R(
            long contentWindowSize,
            long validationWindowSize,
            long gracePeriod,
            List<String> operandNames,
            String resultName) {
        this.contentWindowSize = contentWindowSize;
        this.validationWindowSize = validationWindowSize;
        this.gracePeriod = gracePeriod;
        this.operandNames = operandNames;
        this.resultName = resultName;
    }

    /**
     * Set the current evaluation time.
     * This should be called before eval() to set the "current time" for finalization checks.
     */
    public void setCurrentTime(long time) {
        this.currentEvaluationTime = time;
    }

    @Override
    public List<UnifiedEvent> eval(List<List<UnifiedEvent>> operands) {
        if (operands == null || operands.isEmpty() || operands.get(0) == null) {
            return Collections.emptyList();
        }

        List<UnifiedEvent> allEvents = operands.get(0);
        if (allEvents.isEmpty()) {
            return Collections.emptyList();
        }

        // Derive current time from latest event if not set externally
        long evalTime = currentEvaluationTime > 0 ? currentEvaluationTime :
                allEvents.stream().mapToLong(e -> e.timestamp).max().orElse(0);

        // Group events by key (orderId)
        Map<String, List<UnifiedEvent>> eventsByKey = allEvents.stream()
                .collect(Collectors.groupingBy(e -> e.joinKey));

        List<UnifiedEvent> results = new ArrayList<>();

        for (Map.Entry<String, List<UnifiedEvent>> entry : eventsByKey.entrySet()) {
            String key = entry.getKey();
            List<UnifiedEvent> keyEvents = entry.getValue();

            // Separate by type
            List<UnifiedEvent> orders = keyEvents.stream()
                    .filter(UnifiedEvent::isOrder).collect(Collectors.toList());
            List<UnifiedEvent> shipments = keyEvents.stream()
                    .filter(UnifiedEvent::isShipment).collect(Collectors.toList());
            List<UnifiedEvent> payments = keyEvents.stream()
                    .filter(UnifiedEvent::isPayment).collect(Collectors.toList());

            // Process each order
            for (UnifiedEvent orderEvent : orders) {
                String orderKey = orderEvent.joinKey + "@" + orderEvent.timestamp;
                
                // Skip already finalized orders
                if (finalizedOrders.contains(orderKey)) {
                    continue;
                }

                EvaluationResult result = evaluateOrder(orderEvent, shipments, payments, evalTime);

                switch (result.status) {
                    case VALID:
                        results.addAll(result.validatedResults);
                        finalizedOrders.add(orderKey);
                        break;
                    case INVALID:
                    case NO_CONTENT:
                        finalizedOrders.add(orderKey);
                        break;
                    case PENDING:
                        // Keep waiting, don't finalize
                        break;
                }
            }
        }

        return results;
    }

    /**
     * Evaluate a single order against the elastic validation rules
     */
    private EvaluationResult evaluateOrder(
            UnifiedEvent orderEvent,
            List<UnifiedEvent> shipments,
            List<UnifiedEvent> payments,
            long currentTime) {

        long orderTime = orderEvent.timestamp;
        long validationDeadline = orderTime + validationWindowSize + gracePeriod;
        long contentDeadline = orderTime + contentWindowSize;

        // Step 1: Find valid payment
        UnifiedEvent validPayment = findValidPayment(payments, orderTime, validationDeadline);

        // Step 2: Find valid shipments
        List<UnifiedEvent> validShipments = findValidShipments(shipments, orderTime, contentDeadline);

        // Step 3: Apply elastic rules
        if (validPayment != null) {
            if (!validShipments.isEmpty()) {
                // Payment + Shipments → VALID
                List<UnifiedEvent> results = createValidatedResults(orderEvent, validShipments, validPayment, currentTime);
                return EvaluationResult.valid(results);
            } else if (currentTime > contentDeadline) {
                // Payment exists but content window expired
                return EvaluationResult.noContent();
            } else {
                // Payment exists, waiting for shipments
                return EvaluationResult.pending();
            }
        } else {
            if (currentTime > validationDeadline) {
                // Validation window expired
                return EvaluationResult.invalid();
            } else {
                // Still waiting for payment
                return EvaluationResult.pending();
            }
        }
    }

    private UnifiedEvent findValidPayment(List<UnifiedEvent> payments, long orderTime, long deadline) {
        for (UnifiedEvent payment : payments) {
            if (payment.timestamp >= orderTime && payment.timestamp <= deadline) {
                Payment p = payment.asPayment();
                if (p != null && p.isPaid()) {
                    return payment;
                }
            }
        }
        return null;
    }

    private List<UnifiedEvent> findValidShipments(List<UnifiedEvent> shipments, long orderTime, long deadline) {
        return shipments.stream()
                .filter(s -> s.timestamp >= orderTime && s.timestamp <= deadline)
                .collect(Collectors.toList());
    }

    private List<UnifiedEvent> createValidatedResults(
            UnifiedEvent orderEvent,
            List<UnifiedEvent> shipments,
            UnifiedEvent paymentEvent,
            long outputTime) {

        Order order = orderEvent.asOrder();
        List<UnifiedEvent> results = new ArrayList<>();

        for (UnifiedEvent shipmentEvent : shipments) {
            Shipment shipment = shipmentEvent.asShipment();
            if (order != null && shipment != null) {
                ValidatedOrderShipment validated = new ValidatedOrderShipment(order, shipment, outputTime);
                // Wrap as UnifiedEvent for downstream compatibility
                results.add(UnifiedEvent.fromValidated(validated));
            }
        }

        return results;
    }

    /**
     * Clear finalization state (for testing or reset)
     */
    public void clearState() {
        finalizedOrders.clear();
        currentEvaluationTime = 0;
    }

    @Override
    public List<String> getTvgNames() {
        return operandNames;
    }

    @Override
    public String getResName() {
        return resultName;
    }

    // ==================== Helper Classes ====================

    private enum FinalizationStatus {
        PENDING, VALID, INVALID, NO_CONTENT
    }

    private static class EvaluationResult {
        final FinalizationStatus status;
        final List<UnifiedEvent> validatedResults;

        EvaluationResult(FinalizationStatus status, List<UnifiedEvent> results) {
            this.status = status;
            this.validatedResults = results != null ? results : Collections.emptyList();
        }

        static EvaluationResult pending() {
            return new EvaluationResult(FinalizationStatus.PENDING, null);
        }

        static EvaluationResult invalid() {
            return new EvaluationResult(FinalizationStatus.INVALID, null);
        }

        static EvaluationResult noContent() {
            return new EvaluationResult(FinalizationStatus.NO_CONTENT, null);
        }

        static EvaluationResult valid(List<UnifiedEvent> results) {
            return new EvaluationResult(FinalizationStatus.VALID, results);
        }
    }
}
