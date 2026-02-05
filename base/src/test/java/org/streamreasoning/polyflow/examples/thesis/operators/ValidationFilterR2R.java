package org.streamreasoning.polyflow.examples.thesis.operators;

import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.examples.thesis.events.UnifiedEvent;
import org.streamreasoning.polyflow.examples.thesis.validation.ValidationStateManager;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Relation-to-Relation Operator that filters join results based on validation state.
 * 
 * This operator sits after the interval join (Order ⋈ Shipment) and filters out
 * any order-shipment pairs where the order has NOT been validated (paid).
 * 
 * Input: List of UnifiedEvents (from interval join: orders + shipments mixed together)
 * Output: Filtered list containing only events for orders that are validated
 * 
 * The "symmetry" insight:
 * - Content window: Order ⋈ Shipment within time interval
 * - Validation window: Payment must arrive within validation window
 * - Both are anchored on the order timestamp
 * - If validation evidence arrives late → can't confirm order is valid → can't output join
 */
public class ValidationFilterR2R implements RelationToRelationOperator<List<UnifiedEvent>> {
    
    private final ValidationStateManager validationManager;
    private final List<String> operandNames;
    private final String resultName;
    
    /**
     * Create a validation filter operator.
     * 
     * @param validationManager The validation state manager that tracks paid orders
     * @param operandNames Names of the input operands (typically the join result)
     * @param resultName Name of this operator's result
     */
    public ValidationFilterR2R(ValidationStateManager validationManager,
                               List<String> operandNames,
                               String resultName) {
        this.validationManager = validationManager;
        this.operandNames = operandNames;
        this.resultName = resultName;
    }
    
    @Override
    public List<UnifiedEvent> eval(List<List<UnifiedEvent>> operands) {
        if (operands == null || operands.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Get the join result (first operand)
        List<UnifiedEvent> joinResult = operands.get(0);
        if (joinResult == null || joinResult.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Separate orders and shipments
        List<UnifiedEvent> orders = joinResult.stream()
                .filter(UnifiedEvent::isOrder)
                .collect(Collectors.toList());
        
        // Find which orders are validated
        // Key insight: we check validation using the ORDER's timestamp
        Set<String> validatedOrderIds = orders.stream()
                .filter(e -> {
                    // Strict validation: payment must be within validation window
                    return validationManager.isValidated(e.joinKey, e.timestamp);
                })
                .map(e -> e.joinKey)
                .collect(Collectors.toSet());
        
        // Return only events (orders + shipments) for validated orders
        List<UnifiedEvent> filtered = joinResult.stream()
                .filter(e -> validatedOrderIds.contains(e.joinKey))
                .collect(Collectors.toList());
        
        return filtered;
    }
    
    @Override
    public List<String> getTvgNames() {
        return operandNames;
    }
    
    @Override
    public String getResName() {
        return resultName;
    }
}
