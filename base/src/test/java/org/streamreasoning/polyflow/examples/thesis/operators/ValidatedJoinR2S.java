package org.streamreasoning.polyflow.examples.thesis.operators;

import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.examples.thesis.events.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Relation-to-Stream Operator that converts validated join results to output stream elements.
 * 
 * Input: List of UnifiedEvents (validated orders + their matching shipments)
 * Output: Stream of ValidatedOrderShipment objects
 * 
 * This operator pairs each order with its matching shipments to produce
 * the final output tuples.
 */
public class ValidatedJoinR2S implements RelationToStreamOperator<List<UnifiedEvent>, ValidatedOrderShipment> {
    
    @Override
    public Stream<ValidatedOrderShipment> eval(List<UnifiedEvent> relation, long ts) {
        if (relation == null || relation.isEmpty()) {
            return Stream.empty();
        }
        
        // Group events by orderId
        Map<String, List<UnifiedEvent>> byOrderId = relation.stream()
                .collect(Collectors.groupingBy(e -> e.joinKey));
        
        List<ValidatedOrderShipment> results = new ArrayList<>();
        
        for (Map.Entry<String, List<UnifiedEvent>> entry : byOrderId.entrySet()) {
            List<UnifiedEvent> events = entry.getValue();
            
            // Find the order event
            UnifiedEvent orderEvent = events.stream()
                    .filter(UnifiedEvent::isOrder)
                    .findFirst()
                    .orElse(null);
            
            if (orderEvent == null) {
                // No order in this group, skip
                continue;
            }
            
            Order order = orderEvent.asOrder();
            
            // Create output for each shipment matched with this order
            for (UnifiedEvent e : events) {
                if (e.isShipment()) {
                    Shipment shipment = e.asShipment();
                    results.add(new ValidatedOrderShipment(order, shipment, ts));
                }
            }
        }
        
        return results.stream();
    }
}
