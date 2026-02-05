package org.streamreasoning.polyflow.examples.thesis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamreasoning.polyflow.examples.thesis.events.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for the Three-Stream Validation-Dependent Join Pipeline.
 * 
 * These tests demonstrate the four scenarios (S1-S4) from the thesis specification:
 * - S1 (Normal): Payment arrives quickly, shipment within join window → JOIN OUTPUT
 * - S2 (Late Validation): Payment arrives after validation window → MISSED (incompleteness!)
 * - S3 (Late Shipment): Shipment after join window, payment ok → NO OUTPUT (correct)
 * - S4 (Missing Payment): No payment ever → NO OUTPUT (correct)
 * 
 * The key insight is that S1, S3, S4 show correct (sound) behavior,
 * while S2 demonstrates the INCOMPLETENESS problem that motivates
 * dynamic adaptation techniques.
 */
public class ThreeStreamValidationPipelineTest {
    
    // Configuration for testing (smaller time windows)
    private static final long CONTENT_LOWER = 0;        // Shipment after order
    private static final long CONTENT_UPPER = 1000;     // Within 1 second
    private static final long VALIDATION_WINDOW = 500;  // Payment within 500ms
    private static final long STATE_RETENTION = 2000;   // 2 seconds
    
    private ThreeStreamValidationPipeline pipeline;
    
    @BeforeEach
    void setUp() {
        pipeline = new ThreeStreamValidationPipeline(
                CONTENT_LOWER, CONTENT_UPPER, VALIDATION_WINDOW, STATE_RETENTION);
    }
    
    /**
     * Case S1: Normal - payment arrives quickly
     * 
     * Timeline:
     * - Order at t=1000
     * - Payment at t=1100 (within validation window [1000, 1500])
     * - Shipment at t=1200 (within join window [1000, 2000])
     * 
     * Expected: JOIN OUTPUT PRODUCED
     */
    @Test
    void testCaseS1_Normal_PaymentArrivesQuickly() {
        System.out.println("=== Case S1: Normal - Payment arrives quickly ===");
        
        long baseTime = 1000;
        
        // Create events
        Order order = new Order("ORD-S1", "CUST-1", 100.0, baseTime);
        Payment payment = new Payment("PAY-S1", "ORD-S1", "PAID", baseTime + 100);
        Shipment shipment = new Shipment("SHP-S1", "ORD-S1", "FedEx", baseTime + 200);
        
        // Inject in order: payment first, then shipment, then order (trigger)
        // This ensures validation state is ready when order arrives
        pipeline.injectPayment(payment);
        pipeline.injectShipment(shipment);
        pipeline.injectOrder(order);
        
        // Verify output
        List<ValidatedOrderShipment> results = pipeline.getOutputResults();
        
        System.out.println("Order: " + order);
        System.out.println("Payment: " + payment);
        System.out.println("Shipment: " + shipment);
        System.out.println("Results: " + results);
        
        assertEquals(1, results.size(), "Should produce exactly one join result");
        assertEquals("ORD-S1", results.get(0).order.orderId);
        assertEquals("SHP-S1", results.get(0).shipment.shipmentId);
        
        System.out.println("✓ S1 passed: Join output produced as expected\n");
    }
    
    /**
     * Case S2: Late Validation - payment arrives after validation window
     * 
     * Timeline:
     * - Order at t=1000
     * - Shipment at t=1200 (within join window [1000, 2000])
     * - Payment at t=2000 (LATE! outside validation window [1000, 1500])
     * 
     * Expected with static pipeline: JOIN OUTPUT MISSING (demonstrates incompleteness!)
     * Expected with perfect system: JOIN OUTPUT PRODUCED
     */
    @Test
    void testCaseS2_LateValidation_PaymentArrivesLate() {
        System.out.println("=== Case S2: Late Validation - Payment arrives late ===");
        
        long baseTime = 1000;
        
        // Create events - payment is LATE (arrives after validation window)
        Order order = new Order("ORD-S2", "CUST-2", 200.0, baseTime);
        Shipment shipment = new Shipment("SHP-S2", "ORD-S2", "UPS", baseTime + 200);
        Payment payment = new Payment("PAY-S2", "ORD-S2", "PAID", baseTime + 1000); // LATE!
        
        // Inject shipment first, then order (triggers evaluation)
        // Payment comes AFTER the order is processed
        pipeline.injectShipment(shipment);
        pipeline.injectOrder(order);
        // Payment arrives late
        pipeline.injectPayment(payment);
        
        // Verify output
        List<ValidatedOrderShipment> results = pipeline.getOutputResults();
        
        System.out.println("Order: " + order);
        System.out.println("Shipment: " + shipment);
        System.out.println("Payment: " + payment + " (LATE!)");
        System.out.println("Results: " + results);
        System.out.println("Validation window: [" + baseTime + ", " + (baseTime + VALIDATION_WINDOW) + "]");
        System.out.println("Payment time: " + payment.eventTime + " (outside window)");
        
        // This demonstrates INCOMPLETENESS: the join SHOULD have happened
        // but the payment arrived too late
        assertEquals(0, results.size(), 
                "Static pipeline misses this join (demonstrates incompleteness)");
        
        // Verify that the order IS validated with relaxed check
        assertTrue(pipeline.getValidationManager().isValidatedRelaxed("ORD-S2"),
                "Order should be validated with relaxed (no-window) check");
        
        System.out.println("✓ S2 passed: Join output MISSED as expected (incompleteness demonstrated)");
        System.out.println("  → This is the key result showing need for dynamic adaptation!\n");
    }
    
    /**
     * Case S3: Late Shipment - shipment arrives after join window
     * 
     * Timeline:
     * - Order at t=1000
     * - Payment at t=1100 (within validation window)
     * - Shipment at t=3000 (LATE! outside join window [1000, 2000])
     * 
     * Expected: NO JOIN OUTPUT (correct behavior - soundness check)
     */
    @Test
    void testCaseS3_LateShipment_ShipmentOutsideJoinWindow() {
        System.out.println("=== Case S3: Late Shipment - Shipment outside join window ===");
        
        long baseTime = 1000;
        
        // Create events - shipment is LATE (outside join window)
        Order order = new Order("ORD-S3", "CUST-3", 150.0, baseTime);
        Payment payment = new Payment("PAY-S3", "ORD-S3", "PAID", baseTime + 100);
        Shipment shipment = new Shipment("SHP-S3", "ORD-S3", "DHL", baseTime + 2000); // LATE!
        
        // Inject in order
        pipeline.injectPayment(payment);
        pipeline.injectShipment(shipment); // Note: this is buffered with its late timestamp
        pipeline.injectOrder(order);
        
        // Verify output
        List<ValidatedOrderShipment> results = pipeline.getOutputResults();
        
        System.out.println("Order: " + order);
        System.out.println("Payment: " + payment);
        System.out.println("Shipment: " + shipment + " (LATE!)");
        System.out.println("Join window: [" + (baseTime + CONTENT_LOWER) + ", " + (baseTime + CONTENT_UPPER) + "]");
        System.out.println("Shipment time: " + shipment.eventTime + " (outside window)");
        System.out.println("Results: " + results);
        
        // Correctly no output - shipment is outside join window
        assertEquals(0, results.size(), 
                "Should NOT produce join - shipment outside join window (correct soundness)");
        
        System.out.println("✓ S3 passed: No join output (correct - shipment too late)\n");
    }
    
    /**
     * Case S4: Missing Payment - no payment ever arrives
     * 
     * Timeline:
     * - Order at t=1000
     * - Shipment at t=1200 (within join window)
     * - NO PAYMENT!
     * 
     * Expected: NO JOIN OUTPUT (correct behavior - no validation)
     */
    @Test
    void testCaseS4_MissingPayment_NoPaymentEver() {
        System.out.println("=== Case S4: Missing Payment - No payment arrives ===");
        
        long baseTime = 1000;
        
        // Create events - NO payment
        Order order = new Order("ORD-S4", "CUST-4", 250.0, baseTime);
        Shipment shipment = new Shipment("SHP-S4", "ORD-S4", "USPS", baseTime + 200);
        
        // Inject - note: no payment
        pipeline.injectShipment(shipment);
        pipeline.injectOrder(order);
        
        // Verify output
        List<ValidatedOrderShipment> results = pipeline.getOutputResults();
        
        System.out.println("Order: " + order);
        System.out.println("Payment: NONE");
        System.out.println("Shipment: " + shipment);
        System.out.println("Results: " + results);
        
        // Correctly no output - no payment means no validation
        assertEquals(0, results.size(), 
                "Should NOT produce join - no payment/validation (correct)");
        
        // Verify validation state
        assertFalse(pipeline.getValidationManager().isValidated("ORD-S4", baseTime),
                "Order should NOT be validated");
        
        System.out.println("✓ S4 passed: No join output (correct - no validation)\n");
    }
    
    /**
     * Combined test: Run all four cases and compute precision/recall metrics.
     */
    @Test
    void testAllCases_ComputeMetrics() {
        System.out.println("=== Combined Evaluation: All Four Cases ===\n");
        
        // Reset pipeline
        pipeline = new ThreeStreamValidationPipeline(
                CONTENT_LOWER, CONTENT_UPPER, VALIDATION_WINDOW, STATE_RETENTION);
        
        // S1: Normal
        Order order1 = new Order("ORD-S1", "CUST-1", 100.0, 1000);
        Payment payment1 = new Payment("PAY-S1", "ORD-S1", "PAID", 1100);
        Shipment shipment1 = new Shipment("SHP-S1", "ORD-S1", "FedEx", 1200);
        
        pipeline.injectPayment(payment1);
        pipeline.injectShipment(shipment1);
        pipeline.injectOrder(order1);
        
        // S2: Late validation
        Order order2 = new Order("ORD-S2", "CUST-2", 200.0, 5000);
        Shipment shipment2 = new Shipment("SHP-S2", "ORD-S2", "UPS", 5200);
        Payment payment2 = new Payment("PAY-S2", "ORD-S2", "PAID", 6000); // Late
        
        pipeline.injectShipment(shipment2);
        pipeline.injectOrder(order2);
        pipeline.injectPayment(payment2);
        
        // S3: Late shipment
        Order order3 = new Order("ORD-S3", "CUST-3", 150.0, 10000);
        Payment payment3 = new Payment("PAY-S3", "ORD-S3", "PAID", 10100);
        Shipment shipment3 = new Shipment("SHP-S3", "ORD-S3", "DHL", 12000); // Late
        
        pipeline.injectPayment(payment3);
        pipeline.injectShipment(shipment3);
        pipeline.injectOrder(order3);
        
        // S4: Missing payment
        Order order4 = new Order("ORD-S4", "CUST-4", 250.0, 15000);
        Shipment shipment4 = new Shipment("SHP-S4", "ORD-S4", "USPS", 15200);
        
        pipeline.injectShipment(shipment4);
        pipeline.injectOrder(order4);
        
        // Collect results
        List<ValidatedOrderShipment> results = pipeline.getOutputResults();
        Set<String> actualOrderIds = new HashSet<>();
        for (ValidatedOrderShipment r : results) {
            actualOrderIds.add(r.order.orderId);
        }
        
        // Expected with static baseline: only S1
        Set<String> expectedStatic = new HashSet<>(Arrays.asList("ORD-S1"));
        
        // Expected with perfect system: S1 and S2
        Set<String> expectedPerfect = new HashSet<>(Arrays.asList("ORD-S1", "ORD-S2"));
        
        // Calculate metrics
        int truePositives = 0;
        int falsePositives = 0;
        int falseNegatives = 0;
        
        for (String id : actualOrderIds) {
            if (expectedPerfect.contains(id)) {
                truePositives++;
            } else {
                falsePositives++;
            }
        }
        for (String id : expectedPerfect) {
            if (!actualOrderIds.contains(id)) {
                falseNegatives++;
            }
        }
        
        double precision = actualOrderIds.isEmpty() ? 1.0 : 
                (double) truePositives / actualOrderIds.size();
        double recall = expectedPerfect.isEmpty() ? 1.0 : 
                (double) truePositives / expectedPerfect.size();
        
        System.out.println("=== Evaluation Results ===");
        System.out.println("Actual outputs: " + actualOrderIds);
        System.out.println("Expected (static): " + expectedStatic);
        System.out.println("Expected (perfect): " + expectedPerfect);
        System.out.println();
        System.out.println("True Positives:  " + truePositives);
        System.out.println("False Positives: " + falsePositives);
        System.out.println("False Negatives: " + falseNegatives + " (S2 missed!)");
        System.out.println();
        System.out.printf("Precision: %.2f (should be ~1.0 - sound)%n", precision);
        System.out.printf("Recall:    %.2f (should be <1.0 - incomplete)%n", recall);
        
        // Assertions
        assertEquals(expectedStatic, actualOrderIds, 
                "Static baseline should only produce S1");
        assertEquals(1.0, precision, 0.01, 
                "Precision should be 1.0 (sound - no incorrect outputs)");
        assertEquals(0.5, recall, 0.01, 
                "Recall should be 0.5 (incomplete - S2 missed)");
        
        System.out.println("\n✓ Evaluation complete!");
        System.out.println("→ Precision = 1.0 confirms SOUNDNESS (no incorrect joins)");
        System.out.println("→ Recall < 1.0 confirms INCOMPLETENESS (S2 missed due to late payment)");
        System.out.println("→ This motivates the need for DYNAMIC ADAPTATION!\n");
    }
    
    /**
     * Test multiple orders with the same shipment timing patterns.
     */
    @Test
    void testMultipleOrders_MixedScenarios() {
        System.out.println("=== Multiple Orders - Mixed Scenarios ===\n");
        
        // Create multiple orders with different validation patterns
        for (int i = 0; i < 5; i++) {
            long baseTime = (i + 1) * 10000;
            String orderId = "ORD-" + i;
            
            Order order = new Order(orderId, "CUST-" + i, 100.0 * (i + 1), baseTime);
            Shipment shipment = new Shipment("SHP-" + i, orderId, "Carrier-" + i, baseTime + 200);
            
            // Alternate: even orders get payment on time, odd orders get late payment
            if (i % 2 == 0) {
                Payment payment = new Payment("PAY-" + i, orderId, "PAID", baseTime + 100);
                pipeline.injectPayment(payment);
            }
            
            pipeline.injectShipment(shipment);
            pipeline.injectOrder(order);
            
            // Late payment for odd orders
            if (i % 2 != 0) {
                Payment latePayment = new Payment("PAY-" + i, orderId, "PAID", baseTime + 1000);
                pipeline.injectPayment(latePayment);
            }
        }
        
        List<ValidatedOrderShipment> results = pipeline.getOutputResults();
        
        System.out.println("Total orders: 5");
        System.out.println("Orders with on-time payment: 3 (even: 0, 2, 4)");
        System.out.println("Orders with late payment: 2 (odd: 1, 3)");
        System.out.println("Actual results: " + results.size());
        
        // Should only have results for even orders (on-time payment)
        assertEquals(3, results.size(), "Should have 3 results (orders with on-time payment)");
        
        for (ValidatedOrderShipment r : results) {
            int orderNum = Integer.parseInt(r.order.orderId.split("-")[1]);
            assertTrue(orderNum % 2 == 0, 
                    "Only even-numbered orders should appear (had on-time payment)");
        }
        
        System.out.println("✓ Test passed: Only orders with on-time payment produced output\n");
    }
}
