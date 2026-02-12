package org.streamreasoning.polyflow.examples.thesis.operators;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.streamreasoning.polyflow.examples.thesis.events.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ElasticValidatedJoinR2R
 * 
 * <p>This R2R operator applies elastic validation rules to a relation containing
 * Orders, Shipments, and Payments. It outputs validated results only when
 * finalization conditions are met.</p>
 */
public class ElasticValidatedJoinR2RTest {

    private static final long CONTENT_WINDOW = 100;    // U
    private static final long VALIDATION_WINDOW = 50;  // V
    private static final long GRACE_PERIOD = 30;       // G

    private ElasticValidatedJoinR2R operator;

    @BeforeEach
    void setUp() {
        operator = new ElasticValidatedJoinR2R(
                CONTENT_WINDOW,
                VALIDATION_WINDOW,
                GRACE_PERIOD,
                Collections.singletonList("events"),
                "validated-results"
        );
    }

    @Test
    @DisplayName("Valid order-shipment-payment produces validated result")
    void testValidJoin() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Payment payment = new Payment("P1", "O1", "PAID", 30);
        Shipment shipment = new Shipment("S1", "O1", "UPS", 50);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromPayment(payment),
                UnifiedEvent.fromShipment(shipment)
        );

        operator.setCurrentTime(60);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        assertEquals(1, results.size());
        assertTrue(results.get(0).isValidated());
        
        ValidatedOrderShipment validated = results.get(0).asValidated();
        assertEquals("O1", validated.order.orderId);
        assertEquals("S1", validated.shipment.shipmentId);
    }

    @Test
    @DisplayName("Order without payment stays pending")
    void testPendingWithoutPayment() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Shipment shipment = new Shipment("S1", "O1", "UPS", 50);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromShipment(shipment)
        );

        // Time still within validation window
        operator.setCurrentTime(60);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // Should be pending (no output yet)
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Order without payment expires as invalid")
    void testInvalidAfterExpiry() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Shipment shipment = new Shipment("S1", "O1", "UPS", 50);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromShipment(shipment)
        );

        // Time past validation deadline (V+G = 80)
        operator.setCurrentTime(100);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // Should be finalized as invalid (no output)
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Order with payment but no shipment stays pending")
    void testPendingWithoutShipment() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Payment payment = new Payment("P1", "O1", "PAID", 30);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromPayment(payment)
        );

        // Time within content window
        operator.setCurrentTime(60);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // Should be pending (waiting for shipment)
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Order with payment but expired content window has no content")
    void testNoContentAfterExpiry() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Payment payment = new Payment("P1", "O1", "PAID", 30);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromPayment(payment)
        );

        // Time past content window (U = 100)
        operator.setCurrentTime(150);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // Should be finalized as no_content (no output)
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Multiple orders with different outcomes")
    void testMultipleOrders() {
        // O1: Valid (payment + shipment)
        Order order1 = new Order("O1", "C1", 100.0, 0);
        Payment payment1 = new Payment("P1", "O1", "PAID", 30);
        Shipment shipment1 = new Shipment("S1", "O1", "UPS", 50);

        // O2: Invalid (no payment, past deadline)
        Order order2 = new Order("O2", "C2", 200.0, 0);
        Shipment shipment2 = new Shipment("S2", "O2", "FedEx", 50);

        // O3: Valid (payment + shipment)
        Order order3 = new Order("O3", "C3", 300.0, 10);
        Payment payment3 = new Payment("P3", "O3", "PAID", 40);
        Shipment shipment3 = new Shipment("S3", "O3", "DHL", 60);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order1),
                UnifiedEvent.fromOrder(order2),
                UnifiedEvent.fromOrder(order3),
                UnifiedEvent.fromPayment(payment1),
                UnifiedEvent.fromPayment(payment3),
                UnifiedEvent.fromShipment(shipment1),
                UnifiedEvent.fromShipment(shipment2),
                UnifiedEvent.fromShipment(shipment3)
        );

        operator.setCurrentTime(100);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // O1 and O3 should produce results, O2 should be invalid
        assertEquals(2, results.size());

        Set<String> validatedOrders = new HashSet<>();
        for (UnifiedEvent result : results) {
            assertTrue(result.isValidated());
            validatedOrders.add(result.asValidated().order.orderId);
        }
        assertTrue(validatedOrders.contains("O1"));
        assertTrue(validatedOrders.contains("O3"));
        assertFalse(validatedOrders.contains("O2"));
    }

    @Test
    @DisplayName("Multiple shipments per order produces multiple results")
    void testMultipleShipmentsPerOrder() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Payment payment = new Payment("P1", "O1", "PAID", 30);
        Shipment shipment1 = new Shipment("S1", "O1", "UPS", 40);
        Shipment shipment2 = new Shipment("S2", "O1", "FedEx", 50);
        Shipment shipment3 = new Shipment("S3", "O1", "DHL", 60);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromPayment(payment),
                UnifiedEvent.fromShipment(shipment1),
                UnifiedEvent.fromShipment(shipment2),
                UnifiedEvent.fromShipment(shipment3)
        );

        operator.setCurrentTime(70);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // Should have 3 results (one per shipment)
        assertEquals(3, results.size());

        Set<String> shipmentIds = new HashSet<>();
        for (UnifiedEvent result : results) {
            shipmentIds.add(result.asValidated().shipment.shipmentId);
        }
        assertTrue(shipmentIds.contains("S1"));
        assertTrue(shipmentIds.contains("S2"));
        assertTrue(shipmentIds.contains("S3"));
    }

    @Test
    @DisplayName("Payment outside validation window makes order invalid")
    void testPaymentOutsideWindow() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Payment payment = new Payment("P1", "O1", "PAID", 100); // Past V+G = 80
        Shipment shipment = new Shipment("S1", "O1", "UPS", 50);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromPayment(payment),
                UnifiedEvent.fromShipment(shipment)
        );

        operator.setCurrentTime(110);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // Should be invalid (payment outside window)
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Shipment outside content window not included")
    void testShipmentOutsideWindow() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Payment payment = new Payment("P1", "O1", "PAID", 30);
        Shipment shipment1 = new Shipment("S1", "O1", "UPS", 50);  // Within window
        Shipment shipment2 = new Shipment("S2", "O1", "FedEx", 150); // Outside window

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromPayment(payment),
                UnifiedEvent.fromShipment(shipment1),
                UnifiedEvent.fromShipment(shipment2)
        );

        operator.setCurrentTime(160);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // Should only include shipment1
        assertEquals(1, results.size());
        assertEquals("S1", results.get(0).asValidated().shipment.shipmentId);
    }

    @Test
    @DisplayName("FAILED payment status doesn't validate order")
    void testFailedPayment() {
        Order order = new Order("O1", "C1", 100.0, 0);
        Payment payment = new Payment("P1", "O1", "FAILED", 30);
        Shipment shipment = new Shipment("S1", "O1", "UPS", 50);

        List<UnifiedEvent> input = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromPayment(payment),
                UnifiedEvent.fromShipment(shipment)
        );

        // Within validation window
        operator.setCurrentTime(60);
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(input));

        // Should be pending (FAILED payment doesn't validate)
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Re-evaluation with new input produces correct results")
    void testReEvaluation() {
        // First evaluation: order without payment
        Order order = new Order("O1", "C1", 100.0, 0);
        Shipment shipment = new Shipment("S1", "O1", "UPS", 50);

        List<UnifiedEvent> input1 = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromShipment(shipment)
        );

        operator.setCurrentTime(60);
        List<UnifiedEvent> results1 = operator.eval(Collections.singletonList(input1));
        assertEquals(0, results1.size()); // Pending

        // Clear state for re-evaluation
        operator.clearState();

        // Second evaluation: same order now with payment
        Payment payment = new Payment("P1", "O1", "PAID", 40);
        List<UnifiedEvent> input2 = Arrays.asList(
                UnifiedEvent.fromOrder(order),
                UnifiedEvent.fromPayment(payment),
                UnifiedEvent.fromShipment(shipment)
        );

        operator.setCurrentTime(60);
        List<UnifiedEvent> results2 = operator.eval(Collections.singletonList(input2));
        assertEquals(1, results2.size()); // Valid now
    }

    @Test
    @DisplayName("Empty input returns empty result")
    void testEmptyInput() {
        List<UnifiedEvent> results = operator.eval(Collections.singletonList(Collections.emptyList()));
        assertTrue(results.isEmpty());
    }

    @Test
    @DisplayName("Null input returns empty result")
    void testNullInput() {
        List<UnifiedEvent> results = operator.eval(null);
        assertTrue(results.isEmpty());
    }
}
