package operators.s2r;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.polyflow.api.secret.content.Content;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.strategies.ReportingStrategy;
import org.streamreasoning.polyflow.api.secret.time.ET;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeInstant;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.DynamicIntervalJoinS2ROperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.DynamicIntervalJoinS2ROperator.ProcessingMode;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.DynamicIntervalJoinS2ROperator.TimestampedElement;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests demonstrating the difference between BATCH and INCREMENTAL processing modes
 * in DynamicIntervalJoinS2ROperator.
 * 
 * Scenario: Order-Payment validation
 * - Orders arrive on the probe stream
 * - Payments arrive on the build stream  
 * - Join on orderId with a grace period to wait for matching payments
 */
public class DynamicIntervalJoinProcessingModeTest {

    // Simple Order class
    static class Order {
        final String orderId;
        final double amount;
        final long timestamp;

        Order(String orderId, double amount, long timestamp) {
            this.orderId = orderId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Order{" + orderId + ", $" + amount + "}";
        }
    }

    // Simple Payment class
    static class Payment {
        final String orderId;
        final double amount;
        final long timestamp;

        Payment(String orderId, double amount, long timestamp) {
            this.orderId = orderId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Payment{" + orderId + ", $" + amount + "}";
        }
    }

    // Combined event for join results
    static class OrderPaymentEvent {
        final Order order;
        final Payment payment;

        OrderPaymentEvent(Order order, Payment payment) {
            this.order = order;
            this.payment = payment;
        }

        @Override
        public String toString() {
            return "Joined{" + order + " + " + payment + "}";
        }
    }

    // Simple content implementation for testing
    static class SimpleContent<T> implements Content<T, T, List<T>> {
        private final List<T> elements = new ArrayList<>();

        @Override
        public void add(T element) {
            elements.add(element);
            System.out.println("    [Content] Added: " + element);
        }

        @Override
        public List<T> coalesce() {
            return new ArrayList<>(elements);
        }

        public int size() {
            return elements.size();
        }
    }

    // Content factory
    static class SimpleContentFactory<T> implements ContentFactory<T, T, List<T>> {
        @Override
        public Content<T, T, List<T>> create() {
            return new SimpleContent<>();
        }

        @Override
        public Content<T, T, List<T>> createEmpty() {
            return new SimpleContent<>();
        }
    }

    // Simple Time implementation
    static class SimpleTime implements Time {
        private long appTime = 0;
        private final List<TimeInstant> evaluationTimes = new ArrayList<>();

        @Override
        public long getAppTime() {
            return appTime;
        }

        @Override
        public void setAppTime(long t) {
            this.appTime = t;
        }

        @Override
        public long getScope() {
            return Long.MAX_VALUE;
        }

        @Override
        public ET getEvaluationTimeInstants() {
            return null;
        }

        @Override
        public void addEvaluationTimeInstants(TimeInstant ti) {
            evaluationTimes.add(ti);
        }

        @Override
        public TimeInstant getEvaluationTime() {
            if (evaluationTimes.isEmpty()) return null;
            return evaluationTimes.remove(0);
        }

        @Override
        public boolean hasEvaluationInstant() {
            return !evaluationTimes.isEmpty();
        }

        public List<TimeInstant> getEvaluationTimesList() {
            return evaluationTimes;
        }
    }

    // Tracking report implementation
    static class TrackingReport implements Report {
        private final AtomicInteger reportCount = new AtomicInteger(0);
        private final List<String> reports = new ArrayList<>();
        private final List<ReportingStrategy> strategies = new ArrayList<>();

        @Override
        public boolean report(Window window, Content<?, ?, ?> content, long ts, long systemTime) {
            int count = reportCount.incrementAndGet();
            String msg = "Report #" + count + " at ts=" + ts + " window=[" + window.getO() + "," + window.getC() + "]";
            reports.add(msg);
            System.out.println("  >>> " + msg);
            return true;
        }

        @Override
        public void add(ReportingStrategy r) {
            strategies.add(r);
        }

        @Override
        public ReportingStrategy[] strategies() {
            return strategies.toArray(new ReportingStrategy[0]);
        }

        public int getReportCount() {
            return reportCount.get();
        }

        public List<String> getReports() {
            return reports;
        }

        public void reset() {
            reportCount.set(0);
            reports.clear();
        }
    }

    private Map<String, TreeMap<Long, List<TimestampedElement<Object>>>> buildBuffer;
    private SimpleContentFactory<Object> contentFactory;
    private SimpleTime time;
    private TrackingReport report;

    @BeforeEach
    void setUp() {
        buildBuffer = new HashMap<>();
        contentFactory = new SimpleContentFactory<>();
        time = new SimpleTime();
        report = new TrackingReport();
    }

    @Test
    @DisplayName("BATCH Mode: Collect all matches, report at grace period expiry")
    void testBatchMode() {
        System.out.println("\n========== BATCH MODE TEST ==========");
        System.out.println("Grace period: 1000ms");
        System.out.println("Behavior: Collect all matches, report when grace period expires\n");

        long gracePeriod = 1000;
        long stateRetention = 10000;

        DynamicIntervalJoinS2ROperator<Object, String, Object, List<Object>> operator =
                new DynamicIntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "order-payment-batch",
                        contentFactory,
                        report,
                        gracePeriod,
                        buildBuffer,
                        e -> {
                            if (e instanceof Order) return ((Order) e).orderId;
                            if (e instanceof Payment) return ((Payment) e).orderId;
                            return "unknown";
                        },
                        e -> {
                            if (e instanceof Order) return ((Order) e).timestamp;
                            if (e instanceof Payment) return ((Payment) e).timestamp;
                            return 0L;
                        },
                        null,   // joinResultFactory
                        stateRetention,
                        false,  // closeOnFirstMatch
                        ProcessingMode.BATCH
                );

        System.out.println("Step 1: Order arrives at t=100");
        Order order1 = new Order("ORD-001", 99.99, 100);
        operator.compute(order1, 100);

        System.out.println("  Pending windows: " + operator.getPendingWindowCount());
        System.out.println("  Report count: " + report.getReportCount());
        assertEquals(1, operator.getPendingWindowCount(), "Should have 1 pending window");
        assertEquals(0, report.getReportCount(), "BATCH: Should NOT report yet");

        System.out.println("\nStep 2: Payment arrives at t=200 (within grace period)");
        Payment payment1 = new Payment("ORD-001", 99.99, 200);
        DynamicIntervalJoinS2ROperator.addToBuildBuffer(buildBuffer, payment1, "ORD-001", 200);
        // Trigger matching by calling addToBuildBufferAndMatch
        operator.addToBuildBufferAndMatch(payment1, "ORD-001", 200);

        System.out.println("  Report count: " + report.getReportCount());
        assertEquals(0, report.getReportCount(), "BATCH: Should still NOT report (waiting for grace period)");
        assertTrue(operator.hasMatchedWindowForKey("ORD-001"), "Window should be marked as matched");

        System.out.println("\nStep 3: Another payment at t=300 (still within grace period)");
        Payment payment2 = new Payment("ORD-001", 50.00, 300);
        operator.addToBuildBufferAndMatch(payment2, "ORD-001", 300);

        System.out.println("  Report count: " + report.getReportCount());
        assertEquals(0, report.getReportCount(), "BATCH: Still waiting");

        System.out.println("\nStep 4: Time advances past grace period (t=1200)");
        // Force check expired windows by advancing time
        operator.forceCheckExpiredWindows(1200);

        System.out.println("  Report count: " + report.getReportCount());
        System.out.println("  Completed windows: " + operator.getCompletedWindowCount());
        assertEquals(1, report.getReportCount(), "BATCH: NOW should report (grace period expired)");
        assertEquals(1, operator.getCompletedWindowCount(), "Should have 1 completed window");

        System.out.println("\n✓ BATCH mode works: Collected all matches, reported once at end");
    }

    @Test
    @DisplayName("INCREMENTAL Mode: Report immediately on each match")
    void testIncrementalMode() {
        System.out.println("\n========== INCREMENTAL MODE TEST ==========");
        System.out.println("Grace period: 1000ms");
        System.out.println("Behavior: Report immediately on each match\n");

        long gracePeriod = 1000;
        long stateRetention = 10000;

        DynamicIntervalJoinS2ROperator<Object, String, Object, List<Object>> operator =
                new DynamicIntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN,
                        time,
                        "order-payment-incremental",
                        contentFactory,
                        report,
                        gracePeriod,
                        buildBuffer,
                        e -> {
                            if (e instanceof Order) return ((Order) e).orderId;
                            if (e instanceof Payment) return ((Payment) e).orderId;
                            return "unknown";
                        },
                        e -> {
                            if (e instanceof Order) return ((Order) e).timestamp;
                            if (e instanceof Payment) return ((Payment) e).timestamp;
                            return 0L;
                        },
                        null,  // joinResultFactory
                        stateRetention,
                        false,  // closeOnFirstMatch
                        ProcessingMode.INCREMENTAL
                );

        // Pre-populate build buffer with a payment
        System.out.println("Setup: Payment already in buffer at t=50");
        Payment earlyPayment = new Payment("ORD-001", 99.99, 50);
        DynamicIntervalJoinS2ROperator.addToBuildBuffer(buildBuffer, earlyPayment, "ORD-001", 50);

        System.out.println("\nStep 1: Order arrives at t=100 (matches existing payment)");
        Order order1 = new Order("ORD-001", 99.99, 100);
        operator.compute(order1, 100);

        System.out.println("  Report count: " + report.getReportCount());
        assertEquals(1, report.getReportCount(), "INCREMENTAL: Should report IMMEDIATELY on match");

        System.out.println("\nStep 2: Another payment arrives at t=200");
        Payment payment2 = new Payment("ORD-001", 25.00, 200);
        operator.addToBuildBufferAndMatch(payment2, "ORD-001", 200);

        System.out.println("  Report count: " + report.getReportCount());
        assertEquals(2, report.getReportCount(), "INCREMENTAL: Should report again on new match");

        System.out.println("\nStep 3: Third payment at t=300");
        Payment payment3 = new Payment("ORD-001", 10.00, 300);
        operator.addToBuildBufferAndMatch(payment3, "ORD-001", 300);

        System.out.println("  Report count: " + report.getReportCount());
        assertEquals(3, report.getReportCount(), "INCREMENTAL: Report on each match");

        System.out.println("\n✓ INCREMENTAL mode works: Reported immediately on each match");
    }

    @Test
    @DisplayName("Compare BATCH vs INCREMENTAL with same input")
    void testCompareModesWithSameInput() {
        System.out.println("\n========== COMPARISON TEST ==========");
        System.out.println("Same events, different modes\n");

        long gracePeriod = 500;
        long stateRetention = 10000;

        // Create BATCH operator
        TrackingReport batchReport = new TrackingReport();
        Map<String, TreeMap<Long, List<TimestampedElement<Object>>>> batchBuildBuffer = new HashMap<>();

        DynamicIntervalJoinS2ROperator<Object, String, Object, List<Object>> batchOp =
                new DynamicIntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN, new SimpleTime(), "batch",
                        contentFactory, batchReport, gracePeriod, batchBuildBuffer,
                        e -> e instanceof Order ? ((Order) e).orderId : ((Payment) e).orderId,
                        e -> e instanceof Order ? ((Order) e).timestamp : ((Payment) e).timestamp,
                        null, stateRetention, false, ProcessingMode.BATCH
                );

        // Create INCREMENTAL operator
        TrackingReport incrReport = new TrackingReport();
        Map<String, TreeMap<Long, List<TimestampedElement<Object>>>> incrBuildBuffer = new HashMap<>();

        DynamicIntervalJoinS2ROperator<Object, String, Object, List<Object>> incrOp =
                new DynamicIntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN, new SimpleTime(), "incremental",
                        contentFactory, incrReport, gracePeriod, incrBuildBuffer,
                        e -> e instanceof Order ? ((Order) e).orderId : ((Payment) e).orderId,
                        e -> e instanceof Order ? ((Order) e).timestamp : ((Payment) e).timestamp,
                        null, stateRetention, false, ProcessingMode.INCREMENTAL
                );

        // Same input sequence
        Order order = new Order("ORD-X", 100.0, 100);
        Payment p1 = new Payment("ORD-X", 50.0, 50);
        Payment p2 = new Payment("ORD-X", 30.0, 150);
        Payment p3 = new Payment("ORD-X", 20.0, 200);

        // Pre-add payment to both buffers
        DynamicIntervalJoinS2ROperator.addToBuildBuffer(batchBuildBuffer, p1, "ORD-X", 50);
        DynamicIntervalJoinS2ROperator.addToBuildBuffer(incrBuildBuffer, p1, "ORD-X", 50);

        System.out.println("Event: Order@100 (payment@50 already in buffer)");
        batchOp.compute(order, 100);
        incrOp.compute(order, 100);
        System.out.println("  BATCH reports: " + batchReport.getReportCount());
        System.out.println("  INCREMENTAL reports: " + incrReport.getReportCount());

        System.out.println("\nEvent: Payment@150");
        batchOp.addToBuildBufferAndMatch(p2, "ORD-X", 150);
        incrOp.addToBuildBufferAndMatch(p2, "ORD-X", 150);
        System.out.println("  BATCH reports: " + batchReport.getReportCount());
        System.out.println("  INCREMENTAL reports: " + incrReport.getReportCount());

        System.out.println("\nEvent: Payment@200");
        batchOp.addToBuildBufferAndMatch(p3, "ORD-X", 200);
        incrOp.addToBuildBufferAndMatch(p3, "ORD-X", 200);
        System.out.println("  BATCH reports: " + batchReport.getReportCount());
        System.out.println("  INCREMENTAL reports: " + incrReport.getReportCount());

        System.out.println("\nGrace period expires (t=700)");
        batchOp.forceCheckExpiredWindows(700);
        incrOp.forceCheckExpiredWindows(700);
        System.out.println("  BATCH reports: " + batchReport.getReportCount());
        System.out.println("  INCREMENTAL reports: " + incrReport.getReportCount());

        System.out.println("\n--- Summary ---");
        System.out.println("BATCH total reports: " + batchReport.getReportCount() + " (one at end)");
        System.out.println("INCREMENTAL total reports: " + incrReport.getReportCount() + " (one per match)");

        assertTrue(incrReport.getReportCount() > batchReport.getReportCount(),
                "INCREMENTAL should have more reports (immediate per match)");

        System.out.println("\n✓ BATCH collected all matches, reported once");
        System.out.println("✓ INCREMENTAL reported on each match as it happened");
    }

    @Test
    @DisplayName("Unmatched order behavior in both modes")
    void testUnmatchedOrder() {
        System.out.println("\n========== UNMATCHED ORDER TEST ==========");
        System.out.println("Order without any matching payment\n");

        long gracePeriod = 500;

        // BATCH mode
        TrackingReport batchReport = new TrackingReport();
        DynamicIntervalJoinS2ROperator<Object, String, Object, List<Object>> batchOp =
                new DynamicIntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN, new SimpleTime(), "batch",
                        contentFactory, batchReport, gracePeriod, new HashMap<>(),
                        e -> ((Order) e).orderId, e -> ((Order) e).timestamp,
                        null, 10000, false, ProcessingMode.BATCH
                );

        // INCREMENTAL mode
        TrackingReport incrReport = new TrackingReport();
        DynamicIntervalJoinS2ROperator<Object, String, Object, List<Object>> incrOp =
                new DynamicIntervalJoinS2ROperator<>(
                        Tick.TIME_DRIVEN, new SimpleTime(), "incremental",
                        contentFactory, incrReport, gracePeriod, new HashMap<>(),
                        e -> ((Order) e).orderId, e -> ((Order) e).timestamp,
                        null, 10000, false, ProcessingMode.INCREMENTAL
                );

        Order order = new Order("LONELY-001", 50.0, 100);

        System.out.println("Order arrives with no matching payment");
        batchOp.compute(order, 100);
        incrOp.compute(order, 100);

        System.out.println("  BATCH pending: " + batchOp.getPendingWindowCount());
        System.out.println("  INCREMENTAL pending: " + incrOp.getPendingWindowCount());

        assertEquals(1, batchOp.getPendingWindowCount());
        assertEquals(1, incrOp.getPendingWindowCount());

        System.out.println("\nGrace period expires...");
        batchOp.forceCheckExpiredWindows(700);
        incrOp.forceCheckExpiredWindows(700);

        System.out.println("  BATCH reports: " + batchReport.getReportCount() + " (unmatched order still reported)");
        System.out.println("  INCREMENTAL reports: " + incrReport.getReportCount());

        // Both should report the unmatched order when grace period expires
        assertEquals(1, batchReport.getReportCount(), "BATCH should report even unmatched");
        assertEquals(0, incrReport.getReportCount(), "INCREMENTAL only reports on matches");

        System.out.println("\n✓ BATCH reports unmatched orders at grace period end");
        System.out.println("✓ INCREMENTAL does not report if no match occurred");
    }
}
