package operators.s2r;

import org.junit.jupiter.api.*;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.intervaljoin.*;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.FlinkIntervalJoinOperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.FlinkIntervalJoinOperator.TimestampedOutput;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.FlinkIntervalJoinOperator.SideOutputRecord;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link FlinkIntervalJoinOperator}.
 *
 * Verified behaviour:
 * <ul>
 *   <li>Correct matches within bounds</li>
 *   <li>No matches outside bounds</li>
 *   <li>Late data goes to side outputs</li>
 *   <li>Cleanup removes state (buffers shrink as watermark advances)</li>
 *   <li>Emitted timestamps = max(leftTs, rightTs)</li>
 *   <li>Inclusive / exclusive bound semantics</li>
 *   <li>Negative and symmetric bounds</li>
 *   <li>Multiple keys isolated from each other</li>
 * </ul>
 */
public class FlinkIntervalJoinOperatorTest {

    // ── simple domain types ──

    static class LRec {
        final String key;
        final String value;
        LRec(String key, String value) { this.key = key; this.value = value; }
        @Override public String toString() { return "L(" + key + "," + value + ")"; }
    }

    static class RRec {
        final String key;
        final String value;
        RRec(String key, String value) { this.key = key; this.value = value; }
        @Override public String toString() { return "R(" + key + "," + value + ")"; }
    }

    static class JoinedRec {
        final String leftValue;
        final String rightValue;
        final long outTs;
        JoinedRec(String l, String r, long ts) { leftValue = l; rightValue = r; outTs = ts; }
        @Override public String toString() { return "J(" + leftValue + "+" + rightValue + " @" + outTs + ")"; }
    }

    // ── helper to build a default operator ──

    private FlinkIntervalJoinOperator<String, LRec, RRec, JoinedRec> buildOp(
            long lower, long upper) {
        return buildOp(lower, upper, true, true);
    }

    private FlinkIntervalJoinOperator<String, LRec, RRec, JoinedRec> buildOp(
            long lower, long upper, boolean lowerInc, boolean upperInc) {
        return FlinkIntervalJoinOperator.<String, LRec, RRec, JoinedRec>builder()
                .lowerBound(lower)
                .upperBound(upper)
                .lowerInclusive(lowerInc)
                .upperInclusive(upperInc)
                .leftKeyExtractor(l -> l.key)
                .rightKeyExtractor(r -> r.key)
                .leftLateDataOutputTag("left-late")
                .rightLateDataOutputTag("right-late")
                .processJoinFunction((left, right, ctx, collector) ->
                        collector.collect(
                                new JoinedRec(left.value, right.value, ctx.getTimestamp()),
                                ctx.getTimestamp()))
                .build();
    }

    // ═══════════════════════════════════════════
    //  1. Correct matches within bounds
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Basic match: right arrives within [tL+lower, tL+upper]")
    void testBasicMatchWithinBounds() {
        // tR ∈ [tL - 10, tL + 10]
        var op = buildOp(-10, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R1"), 105); // 105 ∈ [90, 110] ✓

        List<TimestampedOutput<JoinedRec>> out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals("L1", out.get(0).getRecord().leftValue);
        assertEquals("R1", out.get(0).getRecord().rightValue);
    }

    @Test
    @DisplayName("Left arrives after right — symmetric scan works")
    void testLeftArrivesAfterRight() {
        var op = buildOp(-10, 10);

        op.processRight(new RRec("k1", "R1"), 100);
        op.processLeft(new LRec("k1", "L1"), 105); // for L at 105: tR ∈ [95, 115], and R at 100 ∈ range ✓

        List<TimestampedOutput<JoinedRec>> out = op.drainOutputs();
        assertEquals(1, out.size());
    }

    @Test
    @DisplayName("Exact boundary values match (inclusive)")
    void testExactBoundaryInclusive() {
        var op = buildOp(0, 20);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "Rlow"), 100);   // tR = tL + 0 (lower boundary) ✓
        op.processRight(new RRec("k1", "Rhigh"), 120);   // tR = tL + 20 (upper boundary) ✓

        List<TimestampedOutput<JoinedRec>> out = op.drainOutputs();
        assertEquals(2, out.size());
    }

    @Test
    @DisplayName("Multiple matches for same key produce Cartesian product")
    void testCartesianProduct() {
        var op = buildOp(-5, 5);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processLeft(new LRec("k1", "L2"), 102);

        op.processRight(new RRec("k1", "R1"), 101);
        // R1 at 101 matches L1 (101 ∈ [95,105]) and L2 (101 ∈ [97,107])
        List<TimestampedOutput<JoinedRec>> out = op.drainOutputs();
        assertEquals(2, out.size());
        Set<String> leftValues = out.stream().map(o -> o.getRecord().leftValue).collect(Collectors.toSet());
        assertTrue(leftValues.contains("L1"));
        assertTrue(leftValues.contains("L2"));
    }

    // ═══════════════════════════════════════════
    //  2. No matches outside bounds
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Right outside interval produces no match")
    void testNoMatchOutsideBounds() {
        var op = buildOp(0, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R1"), 99);   // before lower bound
        op.processRight(new RRec("k1", "R2"), 111);   // after upper bound

        assertTrue(op.drainOutputs().isEmpty());
    }

    @Test
    @DisplayName("Different keys never match even if time overlaps")
    void testDifferentKeysNoMatch() {
        var op = buildOp(-10, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k2", "R2"), 100); // same time, different key

        assertTrue(op.drainOutputs().isEmpty());
    }

    @Test
    @DisplayName("Exclusive lower bound: boundary value excluded")
    void testExclusiveLowerBound() {
        var op = buildOp(0, 10, false, true);
        // effective lower = 0+1 = 1, effective upper = 10

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "Rexact"), 100);  // tR = tL + 0, but lower is exclusive → no match
        op.processRight(new RRec("k1", "R101"), 101);     // tR = tL + 1 → match

        List<TimestampedOutput<JoinedRec>> out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals("R101", out.get(0).getRecord().rightValue);
    }

    @Test
    @DisplayName("Exclusive upper bound: boundary value excluded")
    void testExclusiveUpperBound() {
        var op = buildOp(0, 10, true, false);
        // effective lower = 0, effective upper = 10-1 = 9

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R110"), 110);  // tR = tL + 10, upper exclusive → no match
        op.processRight(new RRec("k1", "R109"), 109);  // tR = tL + 9  → match

        List<TimestampedOutput<JoinedRec>> out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals("R109", out.get(0).getRecord().rightValue);
    }

    // ═══════════════════════════════════════════
    //  3. Late data goes to side outputs
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Left record below watermark emitted to left-late side output")
    void testLeftLateData() {
        var op = buildOp(-10, 10);

        op.advanceWatermark(200);
        op.processLeft(new LRec("k1", "LATE-L"), 150);  // 150 < 200

        assertTrue(op.drainOutputs().isEmpty());
        List<SideOutputRecord<?>> side = op.drainSideOutputs();
        assertEquals(1, side.size());
        assertEquals("left-late", side.get(0).getTag());
        assertInstanceOf(LRec.class, side.get(0).getValue());
    }

    @Test
    @DisplayName("Right record below watermark emitted to right-late side output")
    void testRightLateData() {
        var op = buildOp(-10, 10);

        op.advanceWatermark(200);
        op.processRight(new RRec("k1", "LATE-R"), 150);  // 150 < 200

        assertTrue(op.drainOutputs().isEmpty());
        List<SideOutputRecord<?>> side = op.drainSideOutputs();
        assertEquals(1, side.size());
        assertEquals("right-late", side.get(0).getTag());
    }

    @Test
    @DisplayName("Record exactly at watermark is NOT late (late = strictly less than)")
    void testRecordAtWatermarkNotLate() {
        var op = buildOp(-10, 10);

        op.advanceWatermark(100);
        // ts == watermark should be treated as on-time (Flink semantics: < wm is late)
        op.processLeft(new LRec("k1", "L1"), 100);

        assertTrue(op.drainSideOutputs().isEmpty());
        assertEquals(1, op.getLeftBufferSize());
    }

    // ═══════════════════════════════════════════
    //  4. Cleanup removes state
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Left buffer cleaned after watermark passes cleanup time")
    void testLeftBufferCleanup() {
        var op = buildOp(0, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        assertEquals(1, op.getLeftBufferSize());

        // left cleanup timer = tL + upperBound = 100 + 10 = 110
        op.advanceWatermark(109);
        assertEquals(1, op.getLeftBufferSize(), "should not be cleaned yet");

        op.advanceWatermark(110);
        assertEquals(0, op.getLeftBufferSize(), "should be cleaned at wm=110");
    }

    @Test
    @DisplayName("Right buffer cleaned after watermark passes cleanup time")
    void testRightBufferCleanup() {
        // lowerBound = -5, upperBound = 10
        var op = buildOp(-5, 10);

        op.processRight(new RRec("k1", "R1"), 100);
        assertEquals(1, op.getRightBufferSize());

        // right cleanup timer = tR + |lowerBound| = 100 + 5 = 105  (since lowerBound <= 0)
        op.advanceWatermark(104);
        assertEquals(1, op.getRightBufferSize());

        op.advanceWatermark(105);
        assertEquals(0, op.getRightBufferSize());
    }

    @Test
    @DisplayName("State shrinks progressively as watermark advances with many records")
    void testProgressiveStateCleanup() {
        var op = buildOp(0, 10);

        for (int i = 0; i < 10; i++) {
            op.processLeft(new LRec("k1", "L" + i), 100 + i * 20);
        }
        assertEquals(10, op.getLeftBufferSize());

        // Cleanup timers at: 110, 130, 150, 170, 190, 210, 230, 250, 270, 290
        op.advanceWatermark(150);
        assertEquals(7, op.getLeftBufferSize(), "3 records cleaned (timers 110, 130, 150)");

        op.advanceWatermark(290);
        assertEquals(0, op.getLeftBufferSize(), "all cleaned");
    }

    // ═══════════════════════════════════════════
    //  5. Emitted timestamps = max(leftTs, rightTs)
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Output timestamp is max(tL, tR) when tL < tR")
    void testOutputTimestampLeftFirst() {
        var op = buildOp(-10, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R1"), 108);

        var out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals(108, out.get(0).getTimestamp());
        assertEquals(108, out.get(0).getRecord().outTs);
    }

    @Test
    @DisplayName("Output timestamp is max(tL, tR) when tR < tL")
    void testOutputTimestampRightFirst() {
        var op = buildOp(-10, 10);

        op.processRight(new RRec("k1", "R1"), 100);
        op.processLeft(new LRec("k1", "L1"), 108); // tR ∈ [98, 118], 100 ∈ range ✓

        var out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals(108, out.get(0).getTimestamp(), "max(108, 100) = 108");
    }

    @Test
    @DisplayName("Output timestamp with equal event times")
    void testOutputTimestampEqual() {
        var op = buildOp(-5, 5);

        op.processLeft(new LRec("k1", "L1"), 200);
        op.processRight(new RRec("k1", "R1"), 200);

        var out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals(200, out.get(0).getTimestamp());
    }

    // ═══════════════════════════════════════════
    //  6. Negative bounds (right before left)
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Negative lower bound: right can precede left")
    void testNegativeBounds() {
        // tR ∈ [tL - 20, tL - 5]
        var op = buildOp(-20, -5);

        op.processRight(new RRec("k1", "R1"), 80);
        op.processLeft(new LRec("k1", "L1"), 100);
        // for L at 100: tR ∈ [80, 95], R at 80 ✓

        var out = op.drainOutputs();
        assertEquals(1, out.size());
    }

    @Test
    @DisplayName("Symmetric bounds: tR ∈ [tL - Δ, tL + Δ]")
    void testSymmetricBounds() {
        var op = buildOp(-5, 5);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "Rbefore"), 95);  // tR=95 ∈ [95,105] ✓
        op.processRight(new RRec("k1", "Rafter"), 105);   // tR=105 ∈ [95,105] ✓
        op.processRight(new RRec("k1", "Rtoolow"), 94);   // tR=94 ∉ [95,105]
        op.processRight(new RRec("k1", "Rtoohigh"), 106); // tR=106 ∉ [95,105]

        var out = op.drainOutputs();
        assertEquals(2, out.size());
        Set<String> rights = out.stream().map(o -> o.getRecord().rightValue).collect(Collectors.toSet());
        assertTrue(rights.contains("Rbefore"));
        assertTrue(rights.contains("Rafter"));
    }

    // ═══════════════════════════════════════════
    //  7. Multi-key isolation
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Multiple keys: state and matches are isolated per key")
    void testMultiKeyIsolation() {
        var op = buildOp(0, 10);

        op.processLeft(new LRec("A", "LA"), 100);
        op.processLeft(new LRec("B", "LB"), 100);

        op.processRight(new RRec("A", "RA"), 105);
        op.processRight(new RRec("B", "RB"), 105);
        op.processRight(new RRec("C", "RC"), 105); // no left for key C

        var out = op.drainOutputs();
        assertEquals(2, out.size());

        Map<String, String> joinedPairs = out.stream().collect(
                Collectors.toMap(
                        o -> o.getRecord().leftValue,
                        o -> o.getRecord().rightValue));
        assertEquals("RA", joinedPairs.get("LA"));
        assertEquals("RB", joinedPairs.get("LB"));
    }

    // ═══════════════════════════════════════════
    //  8. Long.MIN_VALUE rejection
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Long.MIN_VALUE timestamp is rejected on left")
    void testRejectMinValueLeft() {
        var op = buildOp(-10, 10);
        assertThrows(IllegalArgumentException.class,
                () -> op.processLeft(new LRec("k1", "L1"), Long.MIN_VALUE));
    }

    @Test
    @DisplayName("Long.MIN_VALUE timestamp is rejected on right")
    void testRejectMinValueRight() {
        var op = buildOp(-10, 10);
        assertThrows(IllegalArgumentException.class,
                () -> op.processRight(new RRec("k1", "R1"), Long.MIN_VALUE));
    }

    // ═══════════════════════════════════════════
    //  9. Builder validation
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Builder rejects inverted bounds after exclusive adjustment")
    void testBuilderRejectsInvertedBounds() {
        // lower=5, upper=5, both exclusive → effective lower=6, upper=4 → invalid
        assertThrows(IllegalArgumentException.class, () ->
                FlinkIntervalJoinOperator.<String, LRec, RRec, JoinedRec>builder()
                        .lowerBound(5)
                        .upperBound(5)
                        .lowerInclusive(false)
                        .upperInclusive(false)
                        .leftKeyExtractor(l -> l.key)
                        .rightKeyExtractor(r -> r.key)
                        .processJoinFunction((l, r, c, col) -> {})
                        .build());
    }

    // ═══════════════════════════════════════════
    //  10. Orders ⋈ Shipments end-to-end scenario
    // ═══════════════════════════════════════════

    static class Order {
        final String orderId;
        final double amount;
        Order(String id, double amt) { this.orderId = id; this.amount = amt; }
        @Override public String toString() { return "Order(" + orderId + ")"; }
    }

    static class Shipment {
        final String shipmentId;
        final String orderId;
        Shipment(String sid, String oid) { this.shipmentId = sid; this.orderId = oid; }
        @Override public String toString() { return "Shipment(" + shipmentId + "→" + orderId + ")"; }
    }

    static class Matched {
        final String orderId;
        final String shipmentId;
        final long ts;
        Matched(String oid, String sid, long t) { orderId = oid; shipmentId = sid; ts = t; }
    }

    @Test
    @DisplayName("Orders ⋈ Shipments: order_time BETWEEN ship_time − 4h AND ship_time")
    void testOrderShipmentJoin() {
        // Condition rewritten: tR ∈ [tL + 0, tL + 4h]
        final long H = 3_600_000L;
        var op = FlinkIntervalJoinOperator.<String, Order, Shipment, Matched>builder()
                .lowerBound(0)
                .upperBound(4 * H)
                .leftKeyExtractor(o -> o.orderId)
                .rightKeyExtractor(s -> s.orderId)
                .processJoinFunction((o, s, ctx, col) ->
                        col.collect(new Matched(o.orderId, s.shipmentId, ctx.getTimestamp()),
                                ctx.getTimestamp()))
                .build();

        long t0 = 1_000_000;

        // Orders
        op.processLeft(new Order("ORD-1", 100), t0);
        op.processLeft(new Order("ORD-2", 200), t0 + H);

        // Shipments
        op.processRight(new Shipment("S-A", "ORD-1"), t0 + 2 * H);  // within 4h of ORD-1
        op.processRight(new Shipment("S-B", "ORD-1"), t0 + 5 * H);  // outside 4h of ORD-1
        op.processRight(new Shipment("S-C", "ORD-2"), t0 + 3 * H);  // within 4h of ORD-2

        var out = op.drainOutputs();

        // Expected matches: (ORD-1, S-A) and (ORD-2, S-C)
        assertEquals(2, out.size());

        Set<String> matchedShipments = out.stream()
                .map(o -> o.getRecord().shipmentId)
                .collect(Collectors.toSet());
        assertTrue(matchedShipments.contains("S-A"));
        assertTrue(matchedShipments.contains("S-C"));
        assertFalse(matchedShipments.contains("S-B"));

        // Verify output timestamps
        for (var r : out) {
            if ("S-A".equals(r.getRecord().shipmentId)) {
                assertEquals(t0 + 2 * H, r.getTimestamp(), "max(t0, t0+2h) = t0+2h");
            }
            if ("S-C".equals(r.getRecord().shipmentId)) {
                assertEquals(t0 + 3 * H, r.getTimestamp(), "max(t0+h, t0+3h) = t0+3h");
            }
        }

        // Late shipment after watermark advance
        op.advanceWatermark(t0 + 10 * H);
        op.processRight(new Shipment("S-LATE", "ORD-1"), t0 + H);
        var late = op.drainSideOutputs();
        assertEquals(1, late.size());
        assertEquals("right-late", late.get(0).getTag());

        // Buffers should be cleaned
        assertEquals(0, op.getLeftBufferSize());
        assertEquals(0, op.getRightBufferSize());
    }

    // ═══════════════════════════════════════════
    //  11. Watermark does not go backwards
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Watermark advancement is monotonic — no-op if new < old")
    void testWatermarkMonotonic() {
        var op = buildOp(0, 10);

        op.advanceWatermark(100);
        assertEquals(100, op.getCurrentWatermark());

        op.advanceWatermark(50); // should be ignored
        assertEquals(100, op.getCurrentWatermark());
    }

    // ═══════════════════════════════════════════
    //  12. Interleaved left/right processing
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Interleaved left/right produces correct results regardless of order")
    void testInterleavedProcessing() {
        var op = buildOp(-5, 5);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R1"), 103); // match L1
        op.processLeft(new LRec("k1", "L2"), 106);  // should match R1 (tR=103 ∈ [101,111])
        op.processRight(new RRec("k1", "R2"), 108); // should match L2 (tR=108 ∈ [101,111])
                                                      // and L1? tR=108 ∈ [95,105] — yes

        var out = op.drainOutputs();
        // Expected: (L1,R1), (L2,R1), (L1,R2), (L2,R2)
        // Let's count: L1@100 R1@103 R2@108; L2@106
        // L1 posted first: no right → 0
        // R1@103 arrives: scan left [103-5=98, 103+5=108] → L1@100 ✓ → +1  (L1,R1)
        // L2@106 arrives: scan right [106-5=101, 106+5=111] → R1@103 ✓ → +1  (L2,R1)
        // R2@108 arrives: scan left [108-5=103, 108+5=113] → L2@106 ✓ → +1  (L2,R2)
        //                                                    L1@100? 100 < 103 → ✗
        assertEquals(3, out.size());
    }

    // ═══════════════════════════════════════════
    //  13. Zero-width interval (ltime = rtime)
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Zero-width interval: ltime = rtime (lower=0, upper=0)")
    void testZeroWidthInterval() {
        var op = buildOp(0, 0);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "Rexact"), 100);  // exact match
        op.processRight(new RRec("k1", "Roff"), 101);     // off by 1

        var out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals("Rexact", out.get(0).getRecord().rightValue);
    }

    // ═══════════════════════════════════════════
    //  14. Context provides correct timestamps
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("JoinContext provides correct left, right, and output timestamps")
    void testContextTimestamps() {
        List<long[]> capturedTs = new ArrayList<>();

        var op = FlinkIntervalJoinOperator.<String, LRec, RRec, JoinedRec>builder()
                .lowerBound(-10)
                .upperBound(10)
                .leftKeyExtractor(l -> l.key)
                .rightKeyExtractor(r -> r.key)
                .processJoinFunction((left, right, ctx, collector) -> {
                    capturedTs.add(new long[]{ctx.getLeftTimestamp(), ctx.getRightTimestamp(), ctx.getTimestamp()});
                    collector.collect(new JoinedRec(left.value, right.value, ctx.getTimestamp()), ctx.getTimestamp());
                })
                .build();

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R1"), 107);

        assertEquals(1, capturedTs.size());
        assertEquals(100, capturedTs.get(0)[0], "leftTimestamp");
        assertEquals(107, capturedTs.get(0)[1], "rightTimestamp");
        assertEquals(107, capturedTs.get(0)[2], "outputTimestamp = max(100, 107)");
    }

    // ═══════════════════════════════════════════
    //  15. Side output from join function context
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("User join function can emit to custom side output via context")
    void testUserSideOutput() {
        var op = FlinkIntervalJoinOperator.<String, LRec, RRec, JoinedRec>builder()
                .lowerBound(-10)
                .upperBound(10)
                .leftKeyExtractor(l -> l.key)
                .rightKeyExtractor(r -> r.key)
                .processJoinFunction((left, right, ctx, collector) -> {
                    collector.collect(new JoinedRec(left.value, right.value, ctx.getTimestamp()), ctx.getTimestamp());
                    // Also emit to a user-defined side output
                    ctx.output("audit", "matched:" + left.value + "+" + right.value);
                })
                .build();

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R1"), 105);

        assertEquals(1, op.drainOutputs().size());
        var side = op.drainSideOutputs();
        assertEquals(1, side.size());
        assertEquals("audit", side.get(0).getTag());
        assertEquals("matched:L1+R1", side.get(0).getValue());
    }

    // ═══════════════════════════════════════════
    //  16. Large-scale correctness
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("100 left × 100 right: only time-matching pairs join")
    void testLargeScale() {
        var op = buildOp(0, 5);

        // left: timestamps 0, 10, 20, ..., 990
        for (int i = 0; i < 100; i++) {
            op.processLeft(new LRec("k1", "L" + i), i * 10L);
        }
        // right: timestamps 0, 10, 20, ..., 990
        for (int i = 0; i < 100; i++) {
            op.processRight(new RRec("k1", "R" + i), i * 10L);
        }

        // For each left at tL, right at tR matches if tR ∈ [tL, tL+5].
        // Left i at tL = i*10, right j at tR = j*10.
        // Match iff j*10 ∈ [i*10, i*10 + 5] ⟺ j == i (since step=10 > 5, only exact).
        var out = op.drainOutputs();
        assertEquals(100, out.size(), "each left matches exactly its corresponding right");

        // All timestamps should be max(tL, tR) = tL = tR (since they match exactly)
        for (var o : out) {
            assertEquals(o.getRecord().outTs, o.getTimestamp());
        }
    }

    // ═══════════════════════════════════════════
    //  17. Cleanup timer correctness with positive-only bounds
    // ═══════════════════════════════════════════

    @Test
    @DisplayName("Cleanup with positive lowerBound: right timer fires at tR")
    void testCleanupWithPositiveLowerBound() {
        // tR ∈ [tL + 5, tL + 15]
        var op = buildOp(5, 15);

        op.processRight(new RRec("k1", "R1"), 100);
        assertEquals(1, op.getRightBufferSize());

        // rightCleanupTime: lowerBound > 0 → timerTs = tR = 100
        // tsToRemove: lowerBound > 0 → timerTs = 100
        op.advanceWatermark(99);
        assertEquals(1, op.getRightBufferSize());

        op.advanceWatermark(100);
        assertEquals(0, op.getRightBufferSize());
    }

    @Test
    @DisplayName("Cleanup with zero upperBound: left timer fires at tL")
    void testCleanupWithZeroUpperBound() {
        // tR ∈ [tL - 10, tL + 0]
        var op = buildOp(-10, 0);

        op.processLeft(new LRec("k1", "L1"), 100);
        assertEquals(1, op.getLeftBufferSize());

        // leftCleanupTime: upperBound <= 0 → timerTs = tL = 100
        // tsToRemove: upperBound <= 0 → timerTs = 100
        op.advanceWatermark(100);
        assertEquals(0, op.getLeftBufferSize());
    }
}
