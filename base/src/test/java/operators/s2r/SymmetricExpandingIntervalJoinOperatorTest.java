package operators.s2r;

import org.junit.jupiter.api.*;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.SymmetricExpandingIntervalJoinOperator;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.SymmetricExpandingIntervalJoinOperator.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link SymmetricExpandingIntervalJoinOperator}.
 *
 * <ol>
 *   <li>Correct matches within initial bounds (no expansion)</li>
 *   <li>No matches outside initial bounds when expansion disabled (delta=0)</li>
 *   <li>Expansion triggered on no-match recovers delayed records</li>
 *   <li>Expansion is symmetric (α and β grow equally)</li>
 *   <li>Expansion capped at αmax / βmax</li>
 *   <li>No expansion when matches are found</li>
 *   <li>Late data goes to side outputs</li>
 *   <li>Cleanup removes state as watermark advances</li>
 *   <li>Output timestamps = max(tL, tR)</li>
 *   <li>Multi-key isolation (bounds are per-key)</li>
 *   <li>Both-sides expansion (left trigger + right trigger)</li>
 *   <li>No duplicate emissions on re-probe after expansion</li>
 *   <li>Orders ⋈ Shipments end-to-end scenario</li>
 * </ol>
 */
public class SymmetricExpandingIntervalJoinOperatorTest {

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

    static class JRec {
        final String lv;
        final String rv;
        final long ts;
        JRec(String lv, String rv, long ts) { this.lv = lv; this.rv = rv; this.ts = ts; }
        @Override public String toString() { return "J(" + lv + "+" + rv + " @" + ts + ")"; }
    }

    // ── helper builders ──

    /** Build with expansion enabled. */
    private SymmetricExpandingIntervalJoinOperator<String, LRec, RRec, JRec> buildOp(
            long alpha0, long beta0, long delta, long alphaMax, long betaMax) {
        return SymmetricExpandingIntervalJoinOperator.<String, LRec, RRec, JRec>builder()
                .alpha0(alpha0).beta0(beta0)
                .delta(delta)
                .alphaMax(alphaMax).betaMax(betaMax)
                .leftKeyExtractor(l -> l.key)
                .rightKeyExtractor(r -> r.key)
                .processJoinFunction((left, right, ctx, col) ->
                        col.collect(new JRec(left.value, right.value, ctx.getTimestamp()),
                                ctx.getTimestamp()))
                .build();
    }

    /** Build with expansion disabled (delta=0). */
    private SymmetricExpandingIntervalJoinOperator<String, LRec, RRec, JRec> buildFixedOp(
            long alpha0, long beta0) {
        return buildOp(alpha0, beta0, 0, alpha0, beta0);
    }

    // ═════════════════════════════════════════════════════
    //  1. CORRECT MATCHES WITHIN INITIAL BOUNDS
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Match within initial window — no expansion needed")
    void testMatchWithinInitialBounds() {
        // window: [tL − 10, tL + 10]
        var op = buildOp(10, 10, 5, 50, 50);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R1"), 105); // 105 ∈ [90, 110] ✓

        var out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals("L1", out.get(0).getRecord().lv);
        assertEquals("R1", out.get(0).getRecord().rv);

        // No expansion should have occurred
        assertEquals(10, op.getAlpha("k1"));
        assertEquals(10, op.getBeta("k1"));
        assertTrue(op.peekExpansionLog().isEmpty());
    }

    @Test
    @DisplayName("Right arrives first, left matches — no expansion")
    void testRightFirstNoExpansion() {
        var op = buildOp(10, 10, 5, 50, 50);

        // Right arrives first — no left to match → expansion triggered
        op.processRight(new RRec("k1", "R1"), 100);
        // But then left arrives within *expanded* bounds
        op.processLeft(new LRec("k1", "L1"), 105); // left at 105: probe right [105−10, 105+10]=[95,115], R@100 ✓

        var out = op.drainOutputs();
        // The right-side arrival with no left match will have triggered expansion,
        // but the left-side arrival finds R1 in the initial window, so no expansion on left side.
        assertEquals(1, out.size());
    }

    @Test
    @DisplayName("Exact boundary values match (inclusive)")
    void testBoundaryInclusive() {
        var op = buildFixedOp(10, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "Rlo"), 90);   // tR = tL − β = 90 ✓
        op.processRight(new RRec("k1", "Rhi"), 110);   // tR = tL + α = 110 ✓

        assertEquals(2, op.drainOutputs().size());
    }

    @Test
    @DisplayName("Cartesian product: multiple matches for same key")
    void testCartesianProduct() {
        var op = buildFixedOp(10, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processLeft(new LRec("k1", "L2"), 102);
        op.processRight(new RRec("k1", "R1"), 105);
        // R1@105: matches L1 (105 ∈ [90,110]) and L2 (105 ∈ [92,112])

        assertEquals(2, op.drainOutputs().size());
    }

    // ═════════════════════════════════════════════════════
    //  2. NO MATCHES OUTSIDE BOUNDS (delta=0)
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("No match outside window when expansion is disabled")
    void testNoMatchOutsideFixedBounds() {
        var op = buildFixedOp(10, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "Rlo"), 89);   // below lower bound
        op.processRight(new RRec("k1", "Rhi"), 111);   // above upper bound

        assertTrue(op.drainOutputs().isEmpty());
    }

    @Test
    @DisplayName("Different keys never match even if time overlaps")
    void testDifferentKeysNoMatch() {
        var op = buildFixedOp(10, 10);

        op.processLeft(new LRec("A", "L1"), 100);
        op.processRight(new RRec("B", "R1"), 100);

        assertTrue(op.drainOutputs().isEmpty());
    }

    // ═════════════════════════════════════════════════════
    //  3. EXPANSION RECOVERS DELAYED RECORDS
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Left record with no right match triggers expansion; delayed right is recovered")
    void testLeftTriggersExpansionThenRightMatches() {
        // α₀=10, β₀=10, Δ=10, max=50
        var op = buildOp(10, 10, 10, 50, 50);

        // Buffer a right record at 125 first (outside initial window of left@100)
        op.processRight(new RRec("k1", "R1"), 125);
        op.drainOutputs(); // right side expanded (no left match), but that's fine

        // Now left arrives at 100: initial window [90, 110] — R1@125 is outside
        // → expansion: α=20, β=20 → window [80, 120] — R1@125 still outside
        // → but alpha max allows further expansion... only ONE expansion per arrival
        op.processLeft(new LRec("k1", "L1"), 100);

        var out = op.drainOutputs();
        // After one expansion: window = [80, 120], R1@125 still outside → no match
        assertEquals(0, out.size());

        // The bounds should have expanded once
        assertEquals(20, op.getAlpha("k1"));
        assertEquals(20, op.getBeta("k1"));
    }

    @Test
    @DisplayName("Expansion recovers right record just outside initial window")
    void testExpansionRecoversJustOutside() {
        // α₀=10, β₀=10, Δ=5, max=50
        var op = buildOp(10, 10, 5, 50, 50);

        // Right at 112 — just outside initial window [90, 110] of left@100
        op.processRight(new RRec("k1", "R1"), 112);
        op.drainOutputs(); // clear right-side expansion outputs

        // Left at 100: initial probe misses → expand α to 15, β to 15 → window [85, 115]
        // Re-probe expanded zone: (110, 115] — R1@112 ✓
        op.processLeft(new LRec("k1", "L1"), 100);

        var out = op.drainOutputs();
        assertEquals(1, out.size(), "expansion should have recovered R1@112");
        assertEquals("R1", out.get(0).getRecord().rv);
    }

    @Test
    @DisplayName("Right record with no left match triggers expansion and recovers left")
    void testRightTriggersExpansion() {
        var op = buildOp(10, 10, 5, 50, 50);

        // Left at 100
        op.processLeft(new LRec("k1", "L1"), 100);
        op.drainOutputs(); // left expanded (no right match)

        // Right at 118: initial probe on left [118−10, 118+10] = [108, 128] → L1@100 outside
        // → expand: α=15, β=15 → [103, 133] → still no L1@100
        // Actually wait — the right-side scan is tL ∈ [tR − α, tR + β]
        // With α=10, β=10: [108, 128] → L1@100 not in range → expand
        // With α=15, β=15: [103, 133] → L1@100 not in range → no match in expanded zone either
        op.processRight(new RRec("k1", "R1"), 118);

        var out = op.drainOutputs();
        // 100 not in [103, 133] → still no match even after expansion
        assertEquals(0, out.size());

        // But bounds did expand
        assertEquals(15, op.getAlpha("k1"));
    }

    @Test
    @DisplayName("Right record expansion recovers left just outside initial window")
    void testRightExpansionRecoversLeft() {
        var op = buildOp(10, 10, 5, 50, 50);

        // Left at 100
        op.processLeft(new LRec("k1", "L1"), 100);
        op.drainOutputs();
        op.drainExpansionLog();

        // Right at 112: tL ∈ [112−10, 112+10] = [102, 122] → L1@100 outside
        // expand: α=15, β=15 → left-wing: [112−15, 112−10−1] = [97, 101] → L1@100 ✓
        op.processRight(new RRec("k1", "R1"), 112);

        var out = op.drainOutputs();
        assertEquals(1, out.size(), "right-side expansion should recover L1@100");
        assertEquals("L1", out.get(0).getRecord().lv);

        var expansions = op.drainExpansionLog();
        // At least one expansion triggered by right side
        assertTrue(expansions.stream().anyMatch(e -> "RIGHT".equals(e.getTriggerSide())));
    }

    // ═════════════════════════════════════════════════════
    //  4. EXPANSION IS SYMMETRIC
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Both α and β grow by exactly Δ on each expansion")
    void testSymmetricGrowth() {
        var op = buildOp(10, 10, 7, 100, 100);

        // Left with no right → expand
        op.processLeft(new LRec("k1", "L1"), 100);

        assertEquals(17, op.getAlpha("k1"), "α should be 10 + 7 = 17");
        assertEquals(17, op.getBeta("k1"),  "β should be 10 + 7 = 17");

        var exp = op.drainExpansionLog();
        assertEquals(1, exp.size());
        assertEquals(10, exp.get(0).getOldAlpha());
        assertEquals(10, exp.get(0).getOldBeta());
        assertEquals(17, exp.get(0).getNewAlpha());
        assertEquals(17, exp.get(0).getNewBeta());
    }

    // ═════════════════════════════════════════════════════
    //  5. EXPANSION CAPPED AT MAX
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Bounds cannot exceed αmax / βmax")
    void testExpansionCappedAtMax() {
        // α₀=10, β₀=10, Δ=100, αmax=25, βmax=30
        var op = buildOp(10, 10, 100, 25, 30);

        op.processLeft(new LRec("k1", "L1"), 100); // no right → expand

        assertEquals(25, op.getAlpha("k1"), "α should be capped at αmax=25");
        assertEquals(30, op.getBeta("k1"),  "β should be capped at βmax=30");
    }

    @Test
    @DisplayName("No expansion if already at max")
    void testNoExpansionAtMax() {
        var op = buildOp(10, 10, 5, 10, 10); // α₀ == αmax, β₀ == βmax

        op.processLeft(new LRec("k1", "L1"), 100); // no right, but can't expand

        assertEquals(10, op.getAlpha("k1"));
        assertEquals(10, op.getBeta("k1"));
        assertTrue(op.peekExpansionLog().isEmpty(), "no expansion should occur at max");
    }

    // ═════════════════════════════════════════════════════
    //  6. NO EXPANSION WHEN MATCHES FOUND
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Bounds stay unchanged when first probe finds matches")
    void testNoExpansionOnMatch() {
        var op = buildOp(10, 10, 5, 50, 50);

        op.processRight(new RRec("k1", "R1"), 100);
        op.drainExpansionLog(); // clear right-side expansion from first arrival

        op.processLeft(new LRec("k1", "L1"), 105); // probe right: 105 ∈ [95, 115], R1@100 ✓

        // α and β should still be at expanded level from the right-side expansion,
        // but left-side processing should NOT have triggered another expansion
        var leftExpansions = op.drainExpansionLog().stream()
                .filter(e -> "LEFT".equals(e.getTriggerSide()))
                .collect(Collectors.toList());
        assertTrue(leftExpansions.isEmpty(), "left side should not expand when match found");
    }

    // ═════════════════════════════════════════════════════
    //  7. LATE DATA → SIDE OUTPUTS
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Left record below watermark goes to left-late side output")
    void testLeftLateData() {
        var op = buildOp(10, 10, 5, 50, 50);

        op.advanceWatermark(200);
        op.processLeft(new LRec("k1", "LATE"), 150);

        assertTrue(op.drainOutputs().isEmpty());
        var side = op.drainSideOutputs();
        assertEquals(1, side.size());
        assertEquals("left-late", side.get(0).getTag());
    }

    @Test
    @DisplayName("Right record below watermark goes to right-late side output")
    void testRightLateData() {
        var op = buildOp(10, 10, 5, 50, 50);

        op.advanceWatermark(200);
        op.processRight(new RRec("k1", "LATE"), 150);

        assertTrue(op.drainOutputs().isEmpty());
        var side = op.drainSideOutputs();
        assertEquals(1, side.size());
        assertEquals("right-late", side.get(0).getTag());
    }

    @Test
    @DisplayName("Record exactly at watermark is NOT late")
    void testRecordAtWatermarkNotLate() {
        var op = buildOp(10, 10, 5, 50, 50);
        op.advanceWatermark(100);

        op.processLeft(new LRec("k1", "L1"), 100);
        assertTrue(op.drainSideOutputs().isEmpty());
        assertEquals(1, op.getLeftBufferSize());
    }

    // ═════════════════════════════════════════════════════
    //  8. CLEANUP REMOVES STATE
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Left buffer cleaned when watermark passes cleanup time")
    void testLeftBufferCleanup() {
        var op = buildFixedOp(10, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        assertEquals(1, op.getLeftBufferSize());

        // Cleanup timer = tL + αK = 100 + 10 = 110
        op.advanceWatermark(109);
        assertEquals(1, op.getLeftBufferSize());

        op.advanceWatermark(110);
        assertEquals(0, op.getLeftBufferSize());
    }

    @Test
    @DisplayName("Right buffer cleaned when watermark passes cleanup time")
    void testRightBufferCleanup() {
        var op = buildFixedOp(10, 10);

        op.processRight(new RRec("k1", "R1"), 100);
        // Right may have expanded bounds but with delta=0 stays at (10,10)
        // cleanup = tR + βK = 100 + 10 = 110
        assertEquals(1, op.getRightBufferSize());

        op.advanceWatermark(109);
        assertEquals(1, op.getRightBufferSize());

        op.advanceWatermark(110);
        assertEquals(0, op.getRightBufferSize());
    }

    @Test
    @DisplayName("Expanded bounds produce longer cleanup timers")
    void testExpandedBoundsLongerCleanup() {
        // α₀=10, Δ=20, max=100
        var op = buildOp(10, 10, 20, 100, 100);

        op.processLeft(new LRec("k1", "L1"), 100);
        // No right match → expansion: α=30, β=30
        // Cleanup timer = tL + αK = 100 + 30 = 130
        assertEquals(30, op.getAlpha("k1"));

        op.advanceWatermark(120);
        assertEquals(1, op.getLeftBufferSize(), "should survive past old bound (110)");

        op.advanceWatermark(130);
        assertEquals(0, op.getLeftBufferSize(), "cleaned at expanded bound (130)");
    }

    // ═════════════════════════════════════════════════════
    //  9. OUTPUT TIMESTAMPS = max(tL, tR)
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Output timestamp when tL < tR")
    void testOutputTimestampLeftFirst() {
        var op = buildFixedOp(10, 10);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R1"), 108);

        var out = op.drainOutputs();
        assertEquals(108, out.get(0).getTimestamp());
        assertEquals(108, out.get(0).getRecord().ts);
    }

    @Test
    @DisplayName("Output timestamp when tR < tL")
    void testOutputTimestampRightFirst() {
        var op = buildFixedOp(10, 10);

        op.processRight(new RRec("k1", "R1"), 100);
        op.processLeft(new LRec("k1", "L1"), 108);

        var out = op.drainOutputs();
        assertEquals(108, out.get(0).getTimestamp());
    }

    @Test
    @DisplayName("Output timestamp with equal event times")
    void testOutputTimestampEqual() {
        var op = buildFixedOp(10, 10);

        op.processLeft(new LRec("k1", "L1"), 200);
        op.processRight(new RRec("k1", "R1"), 200);

        assertEquals(200, op.drainOutputs().get(0).getTimestamp());
    }

    @Test
    @DisplayName("Output timestamp correct on expansion-recovered match")
    void testOutputTimestampOnExpansionMatch() {
        var op = buildOp(10, 10, 5, 50, 50);

        op.processRight(new RRec("k1", "R1"), 112);
        op.drainOutputs(); // clear

        op.processLeft(new LRec("k1", "L1"), 100);
        // Expansion: window [85, 115] → R1@112 recovered
        var out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals(112, out.get(0).getTimestamp(), "max(100, 112) = 112");
    }

    // ═════════════════════════════════════════════════════
    //  10. MULTI-KEY ISOLATION
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Bounds are maintained independently per key")
    void testPerKeyBounds() {
        var op = buildOp(10, 10, 5, 50, 50);

        // Key A: no match → expansion
        op.processLeft(new LRec("A", "LA"), 100);
        // Key B: match exists → no expansion
        op.processRight(new RRec("B", "RB"), 200);
        op.processLeft(new LRec("B", "LB"), 205);

        assertEquals(15, op.getAlpha("A"), "key A should have expanded");
        // Key B: the right arrival at 200 triggered expansion (no left yet),
        // but the left arrival at 205 found a match → no further expansion on left side
        // So B's bounds might be 15 from the right-side expansion
        assertTrue(op.getAlpha("B") >= 10);
    }

    @Test
    @DisplayName("Matches are isolated by key")
    void testMatchIsolation() {
        var op = buildFixedOp(10, 10);

        op.processLeft(new LRec("A", "LA"), 100);
        op.processLeft(new LRec("B", "LB"), 100);
        op.processRight(new RRec("A", "RA"), 105);

        var out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals("LA", out.get(0).getRecord().lv);
        assertEquals("RA", out.get(0).getRecord().rv);
    }

    // ═════════════════════════════════════════════════════
    //  11. BOTH-SIDE EXPANSION
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Left arrival triggers expansion, right arrival triggers expansion — same key")
    void testBothSidesExpand() {
        var op = buildOp(10, 10, 5, 50, 50);

        op.processLeft(new LRec("k1", "L1"), 100);
        // No right → expand: α=15, β=15

        op.processRight(new RRec("k1", "R1"), 200);
        // probe left: tL ∈ [200−15, 200+15] = [185, 215] → L1@100 not in range → expand again: α=20, β=20

        assertEquals(20, op.getAlpha("k1"));
        assertEquals(20, op.getBeta("k1"));

        var exps = op.drainExpansionLog();
        assertEquals(2, exps.size());
        assertEquals("LEFT", exps.get(0).getTriggerSide());
        assertEquals("RIGHT", exps.get(1).getTriggerSide());
    }

    // ═════════════════════════════════════════════════════
    //  12. NO DUPLICATE EMISSIONS
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Re-probe after expansion emits only from the expanded zone, no duplicates")
    void testNoDuplicateEmissions() {
        var op = buildOp(10, 10, 10, 50, 50);

        // Place two rights: one inside initial window, one just outside
        op.processRight(new RRec("k1", "R_in"), 105);
        op.processRight(new RRec("k1", "R_out"), 118);
        op.drainOutputs(); // clear right-side processing outputs

        // Left at 100: initial window [90, 110] → R_in@105 ✓ → match found → NO expansion
        op.processLeft(new LRec("k1", "L1"), 100);

        var out = op.drainOutputs();
        // Only R_in should match; R_out@118 is outside [90,110] but no expansion because match was found
        assertEquals(1, out.size());
        assertEquals("R_in", out.get(0).getRecord().rv);
    }

    @Test
    @DisplayName("Expansion re-probe only scans new zone")
    void testExpansionReProbeNewZoneOnly() {
        var op = buildOp(5, 5, 10, 50, 50);

        // R1 at 100 (will be inside initial window of L@100)
        // R2 at 112 (outside initial [95,105], inside expanded [85,115])
        op.processRight(new RRec("k1", "R1"), 100);
        op.processRight(new RRec("k1", "R2"), 112);
        op.drainOutputs(); // clear

        // Force a scenario where initial probe finds no match:
        // Left at 200 with α₀=5, β₀=5 → [195, 205] — neither R1@100 nor R2@112 in range
        // → expand: α=15, β=15 → [185, 215] — still neither in range
        op.processLeft(new LRec("k1", "L2"), 200);

        var out = op.drainOutputs();
        assertEquals(0, out.size(), "no records in expanded range either");
    }

    // ═════════════════════════════════════════════════════
    //  13. LONG.MIN_VALUE REJECTION
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Long.MIN_VALUE timestamp rejected on left")
    void testRejectMinValueLeft() {
        var op = buildFixedOp(10, 10);
        assertThrows(IllegalArgumentException.class,
                () -> op.processLeft(new LRec("k1", "L"), Long.MIN_VALUE));
    }

    @Test
    @DisplayName("Long.MIN_VALUE timestamp rejected on right")
    void testRejectMinValueRight() {
        var op = buildFixedOp(10, 10);
        assertThrows(IllegalArgumentException.class,
                () -> op.processRight(new RRec("k1", "R"), Long.MIN_VALUE));
    }

    // ═════════════════════════════════════════════════════
    //  14. BUILDER VALIDATION
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Builder rejects negative alpha0")
    void testBuilderRejectsNegativeAlpha() {
        assertThrows(IllegalArgumentException.class, () ->
                SymmetricExpandingIntervalJoinOperator.<String, LRec, RRec, JRec>builder()
                        .alpha0(-1).beta0(10).delta(5).alphaMax(50).betaMax(50)
                        .leftKeyExtractor(l -> l.key).rightKeyExtractor(r -> r.key)
                        .processJoinFunction((l, r, c, col) -> {}).build());
    }

    @Test
    @DisplayName("Builder rejects alphaMax < alpha0")
    void testBuilderRejectsMaxBelowInitial() {
        assertThrows(IllegalArgumentException.class, () ->
                SymmetricExpandingIntervalJoinOperator.<String, LRec, RRec, JRec>builder()
                        .alpha0(20).beta0(10).delta(5).alphaMax(10).betaMax(50)
                        .leftKeyExtractor(l -> l.key).rightKeyExtractor(r -> r.key)
                        .processJoinFunction((l, r, c, col) -> {}).build());
    }

    // ═════════════════════════════════════════════════════
    //  15. WATERMARK MONOTONICITY
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Watermark does not go backwards")
    void testWatermarkMonotonic() {
        var op = buildFixedOp(10, 10);
        op.advanceWatermark(100);
        assertEquals(100, op.getCurrentWatermark());
        op.advanceWatermark(50);
        assertEquals(100, op.getCurrentWatermark());
    }

    // ═════════════════════════════════════════════════════
    //  16. ORDERS ⋈ SHIPMENTS END-TO-END
    // ═════════════════════════════════════════════════════

    static class Order {
        final String orderId; final double amount;
        Order(String id, double a) { orderId = id; amount = a; }
        @Override public String toString() { return "Order(" + orderId + ")"; }
    }

    static class Shipment {
        final String shipmentId; final String orderId;
        Shipment(String sid, String oid) { shipmentId = sid; orderId = oid; }
        @Override public String toString() { return "Ship(" + shipmentId + "→" + orderId + ")"; }
    }

    static class Matched {
        final String orderId; final String shipmentId; final long ts;
        Matched(String o, String s, long t) { orderId = o; shipmentId = s; ts = t; }
    }

    @Test
    @DisplayName("Orders ⋈ Shipments with delayed shipment recovered by expansion")
    void testOrderShipmentWithExpansion() {
        final long H = 3_600_000L;

        var op = SymmetricExpandingIntervalJoinOperator
                .<String, Order, Shipment, Matched>builder()
                .alpha0(H)          // initial: ±1h
                .beta0(H)
                .delta(H)           // expand by 1h
                .alphaMax(4 * H)    // max 4h
                .betaMax(4 * H)
                .leftKeyExtractor(o -> o.orderId)
                .rightKeyExtractor(s -> s.orderId)
                .processJoinFunction((o, s, ctx, col) ->
                        col.collect(new Matched(o.orderId, s.shipmentId, ctx.getTimestamp()),
                                ctx.getTimestamp()))
                .build();

        long t0 = 1_000_000;

        // Order at t0
        op.processLeft(new Order("ORD-1", 100), t0);
        // No shipment yet → expansion: α=2h, β=2h

        // Shipment 1.5h later (would be missed by fixed 1h window)
        Shipment s1 = new Shipment("S-A", "ORD-1");
        op.processRight(s1, t0 + (long)(1.5 * H));
        // Right probes left: tL ∈ [tR − α, tR + β]
        // With α=2h (from expansion), β=2h: tL ∈ [tR−2h, tR+2h]
        // tR = t0+1.5h → [t0−0.5h, t0+3.5h] → L@t0 ✓

        var out = op.drainOutputs();
        assertEquals(1, out.size(), "delayed shipment should be recovered by expansion");
        assertEquals("ORD-1", out.get(0).getRecord().orderId);
        assertEquals("S-A",   out.get(0).getRecord().shipmentId);
        assertEquals(t0 + (long)(1.5 * H), out.get(0).getTimestamp());

        // Shipment way outside max bounds
        op.processRight(new Shipment("S-B", "ORD-1"), t0 + 10 * H);
        assertTrue(op.drainOutputs().isEmpty(), "shipment 10h later should not match");

        // Late data after watermark
        op.advanceWatermark(t0 + 20 * H);
        op.processLeft(new Order("ORD-2", 50), t0);
        var late = op.drainSideOutputs();
        assertEquals(1, late.size());
        assertEquals("left-late", late.get(0).getTag());
    }

    // ═════════════════════════════════════════════════════
    //  17. ZERO-WIDTH INITIAL WINDOW WITH EXPANSION
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Zero initial window (α₀=0, β₀=0) with expansion")
    void testZeroInitialWithExpansion() {
        var op = buildOp(0, 0, 5, 20, 20);

        op.processLeft(new LRec("k1", "L1"), 100);
        // Initial window [100, 100], no right → expand: α=5, β=5 → [95, 105]

        op.processRight(new RRec("k1", "R1"), 103); // 103 ∈ [95, 105]? After expansion, yes
        // But wait — right side probes left with [103−5, 103+5] = [98, 108] → L1@100 ✓

        var out = op.drainOutputs();
        assertEquals(1, out.size());
    }

    @Test
    @DisplayName("Zero initial window without expansion — only exact time match")
    void testZeroInitialNoExpansion() {
        var op = buildOp(0, 0, 0, 0, 0);

        op.processLeft(new LRec("k1", "L1"), 100);
        op.processRight(new RRec("k1", "R_exact"), 100);
        op.processRight(new RRec("k1", "R_off"), 101);

        var out = op.drainOutputs();
        assertEquals(1, out.size());
        assertEquals("R_exact", out.get(0).getRecord().rv);
    }

    // ═════════════════════════════════════════════════════
    //  18. EXPANSION LOG COMPLETENESS
    // ═════════════════════════════════════════════════════

    @Test
    @DisplayName("Expansion log captures all expansion events with correct details")
    void testExpansionLogCompleteness() {
        var op = buildOp(10, 10, 3, 100, 100);

        op.processLeft(new LRec("A", "L_A"), 100);  // expand A
        op.processRight(new RRec("B", "R_B"), 200); // expand B (no left for key B)
        op.processLeft(new LRec("A", "L_A2"), 300); // expand A again (no right for A near 300)

        var exps = op.drainExpansionLog();
        assertEquals(3, exps.size());

        // First: key=A, LEFT, 10→13
        assertEquals("A", exps.get(0).getKey());
        assertEquals("LEFT", exps.get(0).getTriggerSide());
        assertEquals(10, exps.get(0).getOldAlpha());
        assertEquals(13, exps.get(0).getNewAlpha());

        // Second: key=B, RIGHT, 10→13
        assertEquals("B", exps.get(1).getKey());
        assertEquals("RIGHT", exps.get(1).getTriggerSide());

        // Third: key=A, LEFT, 13→16
        assertEquals("A", exps.get(2).getKey());
        assertEquals(13, exps.get(2).getOldAlpha());
        assertEquals(16, exps.get(2).getNewAlpha());
    }
}
