# Elastic Windowing S2R Operator - Triangle Moving Windows

## Progress Summary

- Built **ElasticWindowingS2ROperator** implementing chained two-stream joins with elastic boundaries
- Window boundaries "move" based on payment arrival (triangle moving effect)
- Supports three modes: FIXED, ELASTIC, EXTENDED_FIXED for comparison
- Chain: Order→Payment (elastic validation), then Payment→Shipment (content window anchored on payment)

---

## Core Concept: Chained Two-Stream Joins

Instead of one fixed window anchored on order time, we decompose into **two stages**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CHAINED TWO-STREAM JOINS                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   STAGE 1: Order ↔ Payment (Elastic Validation)                            │
│   ══════════════════════════════════════════════                            │
│                                                                             │
│   Order arrives at tO                                                       │
│       │                                                                     │
│       ├──────[────── V ──────]──────▶  Base validation window               │
│       │                     │                                               │
│       ├──────[────── V ─────┼── G ──]──▶  Extended with grace               │
│       │                     │       │                                       │
│      tO                   tO+V    tO+V+G                                    │
│                                                                             │
│   Payment must arrive in [tO, tO+V+G] to validate order                     │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   STAGE 2: Payment ↔ Shipment (Content Window)                              │
│   ═════════════════════════════════════════════                             │
│                                                                             │
│   Payment validates at tP                                                   │
│       │                                                                     │
│       ├──────[────── U ──────]──────▶  Shipment window starts from tP!      │
│       │                     │                                               │
│      tP                   tP+U                                              │
│                                                                             │
│   Shipment must arrive in [tP, tP+U] (ELASTIC mode)                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## The Triangle Moving Effect

This is the key insight: **the shipment window moves with the payment timestamp**.

```
FIXED Window (baseline):
═══════════════════════

    Order─────────[────────────── U ──────────────]──────▶
         tO                                     tO+U
         
    Payment arrives late at tP (but still within V+G)
    Shipment arrives at tS
    
    If tS > tO+U → MISS (even though payment was valid!)


ELASTIC Window (triangle moving):
═════════════════════════════════

    Order─────────[── V+G ──]────────────────────────────▶
                           ↓ payment validates at tP
                      Payment────[────── U ──────]───────▶
                             tP                tP+U
    
    Shipment window MOVES with payment!
    If tP ≤ tS ≤ tP+U → MATCH (recovered!)


Visual "Triangle":
══════════════════

         tO        tP      tO+U    tP+U
          │         │        │       │
    ──────┼─────────┼────────┼───────┼─────────▶ Time
          │         │        │       │
          │◄──V+G──►│        │       │
          │         │◄───────U───────►│
          │         │        │       │
          └─────────┴────────┴───────┘
                    ▲        ▲
                    │        │
              Payment    Shipment
              arrives    arrives
              
    The window "triangle" extends as payment arrives later!
```

---

## Three Window Modes Compared

| Mode | Shipment Window | Formula | Use Case |
|------|-----------------|---------|----------|
| **FIXED** | Anchored on order | [tO, tO+U] | Baseline/strict |
| **ELASTIC** | Anchored on payment | [tP, tP+U] | Flexible/recommended |
| **EXTENDED_FIXED** | Anchored on order + extension | [tO, tO+U+ext] | Simple extension |

### Why ELASTIC is Better

```
Example: V=3, G=2, U=5

Order at t=10, Payment at t=14 (within V+G=5), Shipment at t=17

FIXED:        Shipment window = [10, 15]  → 17 > 15  → MISS ❌
ELASTIC:      Shipment window = [14, 19]  → 17 ≤ 19  → MATCH ✅
EXTENDED(+3): Shipment window = [10, 18]  → 17 ≤ 18  → MATCH ✅

ELASTIC naturally adapts to late payments without over-extending!
```

---

## Implementation Structure

```java
public class ElasticWindowingS2ROperator<I, K, W, R extends Iterable<?>>
        implements StreamToRelationOperator<I, W, R> {

    // Stage 1: Order ↔ Payment validation
    private void evaluateStage1(K key, KeyedChainState<I> state, long currentTime) {
        // For each payment, check if it validates any pending order
        // Payment must be in [tO, tO + V + G]
        // Creates ValidatedPair with appropriate shipment window end
    }

    // Stage 2: Payment ↔ Shipment content join
    private void evaluateStage2ForPair(K key, KeyedChainState<I> state, 
                                        ValidatedPair<I> pair, long currentTime) {
        // For each validated pair, check for matching shipments
        // Shipment must be in [shipmentWindowStart, shipmentWindowEnd]
        // Window boundaries depend on mode!
    }

    // Key method: calculate shipment window based on mode
    private long calculateShipmentWindowEnd(long orderTime, long paymentTime) {
        switch (windowMode) {
            case FIXED:
                return orderTime + contentWindow;        // [tO, tO+U]
            case ELASTIC:
                return paymentTime + contentWindow;      // [tP, tP+U] ← Triangle!
            case EXTENDED_FIXED:
                return orderTime + contentWindow + ext;  // [tO, tO+U+ext]
        }
    }
}
```

---

## Finalization States

```
                    ┌─────────────────┐
                    │     ORDER       │
                    │    RECEIVED     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    PENDING      │
                    │  (Stage 1)      │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    Payment arrives    Time > tO+V+G     Payment arrives
    in [tO, tO+V+G]    (no payment)      but FAILED
           │                 │                 │
           ▼                 ▼                 ▼
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │  VALIDATED  │   │   INVALID   │   │   INVALID   │
    │  (Stage 2)  │   │ (no output) │   │ (no output) │
    └──────┬──────┘   └─────────────┘   └─────────────┘
           │
           ▼
    ┌─────────────────┐
    │    PENDING      │
    │  (Stage 2)      │
    └────────┬────────┘
             │
    ┌────────┴────────┐
    │                 │
    ▼                 ▼
Shipment in       Time > window end
[start, end]      (no shipment)
    │                 │
    ▼                 ▼
┌─────────────┐ ┌─────────────┐
│    VALID    │ │ NO_CONTENT  │
│ (emit result│ │ (no output) │
└─────────────┘ └─────────────┘
```

---

## Benefits of Chained Approach

| Benefit | Explanation |
|---------|-------------|
| **Composable** | Two-stream joins are easier to reason about |
| **Generalizable** | Can extend to N-stream chains |
| **Flexible** | Late payment "shifts" shipment window |
| **Natural** | Matches business logic (shipment follows payment) |
| **Recoverable** | More matches with same data |

---

## Comparison with Fixed Windows

### What You Miss with Fixed Windows

```
Scenario: Late payment but timely shipment (relative to payment)

Timeline:
t=10: Order O1 arrives
t=14: Payment P1 arrives (within V+G=5 ✓)
t=17: Shipment S1 arrives

FIXED (U=5):
  - Payment valid: 14 ∈ [10, 15] ✓
  - Shipment window: [10, 15]
  - Shipment check: 17 ∈ [10, 15]? NO ❌
  - Result: NO_CONTENT (missed!)

ELASTIC (U=5):
  - Payment valid: 14 ∈ [10, 15] ✓
  - Shipment window: [14, 19] (moves with payment!)
  - Shipment check: 17 ∈ [14, 19]? YES ✓
  - Result: VALID (recovered!)
```

### When to Use Which Mode

| Use FIXED when... | Use ELASTIC when... | Use EXTENDED_FIXED when... |
|-------------------|---------------------|----------------------------|
| Strict SLAs | Business flexibility | Simple extension needed |
| Deterministic output | Late payments expected | Backward compatibility |
| Baseline comparison | Shipment follows payment | Gradual migration |

---

## Usage Example

```java
// Create ELASTIC operator (recommended)
var elasticOp = new ElasticWindowingS2ROperator<>(
    Tick.TIME_DRIVEN, time, "elastic",
    contentFactory, report,
    3,   // V: validation window
    2,   // G: grace period  
    5,   // U: content window
    0,   // extension (not used in ELASTIC)
    WindowMode.ELASTIC,
    event -> event.joinKey,
    event -> event.timestamp,
    event -> categorize(event),
    (order, tO, payment, tP, shipment, tS, out) -> 
        createResult(order, payment, shipment, out)
);

// Process events
elasticOp.compute(orderEvent, 10);
elasticOp.compute(paymentEvent, 14);  // Stage 1 validates
elasticOp.compute(shipmentEvent, 17); // Stage 2 matches!

// Get results
Map<Window, Content> results = elasticOp.getFinalizedWindows();
// → Contains validated result for O1!
```

---

## Key Insight

> **"The elastic windowing operator treats late payments as opportunities, not failures. By anchoring the shipment window on the payment timestamp, we recover matches that fixed windows would miss."**

This is the **triangle moving** effect: as payment arrives later (within grace), the shipment window extends further into the future, creating a triangle shape when visualized over time.
