# Elastic S2R Validation Operator

## Overview

The **ElasticValidationS2ROperator** implements a "One Elastic S2R" pattern that accumulates events from three streams (Orders, Shipments, Payments) into a single windowed relation per key.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Elastic Validation S2R Pipeline                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Stream A          Stream B           Stream C                              │
│   (Orders)         (Shipments)        (Payments)                             │
│      │                 │                  │                                  │
│      │                 │                  │                                  │
│      ▼                 ▼                  ▼                                  │
│   ┌─────────────────────────────────────────────────────────────┐           │
│   │              UnifiedEvent Wrapper                            │           │
│   │         (Normalizes all three event types)                   │           │
│   └─────────────────────────────────────────────────────────────┘           │
│                              │                                               │
│                              ▼                                               │
│   ┌─────────────────────────────────────────────────────────────┐           │
│   │           ElasticValidationS2ROperator                       │           │
│   │  ┌───────────────────────────────────────────────────────┐  │           │
│   │  │              State per Key (orderId)                  │  │           │
│   │  │  ┌─────────┐  ┌──────────┐  ┌──────────┐             │  │           │
│   │  │  │ Orders  │  │ Shipments│  │ Payments │             │  │           │
│   │  │  └─────────┘  └──────────┘  └──────────┘             │  │           │
│   │  └───────────────────────────────────────────────────────┘  │           │
│   │                         │                                    │           │
│   │                         ▼                                    │           │
│   │  ┌───────────────────────────────────────────────────────┐  │           │
│   │  │           Elastic Validation Rules (R2R)              │  │           │
│   │  │  • Payment within [tO, tO+V+G]?                       │  │           │
│   │  │  • Shipment within [tO, tO+U]?                        │  │           │
│   │  │  • Apply finalization logic                           │  │           │
│   │  └───────────────────────────────────────────────────────┘  │           │
│   └─────────────────────────────────────────────────────────────┘           │
│                              │                                               │
│                              ▼                                               │
│   ┌─────────────────────────────────────────────────────────────┐           │
│   │              ValidatedOrderShipment (Output)                 │           │
│   │         (Only emitted when order is finalized as VALID)      │           │
│   └─────────────────────────────────────────────────────────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Window Parameters

```
                  Order arrives at tO
                         │
    ─────────────────────┼─────────────────────────────────────────▶ Time
                         │
                         │◄──────── U (Content Window) ────────►│
                         │          Shipments must arrive        │
                         │          within this window           │
                         │                                       │
                         │◄────── V (Validation)              ──►│◄── G ──►│
                         │                                       │   Grace │
                         │     Payment must arrive within V+G    │
                         │                                       │
                         │                          Validation   │
                         │                          Deadline     │
                         │                                      ▼│
    ─────────────────────┼───────────────────────────────────────┼────────┼───▶
                        tO                                      tO+V      tO+V+G
```

### Parameters Explained

| Parameter | Symbol | Description                                         |
|-----------|--------|-----------------------------------------------------|
| Content Window | `U` | Maximum time for shipment after order (e.g., 100ms) |
| Validation Window | `V` | Validation window for payment (e.g., 100ms)         |
| Grace Period | `G` | Additional grace for late arrivals (e.g., 30ms)     |                 |

## Finalization States

```
                    ┌─────────────────┐
                    │     ORDER       │
                    │    RECEIVED     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    PENDING      │◄────────────────────┐
                    │ (waiting for    │                     │
                    │  payment and/or │                     │
                    │  shipment)      │                     │
                    └────────┬────────┘                     │
                             │                              │
           ┌─────────────────┼─────────────────┐            │
           │                 │                 │            │
           ▼                 ▼                 ▼            │
   Payment arrives?    Time > tO+V+G?    Payment exists     │
   AND Shipment         (No payment)     but Time > tO+U?   │
   within window?                        (No shipment)      │
           │                 │                 │            │
           ▼                 ▼                 ▼            │
   ┌───────────────┐ ┌───────────────┐ ┌───────────────┐   │
   │    VALID      │ │   INVALID     │ │  NO_CONTENT   │   │
   │               │ │               │ │               │   │
   │ Emit results  │ │ No output     │ │ No output     │   │
   │ (Order +      │ │ (validation   │ │ (content      │   │
   │  Shipments)   │ │  failed)      │ │  window       │   │
   │               │ │               │ │  expired)     │   │
   └───────────────┘ └───────────────┘ └───────────────┘   │
                   
```


### 1. Single Window per Key
Instead of separate windows for each stream, we maintain a single state bucket per key (orderId) that stores:
- All orders for that key
- All shipments for that key  
- All payments for that key



### 2. Finalization Semantics
Orders are finalized exactly once with one of four outcomes:
- **VALID**: Payment + Shipment(s) → Emit `ValidatedOrderShipment`
- **INVALID**: No payment within deadline → No output
- **NO_CONTENT**: Payment exists but no shipments → No output
- **PENDING**: Still waiting 



## Usage Example

```java
// Create the operator
ElasticValidationS2ROperator<UnifiedEvent, String, UnifiedEvent, List<UnifiedEvent>> operator =
    new ElasticValidationS2ROperator<>(
        Tick.TIME_DRIVEN,
        time,
        "elastic-validation",
        contentFactory,
        report,
        100,  // U: content window (shipment must arrive within 100ms)
        100,   // V: validation window
        30,   // G: grace period (payment can arrive up to 80ms after order)
        event -> event.joinKey,           // Key extractor
        event -> event.timestamp,         // Timestamp extractor
        event -> categorize(event),       // Category extractor
        (order, shipment, payment, t) ->  // Result factory
            createValidatedResult(order, shipment, payment, t)
    );

// Process events
operator.compute(UnifiedEvent.fromOrder(order), 0);
operator.compute(UnifiedEvent.fromPayment(payment), 30);
operator.compute(UnifiedEvent.fromShipment(shipment), 50);

// Get finalized results
Map<Window, Content> results = operator.getFinalizedWindows();
```

## Companion R2R Operator

The `ElasticValidatedJoinR2R` provides the same validation logic as a pure R2R operator:

```java
ElasticValidatedJoinR2R r2r = new ElasticValidatedJoinR2R(
    100,  // U: content window
    50,   // V: validation window  
    30,   // G: grace period
    Collections.singletonList("events"),
    "validated-results"
);

// Set current evaluation time
r2r.setCurrentTime(currentTime);

// Evaluate relation containing orders, shipments, payments
List<UnifiedEvent> results = r2r.eval(Collections.singletonList(allEvents));
```

## Test Scenarios

The implementation includes comprehensive tests for:

| Scenario | Expected Outcome |
|----------|------------------|
| Order + Payment + Shipment (all within windows) | VALID → Emit result |
| Order + Shipment (no payment, within deadline) | PENDING → No output yet |
| Order + Shipment (no payment, past deadline) | INVALID → No output |
| Order + Payment (no shipment, within window) | PENDING → No output yet |
| Order + Payment (no shipment, past window) | NO_CONTENT → No output |
| Late payment (outside V+G) | INVALID → No output |

