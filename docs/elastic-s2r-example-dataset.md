# Elastic S2R Validation — Example & Dataset


> **"An order can produce output only if it is paid within V+G (validation window + grace), and matched to a shipment within U (content window)."**

### Domain Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           STREAM DEFINITIONS                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Stream A: ORDERS              Stream C: PAYMENTS                      │
│   ┌──────────────────┐          ┌──────────────────────────┐           │
│   │ orderId (PK)     │          │ paymentId (PK)           │           │
│   │ customerId       │          │ orderId (FK → Orders)    │           │
│   │ amount           │          │ status (PAID/FAILED)     │           │
│   │ t (event time)   │          │ t (event time)           │           │
│   └──────────────────┘          └──────────────────────────┘           │
│                                                                         │
│   Stream B: SHIPMENTS                                                   │
│   ┌──────────────────────────┐                                         │
│   │ shipmentId (PK)          │                                         │
│   │ orderId (FK → Orders)    │                                         │
│   │ carrier                  │                                         │
│   │ t (event time)           │                                         │
│   └──────────────────────────┘                                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Window Parameters

| Parameter | Symbol | Value | Description |
|-----------|--------|-------|-------------|
| Content Window | U | 100 | Shipment must arrive within [tO, tO+100] |
| Validation Window | V | 50 | Base payment window |
| Grace Period | G | 30 | Extra time for late payments |
| **Validation Deadline** | V+G | 80 | Payment must arrive within [tO, tO+80] |

---

## Input Tables 

### Table A: Orders (10 rows)

| orderId | customerId | amount | t (event time) | Notes                           |
|---------|------------|--------|----------------|---------------------------------|
| O1 | C1 | 100.00 | 0 | Valid path       |
| O2 | C2 | 200.00 | 10 | Payment inside grace period     |
| O3 | C3 | 150.00 | 20 | Payment too late (past grace)   |
| O4 | C4 | 300.00 | 30 | Shipment outside content window |
| O5 | C5 | 250.00 | 40 | Missing shipment entirely       |
| O6 | C6 | 175.00 | 50 | Missing payment entirely        |
| O7 | C7 | 125.00 | 60 | Multiple shipments              |
| O8 | C8 | 400.00 | 70 | Duplicate payments (same order) |
| O9 | C9 | 350.00 | 80 | FAILED payment status           |
| O10 | C10 | 500.00 | 90 | Payment before order arrives    |

### Table C: Payments (12 rows)
An order is valid if ∃ at least one PAID payment in the validation window.

| paymentId | orderId | status | t (event time) | Δt from order | Within V+G? |
|-----------|---------|--------|----------------|---------------|-------------|
| P1 | O1 | PAID | 30 | 30 | ✅ Yes (30 ≤ 80) |
| P2 | O2 | PAID | 75 | 65 | ✅ Yes (65 ≤ 80, in grace) |
| P3 | O3 | PAID | 120 | 100 | ❌ No (100 > 80) |
| P4 | O4 | PAID | 50 | 20 | ✅ Yes (20 ≤ 80) |
| P5 | O5 | PAID | 60 | 20 | ✅ Yes (20 ≤ 80) |
| — | O6 | — | — | — | ❌ Missing |
| P7 | O7 | PAID | 90 | 30 | ✅ Yes (30 ≤ 80) |
| P8a | O8 | PAID | 100 | 30 | ✅ Yes (30 ≤ 80) |
| P8b | O8 | PAID | 120 | 50 | ✅ Yes (duplicate!) |
| P9 | O9 | FAILED | 110 | 30 | ❌ No (FAILED ≠ PAID) |
| P10 | O10 | PAID | 85 | -5 | ❌ No (before order) |
| P10b | O10 | PAID | 140 | 50 | ✅ Yes (50 ≤ 80) |

### Table B: Shipments (12 rows)

| shipmentId | orderId | carrier | t (event time) | Δt from order | Within U? |
|------------|---------|---------|----------------|---------------|-----------|
| S1 | O1 | FedEx | 50 | 50 | ✅ Yes (50 ≤ 100) |
| S2 | O2 | UPS | 80 | 70 | ✅ Yes (70 ≤ 100) |
| S3 | O3 | DHL | 90 | 70 | ✅ Yes (70 ≤ 100) |
| S4 | O4 | FedEx | 180 | 150 | ❌ No (150 > 100) |
| — | O5 | — | — | — | ❌ Missing |
| S6 | O6 | UPS | 100 | 50 | ✅ Yes (50 ≤ 100) |
| S7a | O7 | FedEx | 100 | 40 | ✅ Yes (40 ≤ 100) |
| S7b | O7 | DHL | 130 | 70 | ✅ Yes (70 ≤ 100) |
| S8 | O8 | UPS | 120 | 50 | ✅ Yes (50 ≤ 100) |
| S9 | O9 | FedEx | 130 | 50 | ✅ Yes (50 ≤ 100) |
| S10 | O10 | DHL | 150 | 60 | ✅ Yes (60 ≤ 100) |

---

## Output Tables (Different Window Policies)

### Output Table 1: Strict Policy (V=50, G=30, U=100)

**Rule:** Payment within [tO, tO+80], Shipment within [tO, tO+100]

| orderId | shipmentId | paymentId | Order t | Payment t | Shipment t | Status | Reason |
|---------|------------|-----------|---------|-----------|------------|--------|--------|
| O1 | S1 | P1 | 0 | 30 | 50 | ✅ VALID | All conditions met |
| O2 | S2 | P2 | 10 | 75 | 80 | ✅ VALID | Payment in grace period |
| O3 | — | P3 | 20 | 120 | 90 | ❌ INVALID | Payment too late (100 > 80) |
| O4 | S4 | P4 | 30 | 50 | 180 | ❌ NO_CONTENT | Shipment too late (150 > 100) |
| O5 | — | P5 | 40 | 60 | — | ❌ NO_CONTENT | No shipment |
| O6 | S6 | — | 50 | — | 100 | ❌ INVALID | No payment |
| O7 | S7a | P7 | 60 | 90 | 100 | ✅ VALID | First shipment |
| O7 | S7b | P7 | 60 | 90 | 130 | ✅ VALID | Second shipment |
| O8 | S8 | P8a | 70 | 100 | 120 | ✅ VALID | First payment used |
| O9 | — | P9 | 80 | 110 | 130 | ❌ INVALID | Payment FAILED |
| O10 | S10 | P10b | 90 | 140 | 150 | ✅ VALID | Second payment valid |

**Summary:** 6 valid outputs (O1, O2, O7×2, O8, O10), 5 rejected

### Output Table 2: Extended Validation Window (V=80, G=30, U=100)

**Rule:** Payment within [tO, tO+110], Shipment within [tO, tO+100]

| orderId | shipmentId | paymentId | Status | Change from Strict? |
|---------|------------|-----------|--------|---------------------|
| O1 | S1 | P1 | ✅ VALID | Same |
| O2 | S2 | P2 | ✅ VALID | Same |
| O3 | S3 | P3 | ✅ VALID | **NOW VALID** (100 ≤ 110) |
| O4 | S4 | P4 | ❌ NO_CONTENT | Same (shipment still late) |
| O5 | — | P5 | ❌ NO_CONTENT | Same (no shipment) |
| O6 | S6 | — | ❌ INVALID | Same (no payment) |
| O7 | S7a, S7b | P7 | ✅ VALID ×2 | Same |
| O8 | S8 | P8a | ✅ VALID | Same |
| O9 | — | P9 | ❌ INVALID | Same (FAILED status) |
| O10 | S10 | P10b | ✅ VALID | Same |

**Summary:** 7 valid outputs (+1 from O3), 4 rejected

### Output Table 3: Triangle Moving Window (WIP)


| orderId | shipmentId | paymentId | Status | Change from Strict? |
|---------|------------|-----------|--------|---------------------|



---

## Step 4: Semantics Slide

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ELASTIC VALIDATION SEMANTICS                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ORDER arrives at time tO                                                  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                                                                     │  │
│   │    VALIDATION CONDITION (Stream C: Payments)                        │  │
│   │    ════════════════════════════════════════                         │  │
│   │    ∃ Payment P where:                                               │  │
│   │      • P.orderId = Order.orderId                                    │  │
│   │      • P.status = "PAID"                                            │  │
│   │      • tO ≤ P.t ≤ tO + V + G                                        │  │
│   │                                                                     │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                              ∧ (AND)                                        │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                                                                     │  │
│   │    CONTENT CONDITION (Stream B: Shipments)                          │  │
│   │    ═══════════════════════════════════════                          │  │
│   │    ∃ Shipment S where:                                              │  │
│   │      • S.orderId = Order.orderId                                    │  │
│   │      • tO ≤ S.t ≤ tO + U                                            │  │
│   │                                                                     │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                              ↓                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                                                                     │  │
│   │    OUTPUT: ValidatedOrderShipment(Order, Shipment, Payment)         │  │
│   │    ═══════════════════════════════════════════════════════          │  │
│   │    Emitted ONLY when BOTH conditions are satisfied                  │  │
│   │    One output per (Order, Shipment) pair if multiple shipments      │  │
│   │                                                                     │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   FINALIZATION:                                                             │
│   • VALID: Both conditions met → emit output                                │
│   • INVALID: No valid payment by tO + V + G → no output, finalized          │
│   • NO_CONTENT: Valid payment but no shipment by tO + U → no output         │
│   • PENDING: Waiting for payment or shipment → keep in state                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
