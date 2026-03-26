# Ground Truth Benchmark — Thesis Evaluation Framework

## Overview

This benchmark implements the experimental methodology discussed with the thesis committee:

1. **Build ground truth first** — define correct (Order, Payment, Shipment) triples
2. **Generate input streams backward** — construct events that lead to ground truth
3. **Compare operators** — measure accuracy of each strategy against ground truth

## Research Question

> **What is the invariant of the integrity-aware window operator, and how does its accuracy compare against data-independent operators on streams with controlled integrity conflicts?**

## Key Design Decisions

### No 1-to-1 Assumption

As emphasized by the committee: **real systems are not bijective.**

- Multiple payments per order exist (retries, duplicates, partial payments)
- Multiple shipments per order exist (split deliveries, re-shipments)
- Removing the 1-to-1 assumption means **Cartesian products can appear**
- Expansion may introduce duplicates; shrinking may delete correct matches

### Window Operations Analyzed

You can only do **two things** to a window:

| Operation | Direction | Effect |
|-----------|-----------|--------|
| **Expand upper bound** | Forward | Catches late events, may introduce future conflicts |
| **Expand lower bound** | Backward | Catches early events, may introduce past conflicts |
| **Shrink upper bound** | Forward | Reduces window, may miss valid events |
| **Shrink lower bound** | Backward | Reduces window, may miss valid events |

### Invariant Comparison

| Operator | Invariant | Window Type |
|----------|-----------|-------------|
| **Sliding Window** | ∀w: size(w) = α + β | Data-independent |
| **Interval Join** | ∀t: W_t = [t - β, t + α] | Data-independent |
| **Elastic Validation** | Window adapts for integrity | Data-dependent |

The **critical shift**: moving from data-independent to data-dependent window semantics.

## Synthetic Dataset

### Generation Strategy

The dataset is generated **backward** (ground truth first):

1. Define correct output triples
2. Create order events at regular intervals
3. Create payment events (with controlled delays and duplicates)
4. Create shipment events (with controlled delays and multiplicities)
5. Inject negative cases (missing payment, missing shipment)

### Conflict Categories

| Category | Count | Description | Challenge |
|----------|-------|-------------|-----------|
| `NORMAL` | ~20 | All events within standard window | Baseline — all operators should match |
| `LATE_PAYMENT` | ~10 | Payment outside V, within V+G | Interval join misses, elastic catches |
| `LATE_SHIPMENT` | ~8 | Shipment near U boundary | Tests content window sensitivity |
| `DUPLICATE_PAYMENT` | ~10 | Multiple payments, one correct | Cartesian product risk |
| `MULTIPLE_SHIPMENTS` | ~8 | Multiple shipments per order | Each is valid — tests non-bijective handling |
| `COMPLEX` | ~6 | Duplicate payments + multiple shipments | Hardest: fixing one conflict introduces another |
| `NO_SHIPMENT` | ~5 | Payment exists, no shipment | Negative case — should NOT produce output |
| `NO_PAYMENT` | ~5 | Shipment exists, no payment | Negative case — should NOT produce output |

### Window Parameters

| Parameter | Symbol | Value | Description |
|-----------|--------|-------|-------------|
| Content Window | U | 100 | Shipment must arrive within [tO, tO+100] |
| Validation Window | V | 50 | Base payment window |
| Grace Period | G | 30 | Extra time for late payments |
| Validation Deadline | V+G | 80 | Payment must arrive within [tO, tO+80] |
| Interval Join α | α | 100 | Forward bound |
| Interval Join β | β | 0 | Backward bound |

## Evaluation Metrics

### Per-Operator Metrics

| Metric | Formula | Meaning |
|--------|---------|---------|
| **True Positives (TP)** | Operator match ∈ Ground Truth | Correct matches |
| **False Positives (FP)** | Operator match ∉ Ground Truth | Spurious matches |
| **False Negatives (FN)** | Ground Truth ∉ Operator output | Missed matches |
| **Duplicates** | Same match produced > 1 time | Redundant output |
| **Precision** | TP / (TP + FP) | How many outputs are correct |
| **Recall** | TP / (TP + FN) | How many correct outputs were found |
| **F1 Score** | 2·P·R / (P+R) | Harmonic mean of precision and recall |

### Per-Category Recall

Each conflict category is measured independently to understand **where** each operator fails.

### Conflict Direction Analysis

When expansion causes a conflict:

- **Past conflict:** Expanding backward introduces duplicate match (from an earlier payment)
- **Future conflict:** Expanding forward introduces extra joins (from a later shipment)

## Running the Benchmark

```bash
# From the polyflow root directory
mvn test -pl base -Dtest=benchmark.GroundTruthBenchmarkTest
```

### Output Files

Generated in `benchmark_dataset/`:

| File | Contents |
|------|----------|
| `orders.csv` | Order events (orderId, customerId, amount, t) |
| `payments.csv` | Payment events (paymentId, orderId, status, amount, t) |
| `shipments.csv` | Shipment events (shipmentId, orderId, carrier, t) |
| `ground_truth.csv` | Correct matches (orderId, paymentId, shipmentId, times, category) |
| `negative_cases.csv` | Orders that should NOT produce output |
| `dataset_metadata.csv` | Window parameters, event counts, category breakdown |
| `evaluation_results.csv` | Comparison results for all operators |

## The Hard Problem

As discussed with the committee:

> If expanding fixes one conflict but introduces another, what do you do?

**There is no universal optimal solution.** Instead, you **measure error** against ground truth. This is the core shift: you are NOT building a perfect algorithm — you are **comparing strategies**.

The `COMPLEX` category specifically tests this: duplicate payments AND multiple shipments coexist, so expanding to catch a late payment may introduce extra Cartesian product matches.

## Next Steps

1. ✅ Generate synthetic dataset with controlled conflicts
2. ✅ Define ground truth output
3. ✅ Run three operators and measure accuracy
4. 🔜 Integrate with actual polyflow operators (not just simulated)
5. 🔜 Add latency measurements
6. 🔜 Vary window parameters to find sensitivity curves
7. 🔜 Analyze convergence properties of elastic operator
