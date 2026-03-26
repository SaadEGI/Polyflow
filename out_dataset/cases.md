# Window Policy Comparison Summary

| Policy                   | Window Adaptation | Invariant Preserved | Main Idea                        | Risk | Strength |
|--------------------------|------------------|---------------------|----------------------------------|------|----------|
| Fixed Interval Join      | No change: [t-β, t+α] | Constant window size | Static time-based join           | False negatives (miss late matches) | Simple, deterministic |
| Expand (Upper/Lower)     | Increase α or β | Breaks equal-size invariant | Look further for missing matches | Introduces new conflicts | Recovers late events |
| Symmetric Expand         | Increase both α and β | Preserves equal-size invariant | Elastic but structured expansion | More state, more ambiguity | Clean theoretical extension |
| Shrink                   | Decrease α and/or β | Optional | Remove extra candidates          | False negatives | Reduces conflicts |
| Expand → Shrink          | Expand then minimize window |  | Search wide, then refine         | Higher complexity | Better accuracy balance |
| Past &Future Conflict Handling | Recompute or forget history |  | Handle old and new conflicts     | Retractions / inconsistency | Improves correctness |