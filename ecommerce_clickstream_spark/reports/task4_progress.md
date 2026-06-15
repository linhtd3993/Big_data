# Task 4 Progress Report

## Scope

Task 4 implements an end-to-end implicit recommendation pipeline with Spark ALS.

## Interaction Design

- `page_view = 1`
- `add_to_cart = 3`
- `purchase = 5 x quantity`

Clickstream interactions are joined through sessions. Purchase interactions are
joined through orders and order items.

## Train And Evaluation

- Per-user temporal 80/20 holdout.
- Implicit ALS with non-negative factors.
- Popularity baseline.
- Seen-item filtering.
- Top-10 recommendations.
- Precision@10, Recall@10, MAP@10, and NDCG@10.

## Results

| Model | Precision@10 | Recall@10 | MAP@10 | NDCG@10 |
|---|---:|---:|---:|---:|
| ALS | 0.007093 | 0.012564 | 0.003758 | 0.010006 |
| Popularity | 0.007600 | 0.013409 | 0.003993 | 0.010622 |

The popularity baseline slightly outperforms the current ALS configuration.
This is a valid experimental result and indicates strong popularity bias or a
need for further ALS tuning and richer sequential/context features.

## Output

```text
data/output/task4_recommendation/
```

The output includes train/test data, interactions, ALS model, recommendations,
evaluation metrics, validation results, and SVG charts.
