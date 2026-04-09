# Fraud Detection System
### Databricks SDP · Azure Event Hubs · ADLS Gen 2 · Streaming + Batch

---

## Overview

A real-time fraud detection pipeline built on Databricks Structured Data Pipelines (SDP). The system unifies 500K+ historical transactions from Azure Data Lake Storage Gen 2 with a continuous live stream from Azure Event Hubs — both flowing into a single unified table that powers fraud scoring, behavioral feature engineering, and an analytics-ready star schema.

---

## Stack

| Component | Technology |
|---|---|
| Batch storage | Azure Data Lake Storage Gen 2 |
| Real-time ingestion | Azure Event Hubs |
| Pipeline engine | Databricks Structured Data Pipelines (SDP) |
| Table format | Delta Lake |
| Streaming simulator | Python · Azure Event Hubs SDK |

---

## Pipeline Architecture

The pipeline is a directed acyclic graph with two independent roots — one batch, one streaming — that converge at `all_transactions` before fanning out to all downstream outputs.

```
AZURE ADLS GEN 2 (500K+ records)               AZURE EVENT HUBS
         │                                             │
         ▼                                             │  Python event producer
  customers_all (View)                                 ▼
         │                              streaming_transactions [source]  ──  1s
   ┌─────┼──────┬────────┐                             │
   ▼     ▼      ▼        ▼                             ▼
dim_   dim_   dim_     dim_            streaming_transactions [silver]  ──  2s
age_  card_  customer_ customers        schema · dedup · type cast
bands types  type   5K  4   5K                         │
 4s    3s     3s    4s                                  │
                                                        ▼
                                              all_transactions  ──  13s
                                               (unified stream)
                            ┌────────────────────┼──────────────┬──────────────┐
                            ▼                    ▼              ▼              ▼
                     dim_amount_band          dim_time   fact_customer_    user_
                       551K · 3s           551K · 3s      daily 235K·2s  locations
                                                                           live·2s
                                                                              │
                                                              user_transactions_behavior
                                                                        live · 3s
                                                                              │
                                                              fact_fraud_detection
                                                                        live · 1s
```

---

## Streaming Simulation

Production streaming was simulated using a custom Python-based event producer built on the Azure Event Hubs SDK. It publishes realistic transaction events — including edge cases likely to surface fraud signals — directly to Event Hubs at configurable rates.

This meant the full pipeline from bronze ingestion through `fact_fraud_detection` could be validated end-to-end without a live payment feed, under conditions that closely mirror production behaviour.

---

## Data Layers

### Bronze

Two independent ingestion paths, both landing raw data with no transformations applied.

- **Batch:** 500K+ historical transaction records read from ADLS Gen 2 into `customers_all` as a View
- **Streaming:** Event Hubs consumer writing to `streaming_transactions` (source) — latency: 1s

### Silver

Schema enforcement, type casting, and deduplication applied to the streaming path. The batch source (`customers_all`) is already clean and feeds dimension tables directly.

- `streaming_transactions` (silver) — latency: 2s

### Gold

All analytical outputs derive from `all_transactions`, the central unified streaming table that merges both paths.

**Dimension tables** — materialized views from `customers_all`:

| Table | Records | Latency |
|---|---|---|
| `dim_age_bands` | 5K | 4s |
| `dim_card_types` | 4 | 3s |
| `dim_customer_type` | 2 | 3s |
| `dim_customers` | 5K | 4s |
| `dim_amount_band` | 551K | 3s |
| `dim_time` | 551K | 3s |

**Fact tables and features** — all downstream from `all_transactions`:

| Table | Type | Records | Latency |
|---|---|---|---|
| `fact_customer_daily` | Materialized view | 235K | 2s |
| `user_locations` | Streaming table | live | 2s |
| `user_transactions_behavior` | Streaming table | live | 3s |
| `fact_fraud_detection` | Streaming table | live | 1s |

---

## Key Design Decisions

### Single convergence point

`all_transactions` is the central node in the DAG. Batch history from ADLS Gen 2 and live events from Event Hubs are merged here before any feature computation or scoring occurs. This guarantees that backfilling historical data automatically enriches behavioral features, and eliminates any divergence between offline and online processing logic.

### Lag-based feature engineering

All features that feed `fact_fraud_detection` are computed in `user_transactions_behavior` using only data that existed at least 10 minutes before the current transaction timestamp:

```python
WHERE event_timestamp < current_timestamp() - INTERVAL 10 MINUTES
```

This prevents data leakage — one of the most common and damaging failure modes in streaming ML systems, where features inadvertently encode future information and produce artificially strong offline metrics that collapse in production. The constraint is enforced at the query level, not in application logic, so it cannot be bypassed downstream.

### Streaming-first gold layer

`user_locations`, `user_transactions_behavior`, and `fact_fraud_detection` are streaming tables rather than materialized views. Fraud scores are continuously updated as new events arrive. From silver ingestion to final score, end-to-end latency is approximately 6 seconds (2s silver + 3s behavior + 1s scoring).

### Dimension isolation from the streaming path

The six customer dimension tables derive exclusively from `customers_all`, the batch View over ADLS Gen 2. These change slowly and do not need streaming semantics. Keeping them on the batch path reduces load on the streaming tables and avoids redundant recomputation on every micro-batch.

---

## Project Structure

```
├── bronze/
│   ├── batch_ingest.py                   # ADLS Gen 2 → customers_all (View)
│   └── streaming_ingest.py               # Event Hubs → streaming_transactions [source]
├── silver/
│   └── streaming_transactions.py         # Schema enforcement, deduplication
├── gold/
│   ├── all_transactions.py               # Unified batch + streaming table (13s)
│   ├── dimensions/
│   │   ├── dim_age_bands.py
│   │   ├── dim_card_types.py
│   │   ├── dim_customer_type.py
│   │   ├── dim_customers.py
│   │   ├── dim_amount_band.py
│   │   └── dim_time.py
│   ├── fact_customer_daily.py
│   ├── user_locations.py
│   ├── user_transactions_behavior.py     # Lag-safe behavioral features
│   └── fact_fraud_detection.py           # Real-time fraud scoring (1s)
├── simulation/
│   └── event_hub_producer.py             # Python-based Event Hubs simulator
└── pipeline.yml                          # Databricks SDP pipeline definition
```

---

## Correctness Guarantees

| Risk | Mitigation |
|---|---|
| Data leakage in features | 10-minute lag window enforced at query level in `user_transactions_behavior` |
| Duplicate events from Event Hubs | Deduplication at silver layer on transaction ID |
| Schema drift from streaming source | Explicit schema enforcement at bronze → silver boundary |
| Training / serving skew | Unified feature logic across batch backfill and live scoring via `all_transactions` |