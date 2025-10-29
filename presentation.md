# Data Model V3

## Introduction

The goal of this project is to simplify the table schema by replacing the mapped columns with primitive columns. This improves compression which reduces storage and also improves query performance. 

It also adds a view layer on top to provide semantically meaningful column names with minimal overhead.

## 1. Correctness

**All validations passed** - [Full Report](output/benchmarks/validation_summary.md)

- **9/9 metrics validated** (Intigral, SlingTV, NFL)
- **0 differences** in aggregation results between old and new schema
- Tested: success counts, duration metrics, app_startup_time

## 2. Storage Improvements

There are two target tables with the difference between them being the sort key

The first sort key is `(customerId, clientId, sessionId, timestampMs)`

The second sort key is `(customerId, toStartOfInterval(timestampMs, toIntervalHour(1)), clientId, sessionId, timestampMs)`. This is referred to as rui sort key below.

### Overall Table Size

| Schema            | Compressed | Reduction |
|-------------------|------------|-----------|
| Map Columns       | 47.83 GiB  | -         |
| Primitive Columns | 2.23 GiB   | **95.3%** |
| Primitive Columns (rui sort key) | 3.41 GiB   | **92.9%** |

### Metric Columns Only

| Schema | Compressed | Reduction |
|--------|------------|-----------|
| metricIntGroup* (Map) | 2.93 GiB | - |
| int* (Primitive) | 281 MiB | **90.6%** |
| int* (Primitive) (rui sort key) | 262 MiB | **91.3%** |

### Non Metric Columns Only

| table_type        | compressed_bytes   | compressed_size | Reduction |
|-------------------|--------------------|-----------------|-----------|
| source_non_metric | 48205157873        | 44.89 GiB       | -         |
| target_non_metric | 2099628533         | 1.96 GiB        | **95.6%** |
| target_non_metric (rui sort key) | 3389897513         | 3.16 GiB        | **93.0%** |

### Why the Improvement?

**Map columns:**
- Store key strings for every value (e.g., "f_8458_9478_success")
- Map structure overhead

**Primitive columns:**
- 4 bytes per Int32 value
- Simple arrays

**Additional savings:**
- Row reduction: 654.5M → 590M rows (9.9% from pre-aggregation)

## 3. Query Performance: Aggregation

### 3A. Target: eco_cross_page_preagg_pt1m_3cust (24h table)

**Overall:** ~7.58x faster - [Full Report](/output_24h_window/benchmarks/aggregation_benchmark_results.md)
  (calc: Old schema average / New schema average = 0.500 / 0.066 = 7.58x)

#### Per-Customer Results

| Customer | Metrics | Avg Speedup | Min | Max |
|----------|---------|-------------|-----|-----|
| SlingTV | 4 | **6.19x** | 3.86x | 10.92x |
| NFL | 4 | **3.77x** | 2.82x | 5.85x |
| Intigral | 4 | **1.61x** | 1.02x | 3.32x |

#### Detailed Metrics

| Customer | Metric | Old (Map) | New (Primitive) | Speedup |
|----------|--------|-----------|-----------------|---------|
| SlingTV | event_count | 1.963s | 0.180s | 10.92x |
| SlingTV | app_startup_time | 0.738s | 0.122s | 6.06x |
| SlingTV | f_202_363_total_duration | 0.412s | 0.105s | 3.93x |
| SlingTV | f_202_363_success | 0.408s | 0.106s | 3.86x |
| NFL | event_count | 0.102s | 0.017s | 5.85x |
| NFL | f_183_7200_success | 0.045s | 0.014s | 3.23x |
| NFL | f_183_7200_total_duration | 0.045s | 0.014s | 3.17x |
| NFL | app_startup_time | 0.050s | 0.018s | 2.82x |
| Intigral | event_count | 0.118s | 0.036s | 3.32x |
| Intigral | f_8458_9478_success | 0.016s | 0.015s | 1.05x |
| Intigral | f_8458_9478_total_duration | 0.016s | 0.015s | 1.04x |
| Intigral | app_startup_time | 0.037s | 0.037s | 1.02x |

Queries used for benchmarking can be found [here](/output_24h_window/benchmarks/query_index.json)

### 3B. Target: rui_eco_cross_page_preagg_pt1m_1h (1h table)

**Overall:** ~5.69x faster - [Full Report](/output_1h_window/benchmarks/aggregation_benchmark_results.md)
  (calc: Old schema average / New schema average = 0.501 / 0.088 = 5.69x)

#### Per-Customer Results

| Customer | Metrics | Avg Speedup | Min | Max |
|----------|---------|-------------|-----|-----|
| SlingTV | 4 | **4.16x** | 2.67x | 7.66x |
| NFL | 4 | **2.93x** | 2.19x | 4.40x |
| Intigral | 4 | **2.13x** | 1.19x | 4.49x |

#### Detailed Metrics

| Customer | Metric | Old (Map) | New (Primitive) | Speedup |
|----------|--------|-----------|-----------------|---------|
| SlingTV | event_count | 1.964s | 0.256s | 7.66x |
| SlingTV | app_startup_time | 0.736s | 0.204s | 3.61x |
| SlingTV | f_202_363_success | 0.411s | 0.152s | 2.71x |
| SlingTV | f_202_363_total_duration | 0.415s | 0.156s | 2.67x |
| NFL | event_count | 0.103s | 0.023s | 4.40x |
| NFL | f_183_7200_success | 0.045s | 0.017s | 2.61x |
| NFL | f_183_7200_total_duration | 0.045s | 0.018s | 2.51x |
| NFL | app_startup_time | 0.050s | 0.023s | 2.19x |
| Intigral | event_count | 0.118s | 0.026s | 4.49x |
| Intigral | app_startup_time | 0.038s | 0.024s | 1.61x |
| Intigral | f_8458_9478_total_duration | 0.016s | 0.013s | 1.24x |
| Intigral | f_8458_9478_success | 0.016s | 0.013s | 1.19x |

Queries used for benchmarking can be found [here](/output_1h_window/benchmarks/query_index.json)

## 4. View Overhead: Readable Names vs Direct Access

**Average overhead:** 3.5% - [Full Report](/output/view_benchmarks/view_benchmark_results.md)

### Per-Customer Overhead

| Customer | Metrics | Avg Direct | Avg View | Overhead |
|----------|---------|------------|----------|----------|
| Intigral | 3 | 0.140s | 0.147s | 5.3% |
| NFL | 3 | 0.070s | 0.072s | 3.7% |
| SlingTV | 3 | 1.638s | 1.665s | 1.6% |

**Views provide:**
- Readable metric names instead of int1, int2, etc.
- Per-customer isolation
- Minimal performance cost

Queries used for benchmarking can be found [here](/output/view_benchmarks/query_index.json)

## 5. Implementation

**Repository:** https://github.com/Conviva-Internal/data_model_v3_scripts
