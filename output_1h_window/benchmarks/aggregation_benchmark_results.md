# Aggregation Performance Benchmark Results

**Generated:** 2025-10-16 10:51:05

**Date Range:** 2025-10-08 00:00:00 to 2025-10-08 06:00:00
**Old Schema Table:** default.eco_cross_page_flow_pt1m_local_20251008_3cust
**New Schema Table:** default.rui_eco_cross_page_preagg_pt1m_1h

## Overall Performance Statistics

### Old Schema (Map Columns)

- **Average:** 0.501s
- **Median:** 0.076s
- **Min:** 0.014s
- **Max:** 4.328s
- **Std Dev:** 0.930s

### New Schema (Primitive Columns)

- **Average:** 0.088s
- **Median:** 0.025s
- **Min:** 0.012s
- **Max:** 0.329s
- **Std Dev:** 0.098s

## Per-Customer Analysis

### Intigral

- **Metrics Tested:** 4
- **Average Speedup:** 2.13x
- **Min Speedup:** 1.19x
- **Max Speedup:** 4.49x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| event_count | 0.118 | 0.026 | 4.49x |
| app_startup_time | 0.038 | 0.024 | 1.61x |
| f_8458_9478_total_duration | 0.016 | 0.013 | 1.24x |
| f_8458_9478_success | 0.016 | 0.013 | 1.19x |

### NFL

- **Metrics Tested:** 4
- **Average Speedup:** 2.93x
- **Min Speedup:** 2.19x
- **Max Speedup:** 4.40x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| event_count | 0.103 | 0.023 | 4.40x |
| f_183_7200_success | 0.045 | 0.017 | 2.61x |
| f_183_7200_total_duration | 0.045 | 0.018 | 2.51x |
| app_startup_time | 0.050 | 0.023 | 2.19x |

### SlingTV

- **Metrics Tested:** 4
- **Average Speedup:** 4.16x
- **Min Speedup:** 2.67x
- **Max Speedup:** 7.66x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| event_count | 1.964 | 0.256 | 7.66x |
| app_startup_time | 0.736 | 0.204 | 3.61x |
| f_202_363_success | 0.411 | 0.152 | 2.71x |
| f_202_363_total_duration | 0.415 | 0.156 | 2.67x |

## Per-Metric Type Analysis

### Duration

- **Metrics Count:** 6
- **Average Speedup:** 2.31x
- **Min Speedup:** 1.24x
- **Max Speedup:** 3.61x

### Other

- **Metrics Count:** 3
- **Average Speedup:** 5.52x
- **Min Speedup:** 4.40x
- **Max Speedup:** 7.66x

### Success Count

- **Metrics Count:** 3
- **Average Speedup:** 2.17x
- **Min Speedup:** 1.19x
- **Max Speedup:** 2.71x

## Detailed Results

| Customer | Metric | Old Avg (s) | New Avg (s) | Speedup | Improvement |
|----------|--------|-------------|-------------|---------|-------------|
| SlingTV | event_count | 1.964 | 0.256 | 7.66x | 87.0% |
| Intigral | event_count | 0.118 | 0.026 | 4.49x | 77.7% |
| NFL | event_count | 0.103 | 0.023 | 4.40x | 77.3% |
| SlingTV | app_startup_time | 0.736 | 0.204 | 3.61x | 72.3% |
| SlingTV | f_202_363_success | 0.411 | 0.152 | 2.71x | 63.2% |
| SlingTV | f_202_363_total_duration | 0.415 | 0.156 | 2.67x | 62.6% |
| NFL | f_183_7200_success | 0.045 | 0.017 | 2.61x | 61.7% |
| NFL | f_183_7200_total_duration | 0.045 | 0.018 | 2.51x | 60.2% |
| NFL | app_startup_time | 0.050 | 0.023 | 2.19x | 54.3% |
| Intigral | app_startup_time | 0.038 | 0.024 | 1.61x | 37.9% |
| Intigral | f_8458_9478_total_duration | 0.016 | 0.013 | 1.24x | 19.4% |
| Intigral | f_8458_9478_success | 0.016 | 0.013 | 1.19x | 16.2% |
