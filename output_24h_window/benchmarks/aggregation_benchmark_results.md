# Aggregation Performance Benchmark Results

**Generated:** 2025-10-16 10:34:33

**Date Range:** 2025-10-08 00:00:00 to 2025-10-08 06:00:00
**Old Schema Table:** default.eco_cross_page_flow_pt1m_local_20251008_3cust
**New Schema Table:** default.eco_cross_page_preagg_pt1m_3cust

## Overall Performance Statistics

### Old Schema (Map Columns)

- **Average:** 0.500s
- **Median:** 0.075s
- **Min:** 0.015s
- **Max:** 4.316s
- **Std Dev:** 0.929s

### New Schema (Primitive Columns)

- **Average:** 0.066s
- **Median:** 0.035s
- **Min:** 0.013s
- **Max:** 0.297s
- **Std Dev:** 0.069s

## Per-Customer Analysis

### Intigral

- **Metrics Tested:** 4
- **Average Speedup:** 1.61x
- **Min Speedup:** 1.02x
- **Max Speedup:** 3.32x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| event_count | 0.118 | 0.036 | 3.32x |
| f_8458_9478_success | 0.016 | 0.015 | 1.05x |
| f_8458_9478_total_duration | 0.016 | 0.015 | 1.04x |
| app_startup_time | 0.037 | 0.037 | 1.02x |

### NFL

- **Metrics Tested:** 4
- **Average Speedup:** 3.77x
- **Min Speedup:** 2.82x
- **Max Speedup:** 5.85x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| event_count | 0.102 | 0.017 | 5.85x |
| f_183_7200_success | 0.045 | 0.014 | 3.23x |
| f_183_7200_total_duration | 0.045 | 0.014 | 3.17x |
| app_startup_time | 0.050 | 0.018 | 2.82x |

### SlingTV

- **Metrics Tested:** 4
- **Average Speedup:** 6.19x
- **Min Speedup:** 3.86x
- **Max Speedup:** 10.92x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| event_count | 1.963 | 0.180 | 10.92x |
| app_startup_time | 0.738 | 0.122 | 6.06x |
| f_202_363_total_duration | 0.412 | 0.105 | 3.93x |
| f_202_363_success | 0.408 | 0.106 | 3.86x |

## Per-Metric Type Analysis

### Duration

- **Metrics Count:** 6
- **Average Speedup:** 3.01x
- **Min Speedup:** 1.02x
- **Max Speedup:** 6.06x

### Other

- **Metrics Count:** 3
- **Average Speedup:** 6.69x
- **Min Speedup:** 3.32x
- **Max Speedup:** 10.92x

### Success Count

- **Metrics Count:** 3
- **Average Speedup:** 2.71x
- **Min Speedup:** 1.05x
- **Max Speedup:** 3.86x

## Detailed Results

| Customer | Metric | Old Avg (s) | New Avg (s) | Speedup | Improvement |
|----------|--------|-------------|-------------|---------|-------------|
| SlingTV | event_count | 1.963 | 0.180 | 10.92x | 90.8% |
| SlingTV | app_startup_time | 0.738 | 0.122 | 6.06x | 83.5% |
| NFL | event_count | 0.102 | 0.017 | 5.85x | 82.9% |
| SlingTV | f_202_363_total_duration | 0.412 | 0.105 | 3.93x | 74.6% |
| SlingTV | f_202_363_success | 0.408 | 0.106 | 3.86x | 74.1% |
| Intigral | event_count | 0.118 | 0.036 | 3.32x | 69.8% |
| NFL | f_183_7200_success | 0.045 | 0.014 | 3.23x | 69.0% |
| NFL | f_183_7200_total_duration | 0.045 | 0.014 | 3.17x | 68.4% |
| NFL | app_startup_time | 0.050 | 0.018 | 2.82x | 64.6% |
| Intigral | f_8458_9478_success | 0.016 | 0.015 | 1.05x | 4.7% |
| Intigral | f_8458_9478_total_duration | 0.016 | 0.015 | 1.04x | 4.0% |
| Intigral | app_startup_time | 0.037 | 0.037 | 1.02x | 2.2% |
