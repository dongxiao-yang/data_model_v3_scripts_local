# Aggregation Performance Benchmark Results

**Generated:** 2025-10-15 08:14:55

## Overall Performance Statistics

### Old Schema (Map Columns)

- **Average:** 0.598s
- **Median:** 0.145s
- **Min:** 0.056s
- **Max:** 2.427s
- **Std Dev:** 0.749s

### New Schema (Primitive Columns)

- **Average:** 0.099s
- **Median:** 0.029s
- **Min:** 0.018s
- **Max:** 0.304s
- **Std Dev:** 0.106s

## Per-Customer Analysis

### Intigral

- **Metrics Tested:** 3
- **Average Speedup:** 3.71x
- **Min Speedup:** 3.26x
- **Max Speedup:** 4.61x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| app_startup_time | 0.180 | 0.039 | 4.61x |
| f_8458_9478_total_duration | 0.063 | 0.019 | 3.26x |
| f_8458_9478_success | 0.062 | 0.019 | 3.26x |

### NFL

- **Metrics Tested:** 3
- **Average Speedup:** 5.59x
- **Min Speedup:** 5.02x
- **Max Speedup:** 5.92x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| f_183_7200_success | 0.124 | 0.021 | 5.92x |
| f_183_7200_total_duration | 0.122 | 0.021 | 5.82x |
| app_startup_time | 0.146 | 0.029 | 5.02x |

### SlingTV

- **Metrics Tested:** 3
- **Average Speedup:** 6.18x
- **Min Speedup:** 5.10x
- **Max Speedup:** 8.16x

| Metric | Old Avg (s) | New Avg (s) | Speedup |
|--------|-------------|-------------|---------|
| app_startup_time | 2.315 | 0.284 | 8.16x |
| f_202_363_success | 1.186 | 0.225 | 5.27x |
| f_202_363_total_duration | 1.184 | 0.232 | 5.10x |

## Per-Metric Type Analysis

### Duration

- **Metrics Count:** 6
- **Average Speedup:** 5.33x
- **Min Speedup:** 3.26x
- **Max Speedup:** 8.16x

### Success Count

- **Metrics Count:** 3
- **Average Speedup:** 4.82x
- **Min Speedup:** 3.26x
- **Max Speedup:** 5.92x

## Detailed Results

| Customer | Metric | Old Avg (s) | New Avg (s) | Speedup | Improvement |
|----------|--------|-------------|-------------|---------|-------------|
| SlingTV | app_startup_time | 2.315 | 0.284 | 8.16x | 87.7% |
| NFL | f_183_7200_success | 0.124 | 0.021 | 5.92x | 83.1% |
| NFL | f_183_7200_total_duration | 0.122 | 0.021 | 5.82x | 82.8% |
| SlingTV | f_202_363_success | 1.186 | 0.225 | 5.27x | 81.0% |
| SlingTV | f_202_363_total_duration | 1.184 | 0.232 | 5.10x | 80.4% |
| NFL | app_startup_time | 0.146 | 0.029 | 5.02x | 80.1% |
| Intigral | app_startup_time | 0.180 | 0.039 | 4.61x | 78.3% |
| Intigral | f_8458_9478_total_duration | 0.063 | 0.019 | 3.26x | 69.4% |
| Intigral | f_8458_9478_success | 0.062 | 0.019 | 3.26x | 69.3% |
