# View-Based Access Performance Benchmark Results

**Generated:** 2025-10-15 08:26:04

## Overall Performance Statistics

### Direct Table Access

- **Average:** 0.616s
- **Median:** 0.140s
- **Min:** 0.069s
- **Max:** 1.642s
- **Std Dev:** 0.767s
- **P95:** 1.642s
- **P99:** 1.642s

### Through View Access

- **Average:** 0.628s
- **Median:** 0.144s
- **Min:** 0.070s
- **Max:** 1.668s
- **Std Dev:** 0.778s
- **P95:** 1.668s
- **P99:** 1.668s

## Per-Customer Analysis

### Intigral

- **Metrics Tested:** 3
- **Average Direct:** 0.140s
- **Average View:** 0.147s
- **Average Overhead:** 5.33%

| Metric | Direct Avg (s) | View Avg (s) | Overhead |
|--------|----------------|--------------|----------|
| f_8458_9478_total_duration | 0.139 | 0.158 | +13.61% |
| app_startup_time | 0.141 | 0.144 | +2.13% |
| f_8458_9478_success | 0.140 | 0.140 | +0.24% |

### NFL

- **Metrics Tested:** 3
- **Average Direct:** 0.070s
- **Average View:** 0.072s
- **Average Overhead:** 3.69%

| Metric | Direct Avg (s) | View Avg (s) | Overhead |
|--------|----------------|--------------|----------|
| app_startup_time | 0.071 | 0.075 | +5.75% |
| f_183_7200_total_duration | 0.069 | 0.072 | +3.79% |
| f_183_7200_success | 0.069 | 0.070 | +1.55% |

### SlingTV

- **Metrics Tested:** 3
- **Average Direct:** 1.638s
- **Average View:** 1.665s
- **Average Overhead:** 1.62%

| Metric | Direct Avg (s) | View Avg (s) | Overhead |
|--------|----------------|--------------|----------|
| f_202_363_success | 1.638 | 1.668 | +1.83% |
| app_startup_time | 1.636 | 1.662 | +1.59% |
| f_202_363_total_duration | 1.642 | 1.665 | +1.44% |

## Detailed Results

| Customer | Metric | Direct Avg (s) | View Avg (s) | Overhead |
|----------|--------|----------------|--------------|----------|
| Intigral | f_8458_9478_total_duration | 0.139 | 0.158 | +13.61% |
| NFL | app_startup_time | 0.071 | 0.075 | +5.75% |
| NFL | f_183_7200_total_duration | 0.069 | 0.072 | +3.79% |
| Intigral | app_startup_time | 0.141 | 0.144 | +2.13% |
| SlingTV | f_202_363_success | 1.638 | 1.668 | +1.83% |
| SlingTV | app_startup_time | 1.636 | 1.662 | +1.59% |
| NFL | f_183_7200_success | 0.069 | 0.070 | +1.55% |
| SlingTV | f_202_363_total_duration | 1.642 | 1.665 | +1.44% |
| Intigral | f_8458_9478_success | 0.140 | 0.140 | +0.24% |
