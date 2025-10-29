# Data Model V3 Physical Schema Transformation

This project transforms production ClickHouse data from the current Map-column based schema to a new flattened primitive column schema for improved query performance.

## Overview

- **Source**: `eco_cross_page_flow_pt1m_local` table with Map columns
- **Target**: Pre-aggregated table with primitive int/float columns
- **Scope**: 1 day of data transformation for proof-of-concept

## Usage

### Data Transformation

#### Running the Pipeline

```bash
# Option 1: Using the shortcut command
poetry run transform

# Option 2: Direct execution
poetry run python run_transformation_pipeline.py
```

#### Configuring Pipeline Phases

Edit `run_transformation_pipeline.py` to control which phases execute:

```python
# ============================================================
# PIPELINE CONFIGURATION - Change these to control execution
# ============================================================
RUN_KEY_DISCOVERY = True # Control whether to run key discovery (set to False if key mapping already exists)
DROP_TARGET_TABLE_BEFORE_CREATE = True # Set to False to not drop target table before starting pipeline
TRUNCATE_TARGET_TABLE = True  # Set to False to continue from where you left off
START_FROM_CHUNK = 0  # Start from chunk N (0-based indexing, so chunk 95 = index 96)
# ============================================================
```

#### Running Analysis Phase

To run the log analysis phase, run the following command `poetry run python src/analysis/parse_transformation_logs.py`. This will look at logs in `output/reports/transformation.log` and parse the logs to collect metrics and write out a report.

### Benchmark Pipeline

The project includes a comprehensive benchmarking pipeline to compare aggregation query performance between the old schema (Map columns) and new schema (primitive columns).

#### Running the Pipeline

```bash
# Option 1: Using the shortcut command
poetry run benchmark

# Option 2: Direct execution
poetry run python run_benchmark_pipeline.py
```

#### Configuring Pipeline Phases

Edit `run_benchmark_pipeline.py` to control which phases execute:

```python
# ============================================================
# PIPELINE CONFIGURATION - Change these to control execution
# ============================================================
RUN_PHASE_1 = True   # Metric Discovery
RUN_PHASE_2 = True   # Query Generation
RUN_PHASE_3 = True   # Validation
RUN_PHASE_4 = True   # Benchmark Execution
RUN_PHASE_5 = True   # Analysis & Reporting
# ============================================================
```

#### Pipeline Phases

1. **Phase 1: Metric Discovery** - Identifies metrics, flowIds, and column mappings
2. **Phase 2: Query Generation** - Creates SQL queries for old and new schemas
3. **Phase 3: Validation** - Validates transformation correctness by comparing aggregation results
4. **Phase 4: Benchmark Execution** - Executes queries and records timing data
5. **Phase 5: Analysis & Reporting** - Generates performance reports and statistics

### View Benchmark Pipeline

The project includes a view benchmarking pipeline to compare query performance between direct table access (using primitive columns) and view-based access (using readable metric names).

#### Running the Pipeline

```bash
# Option 1: Using the shortcut command
poetry run view-benchmark

# Option 2: Direct execution
poetry run python run_view_benchmark_pipeline.py
```

#### Configuring Pipeline Phases

Edit `run_view_benchmark_pipeline.py` to control which phases execute:

```python
# ============================================================
# PIPELINE CONFIGURATION - Change these to control execution
# ============================================================
RUN_PHASE_1 = True   # View Generation
RUN_PHASE_2 = True   # Query Generation
RUN_PHASE_3 = True   # Benchmark Execution
RUN_PHASE_4 = True   # Analysis & Reporting
# ============================================================
```

#### Pipeline Phases

1. **Phase 1: View Generation** - Creates per-customer views mapping primitive columns to metric names
2. **Phase 2: Query Generation** - Creates SQL queries for direct table and view-based access
3. **Phase 3: Benchmark Execution** - Executes queries and records timing data
4. **Phase 4: Analysis & Reporting** - Generates performance reports comparing view overhead