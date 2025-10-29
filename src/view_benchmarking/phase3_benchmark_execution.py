"""
Phase 3: Benchmark Execution for View Benchmarking

This script:
1. Loads query index from Phase 2
2. Executes warm-up runs (5x per query)
3. Executes measurement runs (50x per query)
4. Records execution time and row count for each run
5. Saves raw timing data to CSV

Compares two access methods:
- Direct table: Query table directly using primitive columns (intN)
- Through view: Query through views using readable metric names
"""

import json
import csv
import time
from typing import Dict
import clickhouse_connect

from src.config.settings import TARGET_DB, CUSTOMER_NAMES

# Constants
WARMUP_RUNS = 5
MEASUREMENT_RUNS = 50


def load_query_index() -> Dict:
    """Load query index from Phase 2."""
    with open("output/view_benchmarks/query_index.json", "r") as f:
        return json.load(f)


def read_query_file(filepath: str) -> str:
    """Read SQL query from file."""
    with open(filepath, "r") as f:
        return f.read()


def execute_query_with_timing(client, query: str) -> tuple:
    """
    Execute query and measure timing.
    Returns: (execution_time_seconds, row_count)
    """
    start_time = time.perf_counter()
    result = client.query(query)
    end_time = time.perf_counter()

    execution_time = end_time - start_time
    row_count = len(result.result_rows)

    return execution_time, row_count


def run_benchmark():
    """Main benchmark execution process."""
    print("=" * 80)
    print("Phase 3: Benchmark Execution for View Benchmarking")
    print("=" * 80)
    print()

    # Load query index
    print("Loading query index from Phase 2...")
    index_data = load_query_index()
    queries = index_data["queries"]
    print(f"Loaded {len(queries)} metrics to benchmark")
    print()

    # Connect to target database
    print("Connecting to target database...")
    print(f"  Host: {TARGET_DB.host}:{TARGET_DB.port}")
    client = clickhouse_connect.get_client(
        host=TARGET_DB.host,
        port=TARGET_DB.port,
        settings={"max_execution_time": TARGET_DB.timeout}
    )
    print("✓ Connected to database")
    print()

    # Results storage
    results = []

    # Process each metric
    total_queries = len(queries) * 2  # direct + view
    total_runs = total_queries * (WARMUP_RUNS + MEASUREMENT_RUNS)
    completed_runs = 0

    print("=" * 80)
    print(f"Starting Benchmark Execution")
    print(f"  Metrics: {len(queries)}")
    print(f"  Access types: 2 (direct table + through view)")
    print(f"  Warm-up runs: {WARMUP_RUNS} per query")
    print(f"  Measurement runs: {MEASUREMENT_RUNS} per query")
    print(f"  Total query executions: {total_runs}")
    print("=" * 80)
    print()

    for idx, query_info in enumerate(queries, start=1):
        customer_id = query_info["customer_id"]
        customer_name = query_info["customer_name"]
        metric_key = query_info["metric_key"]

        print(f"[{idx}/{len(queries)}] Benchmarking: {customer_name} - {metric_key}")
        print("-" * 80)

        # DIRECT TABLE BENCHMARK
        print(f"  Direct Table Access (primitive columns):")
        direct_query_file = f"output/view_benchmarks/queries/{query_info['direct_table_query_file']}"
        direct_query = read_query_file(direct_query_file)

        # Warm-up
        print(f"    Warm-up run...", end=" ", flush=True)
        try:
            execute_query_with_timing(client, direct_query)
            completed_runs += WARMUP_RUNS
            print("✓")
        except Exception as e:
            print(f"❌ Error: {e}")
            client.close()
            raise

        # Measurement runs
        print(f"    Measurement runs: ", end="", flush=True)
        for run_num in range(1, MEASUREMENT_RUNS + 1):
            try:
                exec_time, row_count = execute_query_with_timing(client, direct_query)
                completed_runs += 1

                results.append({
                    "customer_id": customer_id,
                    "customer_name": customer_name,
                    "metric_key": metric_key,
                    "access_type": "direct_table",
                    "run_number": run_num,
                    "execution_time_seconds": exec_time,
                    "row_count": row_count,
                    "query_date": query_info["date_range"]
                })

                print(f"{run_num}", end="", flush=True)
                if run_num < MEASUREMENT_RUNS:
                    print(".", end="", flush=True)
            except Exception as e:
                print(f" ❌ Error on run {run_num}: {e}")
                client.close()
                raise

        avg_time = sum(r["execution_time_seconds"] for r in results[-MEASUREMENT_RUNS:]) / MEASUREMENT_RUNS
        print(f" ✓ (avg: {avg_time:.3f}s)")

        # THROUGH VIEW BENCHMARK
        print(f"  Through View Access (readable metric names):")
        view_query_file = f"output/view_benchmarks/queries/{query_info['through_view_query_file']}"
        view_query = read_query_file(view_query_file)

        # Warm-up
        print(f"    Warm-up run...", end=" ", flush=True)
        try:
            execute_query_with_timing(client, view_query)
            completed_runs += WARMUP_RUNS
            print("✓")
        except Exception as e:
            print(f"❌ Error: {e}")
            client.close()
            raise

        # Measurement runs
        print(f"    Measurement runs: ", end="", flush=True)
        for run_num in range(1, MEASUREMENT_RUNS + 1):
            try:
                exec_time, row_count = execute_query_with_timing(client, view_query)
                completed_runs += 1

                results.append({
                    "customer_id": customer_id,
                    "customer_name": customer_name,
                    "metric_key": metric_key,
                    "access_type": "through_view",
                    "run_number": run_num,
                    "execution_time_seconds": exec_time,
                    "row_count": row_count,
                    "query_date": query_info["date_range"]
                })

                print(f"{run_num}", end="", flush=True)
                if run_num < MEASUREMENT_RUNS:
                    print(".", end="", flush=True)
            except Exception as e:
                print(f" ❌ Error on run {run_num}: {e}")
                client.close()
                raise

        avg_time = sum(r["execution_time_seconds"] for r in results[-MEASUREMENT_RUNS:]) / MEASUREMENT_RUNS
        print(f" ✓ (avg: {avg_time:.3f}s)")
        print()

    # Close connection
    client.close()

    # Save results to CSV
    output_csv = "output/view_benchmarks/raw_timing_data.csv"
    print("Saving results to CSV...")
    with open(output_csv, "w", newline="") as f:
        fieldnames = [
            "customer_id",
            "customer_name",
            "metric_key",
            "access_type",
            "run_number",
            "execution_time_seconds",
            "row_count",
            "query_date"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"✓ Results saved to: {output_csv}")
    print()

    # Print summary statistics
    print("=" * 80)
    print("✓ Phase 3 Complete!")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  Total query executions: {completed_runs}")
    print(f"  Total measurement runs: {len(results)}")
    print(f"  Results saved to: {output_csv}")
    print()

    # Quick summary stats
    direct_times = [r["execution_time_seconds"] for r in results if r["access_type"] == "direct_table"]
    view_times = [r["execution_time_seconds"] for r in results if r["access_type"] == "through_view"]

    if direct_times and view_times:
        direct_avg = sum(direct_times) / len(direct_times)
        view_avg = sum(view_times) / len(view_times)
        overhead_pct = ((view_avg - direct_avg) / direct_avg * 100) if direct_avg > 0 else 0

        print("Quick Performance Summary:")
        print(f"  Direct table avg: {direct_avg:.3f}s")
        print(f"  Through view avg: {view_avg:.3f}s")
        if overhead_pct >= 0:
            print(f"  View overhead: +{overhead_pct:.2f}%")
        else:
            print(f"  View improvement: {abs(overhead_pct):.2f}% faster")
        print()


if __name__ == "__main__":
    run_benchmark()
