"""
Phase 1: Metric Selection & Discovery for Aggregation Benchmarking

This script:
1. Selects 3 metrics per customer (12 total)
2. Discovers flowId for each metric
3. Discovers source map column (metricIntGroupX) for each metric
4. Maps metrics to target physical columns (intN)
5. Outputs complete mapping to JSON
"""

import json
import sys
from typing import Dict, Tuple
import clickhouse_connect

from src.config.settings import (
    SOURCE_DB,
    SOURCE_TABLE,
    CUSTOMERS,
    UNIFIED_DATE_START,
    UNIFIED_DATE_END,
    KEY_MAPPING_FILE
)

# Use unified dates for discovering metrics in source table
DATE_START = UNIFIED_DATE_START
DATE_END = UNIFIED_DATE_END

# Selected metrics: 3 per customer (mix of counts and durations)
# Only selecting metrics from Map columns (not standard columns)
SELECTED_METRICS = {
    1960181009: ["f_8458_9478_success", "f_8458_9478_total_duration", "app_startup_time", "event_count"],
    1960181845: ["f_202_363_success", "f_202_363_total_duration", "app_startup_time", "event_count"],
    1960183305: ["f_183_7200_success", "f_183_7200_total_duration", "app_startup_time", "event_count"],
    1960183601: ["f_710_1376_success", "f_710_1376_total_duration", "app_startup_time"],
}


def load_key_mapping() -> Dict:
    """Load the global key mapping file."""
    with open(KEY_MAPPING_FILE, "r") as f:
        return json.load(f)


def discover_flowid(
    client: clickhouse_connect.driver.Client,
    customer_id: int,
    metric_key: str
) -> Tuple[str, bool]:
    """
    Discover which flowId(s) are associated with a metric.
    This checks where the metric actually appears with a non-zero value.
    Returns: (most_common_flowid, has_multiple_flowids)
    """
    print(f"  Discovering flowId for metric: {metric_key}")

    # Build query to check all 15 metricIntGroup columns
    # Use mapContains which is more efficient
    conditions = []
    for i in range(1, 16):
        conditions.append(f"mapContains(metricIntGroup{i}, '{metric_key}')")

    where_clause = " OR ".join(conditions)

    query = f"""
    SELECT flowId, count(*) as cnt
    FROM {SOURCE_TABLE}
    WHERE customerId = {customer_id}
      AND timestampMs >= '{DATE_START}'
      AND timestampMs < '{DATE_END}'
      AND ({where_clause})
    GROUP BY flowId
    ORDER BY cnt DESC
    LIMIT 10
    """

    result = client.query(query)
    rows = result.result_rows

    if not rows:
        raise Exception(f"No flowId found for metric {metric_key} in customer {customer_id}")

    most_common_flowid = rows[0][0]
    has_multiple = len(rows) > 1

    if has_multiple:
        print(f"    ⚠️  Metric has multiple flowIds: {[row[0] for row in rows[:5]]}")
        print(f"    Using most common: {most_common_flowid} (count: {rows[0][1]})")
    else:
        print(f"    Found flowId: {most_common_flowid}")

    return str(most_common_flowid), has_multiple


def discover_source_map_column(
    client: clickhouse_connect.driver.Client,
    customer_id: int,
    metric_key: str,
    flowid: str
) -> str:
    """
    Discover which metricIntGroupX contains the metric.
    """
    print(f"  Discovering source map column for: {metric_key}")

    # Build query to check all 15 metricIntGroup columns
    # We'll check which one contains the metric
    conditions = []
    for i in range(1, 16):
        conditions.append(f"mapContains(metricIntGroup{i}, '{metric_key}')")

    where_clause = " OR ".join(conditions)

    query = f"""
    SELECT
        {', '.join([f"if(mapContains(metricIntGroup{i}, '{metric_key}'), 'metricIntGroup{i}', '') as g{i}" for i in range(1, 16)])}
    FROM {SOURCE_TABLE}
    WHERE customerId = {customer_id}
      AND flowId = '{flowid}'
      AND timestampMs >= '{DATE_START}'
      AND timestampMs < '{DATE_END}'
      AND ({where_clause})
    LIMIT 1
    """

    result = client.query(query)
    row = result.result_rows[0] if result.result_rows else None

    if not row:
        raise Exception(f"No data found for metric {metric_key} in customer {customer_id} with flowId {flowid}")

    # Find which column has the metric
    for i, value in enumerate(row, start=1):
        if value:
            print(f"    Found in: {value}")
            return value

    raise Exception(f"Metric {metric_key} not found in any metricIntGroup column")


def map_to_physical_column(key_mapping: Dict, metric_key: str, customer_id: int) -> str:
    """Map metric key to physical column using per-customer mapping."""
    customer_data = key_mapping["customers"].get(str(customer_id))

    if not customer_data:
        raise Exception(f"No mapping found for customer {customer_id}")

    int_mapping = customer_data.get("int_mapping", {})
    float_mapping = customer_data.get("float_mapping", {})

    if metric_key in int_mapping:
        return int_mapping[metric_key]
    elif metric_key in float_mapping:
        return float_mapping[metric_key]
    else:
        raise Exception(f"Metric {metric_key} not found for customer {customer_id}")


def run_discovery():
    """Main discovery process."""
    print("=" * 80)
    print("Phase 1: Metric Discovery for Aggregation Benchmark")
    print("=" * 80)
    print()

    # Load key mapping
    print("Loading key mapping...")
    key_mapping = load_key_mapping()
    print(f"Loaded mapping with max {key_mapping['max_columns']['int_columns']} int columns")
    print()

    # Connect to source database
    print("Connecting to source database...")
    print(f"  Host: {SOURCE_DB.host}")
    client = clickhouse_connect.get_client(
        host=SOURCE_DB.host,
        port=SOURCE_DB.port,
        settings={"max_execution_time": SOURCE_DB.timeout}
    )
    print("Connected!")
    print()

    # Results storage
    results = []

    # Process each customer and their metrics
    for customer_id in CUSTOMERS:
        print(f"Processing Customer {customer_id}")
        print("-" * 80)

        metrics = SELECTED_METRICS[customer_id]

        for metric_key in metrics:
            print(f"\nMetric: {metric_key}")

            try:
                # Discover flowId
                flowid, has_multiple_flowids = discover_flowid(client, customer_id, metric_key)

                # Discover source map column
                source_map_column = discover_source_map_column(client, customer_id, metric_key, flowid)

                # Map to physical column using customer-specific mapping
                target_physical_column = map_to_physical_column(key_mapping, metric_key, customer_id)
                print(f"  Target column: {target_physical_column}")

                # Store result
                results.append({
                    "customer_id": customer_id,
                    "metric_key": metric_key,
                    "flowid": flowid,
                    "source_map_column": source_map_column,
                    "target_physical_column": target_physical_column,
                    "has_multiple_flowids": has_multiple_flowids
                })

            except Exception as e:
                print(f"  ❌ Error: {e}")
                client.close()
                sys.exit(1)

        print()

    client.close()

    # Save results
    output_path = "output/benchmarks/metric_mappings.json"
    output_data = {
        "metadata": {
            "phase": "Phase 1: Metric Discovery",
            "total_metrics": len(results),
            "customers": CUSTOMERS,
            "date_range": f"{DATE_START} to {DATE_END}",
            "source_table": SOURCE_TABLE
        },
        "mappings": results
    }

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2)

    print("=" * 80)
    print(f"✓ Phase 1 Complete! Results saved to: {output_path}")
    print("=" * 80)
    print()

    # Print summary
    print("Summary:")
    print(f"  Total metrics discovered: {len(results)}")
    multiple_flowids = sum(1 for r in results if r["has_multiple_flowids"])
    print(f"  Metrics with multiple flowIds: {multiple_flowids}")
    print()


if __name__ == "__main__":
    run_discovery()
