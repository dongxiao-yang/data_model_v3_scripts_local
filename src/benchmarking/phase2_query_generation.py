"""
Phase 2: Query Generation for Aggregation Benchmarking

This script:
1. Loads metric mappings from Phase 1
2. Generates old schema queries (with Map columns)
3. Generates new schema queries (with primitive columns)
4. Saves all queries to individual files
5. Creates a query summary/index file
"""

import json
import os
from typing import Dict, List

from src.config.settings import (
    CUSTOMER_NAMES,
    UNIFIED_DATE_START,
    UNIFIED_DATE_END,
    SOURCE_TABLE,
    TARGET_TABLE,
    DIMENSIONS,
    AGG_WINDOW_HOURS
)

# Use unified start; compute aggregation end based on configured window
DATE_START = UNIFIED_DATE_START
DATE_END = UNIFIED_DATE_END

# Compute a phase-specific aggregation end time (6h by default)
from datetime import datetime, timedelta
_start_dt = datetime.strptime(DATE_START, "%Y-%m-%d %H:%M:%S")
AGG_DATE_END = (_start_dt + timedelta(hours=AGG_WINDOW_HOURS)).strftime("%Y-%m-%d %H:%M:%S")

# Table references from settings
OLD_SCHEMA_TABLE = SOURCE_TABLE  # eco_cross_page_flow_pt1m_local_20251008
NEW_SCHEMA_TABLE = TARGET_TABLE  # eco_cross_page_preagg_pt1m_test_mul_cust


def load_metric_mappings() -> Dict:
    """Load metric mappings from Phase 1."""
    with open("output/benchmarks/metric_mappings.json", "r") as f:
        return json.load(f)


def generate_old_schema_query(mapping: Dict) -> str:
    """Generate query for old schema (Map columns)."""
    dimensions_str = ", ".join(DIMENSIONS)

    query = f"""SELECT
    {dimensions_str},
    SUM({mapping['source_map_column']}['{mapping['metric_key']}']) as metric_sum
FROM {OLD_SCHEMA_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND flowId = '{mapping['flowid']}'
  AND {mapping['source_map_column']}['{mapping['metric_key']}'] > 0
  AND timestampMs >= '{DATE_START}'
  AND timestampMs < '{AGG_DATE_END}'
GROUP BY {dimensions_str}
ORDER BY {dimensions_str}"""

    return query


def generate_new_schema_query(mapping: Dict) -> str:
    """Generate query for new schema (primitive columns)."""
    dimensions_str = ", ".join(DIMENSIONS)

    # NOTE: Target table excludes flowId column, so we filter by metric > 0
    # to only include dimension combinations where this metric actually exists
    # (equivalent to the flowId filter in old schema)
    query = f"""SELECT
    {dimensions_str},
    SUM({mapping['target_physical_column']}) as metric_sum
FROM {NEW_SCHEMA_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND {mapping['target_physical_column']} > 0
  AND timestampMs >= '{DATE_START}'
  AND timestampMs < '{AGG_DATE_END}'
GROUP BY {dimensions_str}
ORDER BY {dimensions_str}"""

    return query


def generate_groupby_country_old_schema(mapping: Dict) -> str:
    """Generate GROUP BY country query for old schema (Map columns)."""
    query = f"""SELECT
    country,
    SUM({mapping['source_map_column']}['{mapping['metric_key']}']) as metric_sum
FROM {OLD_SCHEMA_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND {mapping['source_map_column']}['{mapping['metric_key']}'] > 0
  AND timestampMs >= '{DATE_START}'
  AND timestampMs < '{AGG_DATE_END}'
GROUP BY country
ORDER BY country"""
    return query


def generate_groupby_country_new_schema(mapping: Dict) -> str:
    """Generate GROUP BY country query for new schema (primitive columns)."""
    query = f"""SELECT
    country,
    SUM({mapping['target_physical_column']}) as metric_sum
FROM {NEW_SCHEMA_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND {mapping['target_physical_column']} > 0
  AND timestampMs >= '{DATE_START}'
  AND timestampMs < '{AGG_DATE_END}'
GROUP BY country
ORDER BY country"""
    return query


def generate_groupby_minute_old_schema(mapping: Dict) -> str:
    """Generate GROUP BY minute query for old schema (Map columns)."""
    query = f"""SELECT
    toStartOfMinute(timestampMs) as minute,
    SUM({mapping['source_map_column']}['{mapping['metric_key']}']) as metric_sum
FROM {OLD_SCHEMA_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND {mapping['source_map_column']}['{mapping['metric_key']}'] > 0
  AND timestampMs >= '{DATE_START}'
  AND timestampMs < '{DATE_END}'
GROUP BY minute
ORDER BY minute"""
    return query


def generate_groupby_minute_new_schema(mapping: Dict) -> str:
    """Generate GROUP BY minute query for new schema (primitive columns)."""
    query = f"""SELECT
    toStartOfMinute(timestampMs) as minute,
    SUM({mapping['target_physical_column']}) as metric_sum
FROM {NEW_SCHEMA_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND {mapping['target_physical_column']} > 0
  AND timestampMs >= '{DATE_START}'
  AND timestampMs < '{DATE_END}'
GROUP BY minute
ORDER BY minute"""
    return query


def generate_groupby_minute_filtered_old_schema(mapping: Dict, hours: int = AGG_WINDOW_HOURS) -> str:
    """Generate GROUP BY minute query with time filter for old schema (Map columns)."""
    # Calculate end time based on hours
    from datetime import datetime, timedelta
    start_dt = datetime.strptime(DATE_START, "%Y-%m-%d %H:%M:%S")
    end_dt = start_dt + timedelta(hours=hours)
    end_time = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""SELECT
    toStartOfMinute(timestampMs) as minute,
    SUM({mapping['source_map_column']}['{mapping['metric_key']}']) as metric_sum
FROM {OLD_SCHEMA_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND {mapping['source_map_column']}['{mapping['metric_key']}'] > 0
  AND timestampMs >= toDateTime('{DATE_START}')
  AND timestampMs < toDateTime('{end_time}')
GROUP BY minute
ORDER BY minute"""
    return query


def generate_groupby_minute_filtered_new_schema(mapping: Dict, hours: int = AGG_WINDOW_HOURS) -> str:
    """Generate GROUP BY minute query with time filter for new schema (primitive columns)."""
    # Calculate end time based on hours
    from datetime import datetime, timedelta
    start_dt = datetime.strptime(DATE_START, "%Y-%m-%d %H:%M:%S")
    end_dt = start_dt + timedelta(hours=hours)
    end_time = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""SELECT
    toStartOfMinute(timestampMs) as minute,
    SUM({mapping['target_physical_column']}) as metric_sum
FROM {NEW_SCHEMA_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND {mapping['target_physical_column']} > 0
  AND timestampMs >= toDateTime('{DATE_START}')
  AND timestampMs < toDateTime('{end_time}')
GROUP BY minute
ORDER BY minute"""
    return query


def sanitize_filename(text: str) -> str:
    """Sanitize text for use in filename."""
    return text.replace("_", "-")


def save_query(query: str, filepath: str):
    """Save query to file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        f.write(query)


def load_key_mapping() -> Dict:
    """Load the global key mapping file."""
    from src.config.settings import KEY_MAPPING_FILE
    with open(KEY_MAPPING_FILE, "r") as f:
        return json.load(f)


def build_event_count_mappings_from_phase1(phase1_mappings: List[Dict]) -> List[Dict]:
    """
    Use Phase 1 mappings to build event_count mappings for additional patterns.
    Override flowid to 'ALL' so old-schema pattern queries do not filter by flowId
    (matching the desired query shape), while retaining the discovered source_map_column.
    """
    print("Preparing event_count mappings from Phase 1 results...")

    event_count_mappings: List[Dict] = []
    for m in phase1_mappings:
        if m.get("metric_key") == "event_count":
            copy = dict(m)
            copy["flowid"] = "ALL"
            event_count_mappings.append(copy)
            customer_id = copy["customer_id"]
            print(f"  ✓ Customer {customer_id} ({CUSTOMER_NAMES[customer_id]}): "
                  f"event_count -> {copy['target_physical_column']}, source={copy['source_map_column']}")

    print(f"Found {len(event_count_mappings)} event_count mappings")
    print()
    return event_count_mappings


def generate_queries():
    """Main query generation process."""
    print("=" * 80)
    print("Phase 2: Query Generation for Aggregation Benchmark")
    print("=" * 80)
    print()

    # Load mappings
    print("Loading metric mappings from Phase 1...")
    data = load_metric_mappings()
    mappings = data["mappings"]
    print(f"Loaded {len(mappings)} metrics")
    print()

    # Track generated queries
    query_index = []

    # Generate queries for each metric (original Phase 1 metrics)
    for idx, mapping in enumerate(mappings, start=1):
        customer_id = mapping["customer_id"]
        customer_name = CUSTOMER_NAMES[customer_id]
        metric_key = mapping["metric_key"]

        print(f"[{idx}/{len(mappings)}] Generating queries for {customer_name} - {metric_key}")

        # Generate old schema query
        old_query = generate_old_schema_query(mapping)
        old_filename = f"old_schema_{customer_id}_{metric_key}.sql"
        old_filepath = f"output/benchmarks/queries/{old_filename}"
        save_query(old_query, old_filepath)
        print(f"  ✓ Old schema query: {old_filename}")

        # Generate new schema query
        new_query = generate_new_schema_query(mapping)
        new_filename = f"new_schema_{customer_id}_{metric_key}.sql"
        new_filepath = f"output/benchmarks/queries/{new_filename}"
        save_query(new_query, new_filepath)
        print(f"  ✓ New schema query: {new_filename}")

        # Add to index
        query_index.append({
            "metric_number": idx,
            "query_type": "aggregation",
            "customer_id": customer_id,
            "customer_name": customer_name,
            "metric_key": metric_key,
            "flowid": mapping["flowid"],
            "source_map_column": mapping["source_map_column"],
            "target_physical_column": mapping["target_physical_column"],
            "old_schema_query_file": old_filename,
            "new_schema_query_file": new_filename,
            "date_range": f"{DATE_START} to {AGG_DATE_END}"
        })
        print()

    # Generate additional query patterns using event_count
    print("=" * 80)
    print("Generating Additional Query Patterns (event_count)")
    print("=" * 80)
    print()

    # Build event_count mappings from Phase 1 results
    event_count_mappings = build_event_count_mappings_from_phase1(mappings)

    query_patterns = [
        ("groupby_country", "Group by Country", generate_groupby_country_old_schema, generate_groupby_country_new_schema),
        ("groupby_minute", "Group by Minute", generate_groupby_minute_old_schema, generate_groupby_minute_new_schema),
        ("groupby_minute_filtered", "Group by Minute (6h filter)", generate_groupby_minute_filtered_old_schema, generate_groupby_minute_filtered_new_schema),
    ]

    pattern_idx = len(mappings) + 1

    for event_mapping in event_count_mappings:
        customer_id = event_mapping["customer_id"]
        customer_name = CUSTOMER_NAMES[customer_id]

        for pattern_type, pattern_name, old_func, new_func in query_patterns:
            print(f"[{pattern_idx}] {pattern_name}: {customer_name} - event_count")

            # Generate old schema query
            old_query = old_func(event_mapping)
            old_filename = f"{pattern_type}_old_schema_{customer_id}_event_count.sql"
            old_filepath = f"output/benchmarks/queries/{old_filename}"
            save_query(old_query, old_filepath)
            print(f"  ✓ Old schema query: {old_filename}")

            # Generate new schema query
            new_query = new_func(event_mapping)
            new_filename = f"{pattern_type}_new_schema_{customer_id}_event_count.sql"
            new_filepath = f"output/benchmarks/queries/{new_filename}"
            save_query(new_query, new_filepath)
            print(f"  ✓ New schema query: {new_filename}")

            # Add to index
            query_index.append({
                "metric_number": pattern_idx,
                "query_type": pattern_type,
                "customer_id": customer_id,
                "customer_name": customer_name,
                "metric_key": "event_count",
                "flowid": event_mapping["flowid"],
                "source_map_column": event_mapping["source_map_column"],
                "target_physical_column": event_mapping["target_physical_column"],
                "old_schema_query_file": old_filename,
                "new_schema_query_file": new_filename,
                "date_range": f"{DATE_START} to {AGG_DATE_END}"
            })

            pattern_idx += 1
            print()

    # Save query index
    total_queries = len(query_index) * 2  # old + new for each
    index_filepath = "output/benchmarks/query_index.json"
    with open(index_filepath, "w") as f:
        json.dump({
            "metadata": {
                "phase": "Phase 2: Query Generation",
                "total_queries": total_queries,
                "total_metrics": len(query_index),
                "original_metrics": len(mappings),
                "additional_patterns": len(query_index) - len(mappings),
                "dimensions": DIMENSIONS,
                "old_schema_table": OLD_SCHEMA_TABLE,
                "new_schema_table": NEW_SCHEMA_TABLE,
                "unified_date_range": f"{DATE_START} to {AGG_DATE_END}"
            },
            "queries": query_index
        }, f, indent=2)

    print("=" * 80)
    print(f"✓ Phase 2 Complete!")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  Original metrics: {len(mappings)}")
    print(f"  Additional patterns: {len(query_index) - len(mappings)}")
    print(f"  Total queries generated: {total_queries} ({len(query_index)} pairs)")
    print(f"  Query files saved to: output/benchmarks/queries/")
    print(f"  Query index saved to: {index_filepath}")
    print()


if __name__ == "__main__":
    generate_queries()
