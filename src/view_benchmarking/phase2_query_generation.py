"""
Phase 2: Query Generation for View Benchmarking

This script:
1. Loads metric mappings from existing benchmarks
2. Loads view definitions from Phase 1
3. Generates direct table queries (baseline using intN columns)
4. Generates through-view queries (using readable metric names)
5. Saves all queries to individual files
6. Creates query index catalog
"""

import json
import os
from typing import Dict

from src.config.settings import (
    CUSTOMER_NAMES,
    TARGET_TABLE,
    UNIFIED_DATE_START,
    UNIFIED_DATE_END,
    DIMENSIONS
)

# Use unified dates for view benchmarking (querying target table)
DATE_START = UNIFIED_DATE_START
DATE_END = UNIFIED_DATE_END


def load_metric_mappings() -> Dict:
    """Load metric mappings from existing benchmark Phase 1."""
    with open("output/benchmarks/metric_mappings.json", "r") as f:
        return json.load(f)


def load_view_definitions() -> Dict:
    """Load view definitions from view benchmark Phase 1."""
    with open("output/view_benchmarks/view_definitions.json", "r") as f:
        return json.load(f)


def get_view_name(customer_id: int) -> str:
    """Get view name for customer."""
    return f"eco_cross_page_customer_{customer_id}_view"


def escape_column_name(name: str) -> str:
    """
    Escape column name for SQL.
    Uses backticks for names with special characters.
    """
    return f"`{name}`"


def generate_direct_table_query(mapping: Dict) -> str:
    """
    Generate query that accesses table directly using primitive column (intN).
    This is the baseline for comparison.
    """
    dimensions_str = ", ".join(DIMENSIONS)

    query = f"""SELECT
    {dimensions_str},
    SUM({mapping['target_physical_column']}) as metric_sum
FROM {TARGET_TABLE}
WHERE customerId = {mapping['customer_id']}
  AND timestampMs >= '{DATE_START}'
  AND timestampMs < '{DATE_END}'
GROUP BY {dimensions_str}"""

    return query


def generate_through_view_query(mapping: Dict, customer_id: int) -> str:
    """
    Generate query that accesses data through view using readable metric name.
    This is what we're testing for overhead.
    """
    dimensions_str = ", ".join(DIMENSIONS)
    view_name = get_view_name(customer_id)
    metric_name = escape_column_name(mapping['metric_key'])

    query = f"""SELECT
    {dimensions_str},
    SUM({metric_name}) as metric_sum
FROM {view_name}
WHERE timestampMs >= '{DATE_START}'
  AND timestampMs < '{DATE_END}'
GROUP BY {dimensions_str}"""

    return query


def save_query(query: str, filepath: str):
    """Save query to file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        f.write(query)


def generate_queries():
    """Main query generation process."""
    print("=" * 80)
    print("Phase 2: Query Generation for View Benchmarking")
    print("=" * 80)
    print()

    # Load metric mappings from existing benchmarks
    print("Loading metric mappings from existing benchmarks...")
    metric_data = load_metric_mappings()
    mappings = metric_data["mappings"]
    print(f"Loaded {len(mappings)} metrics")
    print()

    # Load view definitions
    print("Loading view definitions from Phase 1...")
    view_data = load_view_definitions()
    views = view_data["views"]
    print(f"Loaded {len(views)} view definitions")
    print()

    # Track generated queries
    query_index = []

    # Generate queries for each metric
    for idx, mapping in enumerate(mappings, start=1):
        customer_id = mapping["customer_id"]
        customer_name = CUSTOMER_NAMES.get(customer_id, f"Customer {customer_id}")
        metric_key = mapping["metric_key"]

        print(f"[{idx}/{len(mappings)}] Generating queries for {customer_name} - {metric_key}")

        try:
            # Generate direct table query (baseline)
            print(f"  Generating direct table query...")
            direct_query = generate_direct_table_query(mapping)
            direct_filename = f"direct_table_{customer_id}_{metric_key}.sql"
            direct_filepath = f"output/view_benchmarks/queries/{direct_filename}"
            save_query(direct_query, direct_filepath)
            print(f"    ✓ Saved: {direct_filename}")

            # Generate through-view query
            print(f"  Generating through-view query...")
            view_query = generate_through_view_query(mapping, customer_id)
            view_filename = f"through_view_{customer_id}_{metric_key}.sql"
            view_filepath = f"output/view_benchmarks/queries/{view_filename}"
            save_query(view_query, view_filepath)
            print(f"    ✓ Saved: {view_filename}")

            # Add to index
            query_index.append({
                "metric_number": idx,
                "customer_id": customer_id,
                "customer_name": customer_name,
                "metric_key": metric_key,
                "flowid": mapping.get("flowid", "N/A"),
                "source_map_column": mapping.get("source_map_column", "N/A"),
                "target_physical_column": mapping["target_physical_column"],
                "view_name": get_view_name(customer_id),
                "direct_table_query_file": direct_filename,
                "through_view_query_file": view_filename,
                "date_range": f"{DATE_START} to {DATE_END}",
                "dimensions": DIMENSIONS
            })

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import sys
            sys.exit(1)

        print()

    # Save query index
    index_filepath = "output/view_benchmarks/query_index.json"
    output_data = {
        "metadata": {
            "phase": "Phase 2: Query Generation",
            "total_queries": len(mappings) * 2,
            "total_metrics": len(mappings),
            "access_types": ["direct_table", "through_view"],
            "dimensions": DIMENSIONS,
            "target_table": TARGET_TABLE,
            "date_range": f"{DATE_START} to {DATE_END}",
            "comparison": "Direct table access (intN columns) vs View access (metric names)"
        },
        "queries": query_index
    }

    with open(index_filepath, "w") as f:
        json.dump(output_data, f, indent=2)

    print("=" * 80)
    print("✓ Phase 2 Complete!")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  Total queries generated: {len(mappings) * 2} ({len(mappings)} direct + {len(mappings)} view)")
    print(f"  Query files saved to: output/view_benchmarks/queries/")
    print(f"  Query index saved to: {index_filepath}")
    print()
    print("Query types:")
    print(f"  • Direct table: Query {TARGET_TABLE} using intN columns")
    print(f"  • Through view: Query customer views using metric names")
    print()


if __name__ == "__main__":
    generate_queries()
