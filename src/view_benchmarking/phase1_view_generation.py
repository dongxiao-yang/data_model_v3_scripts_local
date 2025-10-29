"""
Phase 1: View Generation for View Benchmarking

This script:
1. Loads per-customer metric mappings from key_mapping.json
2. Generates CREATE VIEW DDL for each customer
3. Views map primitive columns (intN) back to original metric names
4. Views are filtered by customerId and include only used columns
5. Executes DDL on target database
6. Saves view definitions and metadata to JSON
"""

import json
import os
import sys
from typing import Dict, List
import clickhouse_connect

from src.config.settings import TARGET_DB, TARGET_TABLE, STANDARD_COLUMNS, CUSTOMER_NAMES, KEY_MAPPING_FILE


def load_key_mapping() -> Dict:
    """Load the global key mapping file."""
    with open(KEY_MAPPING_FILE, "r") as f:
        return json.load(f)


def generate_view_name(customer_id: int) -> str:
    """Generate view name for customer."""
    return f"eco_cross_page_customer_{customer_id}_view"


def escape_column_name(name: str) -> str:
    """
    Escape column name for SQL.
    Uses backticks for names with special characters.
    """
    # If name has special characters, wrap in backticks
    if any(c in name for c in ['-', '.', ' ', '/', '*', '+']):
        return f"`{name}`"
    return f"`{name}`"  # Always use backticks for consistency


def generate_view_ddl(
    customer_id: int,
    customer_mapping: Dict
) -> tuple[str, List[Dict]]:
    """
    Generate CREATE VIEW DDL for a customer using per-customer mapping.

    Args:
        customer_id: Customer ID
        customer_mapping: Full customer mapping with int_mapping, float_mapping, etc.

    Returns:
        tuple: (ddl_string, column_mappings)
    """
    view_name = generate_view_name(customer_id)

    # Collect column definitions
    column_defs = []
    column_mappings = []

    # Add all standard columns (pass-through)
    for col in STANDARD_COLUMNS:
        column_defs.append(col)

    # Add metric columns with aliases using per-customer mapping
    int_keys = customer_mapping.get("int_keys", [])
    float_keys = customer_mapping.get("float_keys", [])

    # Get customer-specific mappings
    int_mapping = customer_mapping.get("int_mapping", {})
    float_mapping = customer_mapping.get("float_mapping", {})

    # Map int keys to primitive columns
    for metric_key in sorted(int_keys):  # Sort for consistency
        if metric_key in int_mapping:
            primitive_col = int_mapping[metric_key]
            escaped_name = escape_column_name(metric_key)
            column_defs.append(f"{primitive_col} AS {escaped_name}")
            column_mappings.append({
                "metric_key": metric_key,
                "primitive_column": primitive_col,
                "column_type": "int"
            })

    # Map float keys to primitive columns (if any)
    for metric_key in sorted(float_keys):  # Sort for consistency
        if metric_key in float_mapping:
            primitive_col = float_mapping[metric_key]
            escaped_name = escape_column_name(metric_key)
            column_defs.append(f"{primitive_col} AS {escaped_name}")
            column_mappings.append({
                "metric_key": metric_key,
                "primitive_column": primitive_col,
                "column_type": "float"
            })

    # Build DDL
    columns_str = ",\n    ".join(column_defs)
    ddl = f"""CREATE VIEW {view_name} AS
SELECT
    {columns_str}
FROM {TARGET_TABLE}
WHERE customerId = {customer_id}"""

    return ddl, column_mappings


def create_view_on_database(client: clickhouse_connect.driver.Client, ddl: str, view_name: str):
    """
    Create view on database.
    Drops existing view if it exists.
    """
    # Drop view if it exists
    drop_ddl = f"DROP VIEW IF EXISTS {view_name}"
    print(f"  Dropping existing view (if exists)...")
    try:
        client.command(drop_ddl)
    except Exception as e:
        print(f"    Note: {e}")

    # Create view
    print(f"  Creating view...")
    try:
        client.command(ddl)
        print(f"  ✓ View created successfully")
    except Exception as e:
        print(f"  ❌ Error creating view: {e}")
        raise


def verify_view(client: clickhouse_connect.driver.Client, view_name: str) -> Dict:
    """
    Verify view by querying it.
    Returns row count and sample data.
    """
    print(f"  Verifying view...")

    # Get row count
    count_query = f"SELECT count(*) as cnt FROM {view_name}"
    result = client.query(count_query)
    row_count = result.result_rows[0][0]

    # Get sample row
    sample_query = f"SELECT * FROM {view_name} LIMIT 1"
    result = client.query(sample_query)
    has_data = len(result.result_rows) > 0

    print(f"    Row count: {row_count:,}")
    print(f"    Has data: {has_data}")

    return {
        "row_count": row_count,
        "has_data": has_data
    }


def run_view_generation():
    """Main view generation process."""
    print("=" * 80)
    print("Phase 1: View Generation for View Benchmarking")
    print("=" * 80)
    print()

    # Load key mapping
    print("Loading key mapping...")
    key_mapping = load_key_mapping()
    customers_data = key_mapping.get("customers", {})
    max_columns = key_mapping.get("max_columns", {})
    print(f"Loaded per-customer mapping for {len(customers_data)} customers")
    print(f"Max columns: {max_columns.get('int_columns', 0)} int, {max_columns.get('float_columns', 0)} float")
    print()

    # Connect to target database
    print("Connecting to target database...")
    print(f"  Host: {TARGET_DB.host}:{TARGET_DB.port}")
    print(f"  Table: {TARGET_TABLE}")
    client = clickhouse_connect.get_client(
        host=TARGET_DB.host,
        port=TARGET_DB.port,
        settings={"max_execution_time": TARGET_DB.timeout}
    )
    print("✓ Connected!")
    print()

    # Results storage
    view_definitions = []

    # Process each customer
    for idx, (customer_id_str, customer_mapping) in enumerate(customers_data.items(), start=1):
        customer_id = int(customer_id_str)
        customer_name = CUSTOMER_NAMES.get(customer_id, f"Customer {customer_id}")

        print(f"[{idx}/{len(customers_data)}] Processing {customer_name} (ID: {customer_id})")
        print("-" * 80)

        try:
            # Generate DDL using per-customer mapping
            print(f"  Generating view DDL...")
            view_name = generate_view_name(customer_id)
            ddl, column_mappings = generate_view_ddl(customer_id, customer_mapping)

            int_cols = len([m for m in column_mappings if m["column_type"] == "int"])
            float_cols = len([m for m in column_mappings if m["column_type"] == "float"])
            print(f"    View name: {view_name}")
            print(f"    Columns: {len(STANDARD_COLUMNS)} standard + {int_cols} int + {float_cols} float = {len(STANDARD_COLUMNS) + int_cols + float_cols} total")

            # Create view on database
            create_view_on_database(client, ddl, view_name)

            # Verify view
            verification = verify_view(client, view_name)

            # Store result
            view_definitions.append({
                "customer_id": customer_id,
                "customer_name": customer_name,
                "view_name": view_name,
                "ddl": ddl,
                "column_count": {
                    "standard": len(STANDARD_COLUMNS),
                    "int_metrics": int_cols,
                    "float_metrics": float_cols,
                    "total": len(STANDARD_COLUMNS) + int_cols + float_cols
                },
                "column_mappings": column_mappings,
                "verification": verification
            })

            print(f"  ✓ Complete")

        except Exception as e:
            print(f"  ❌ Error: {e}")
            client.close()
            sys.exit(1)

        print()

    # Close connection
    client.close()

    # Save results
    output_path = "output/view_benchmarks/view_definitions.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    output_data = {
        "metadata": {
            "phase": "Phase 1: View Generation",
            "total_views": len(view_definitions),
            "customers": list(CUSTOMER_NAMES.values()),
            "target_table": TARGET_TABLE,
            "target_database": f"{TARGET_DB.host}:{TARGET_DB.port}"
        },
        "views": view_definitions
    }

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2)

    print("=" * 80)
    print("✓ Phase 1 Complete!")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  Total views created: {len(view_definitions)}")
    print(f"  Views saved to database: {TARGET_DB.host}")
    print(f"  View definitions saved to: {output_path}")
    print()

    # Print view names
    print("Created views:")
    for view_def in view_definitions:
        print(f"  • {view_def['view_name']} ({view_def['customer_name']}) - {view_def['verification']['row_count']:,} rows")
    print()


if __name__ == "__main__":
    run_view_generation()
