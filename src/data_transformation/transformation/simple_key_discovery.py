"""
Simple key discovery with small sample size.
"""

import json
import logging
from typing import Set, Dict, List, Tuple
from datetime import datetime, timedelta

from ..database.connection import ClickHouseConnection
from src.config.settings import SOURCE_DB, SOURCE_TABLE, TRANSFORMATION_DATE, CUSTOMERS


logger = logging.getLogger(__name__)


class SimpleKeyDiscoverer:
    """Simple key discovery with small samples."""

    def __init__(self):
        self.source_conn = ClickHouseConnection(SOURCE_DB)

    def discover_all_keys(self, fixed_date: str, customer_id: int) -> Tuple[List[str], List[str]]:
        """
        Discover all keys using database-side aggregation with customer filtering and chunking.

        Args:
            fixed_date: Date to process (YYYY-MM-DD format)
            customer_id: Customer ID to process

        Returns:
            Tuple of (sorted_int_keys, sorted_float_keys)
        """

        FIXED_DATE = fixed_date
        CUSTOMER_ID = customer_id
        CHUNK_MINUTES = 1  # Configure chunk size here (must evenly divide 60: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60)

        if 60 % CHUNK_MINUTES != 0:
            raise ValueError(f"CHUNK_MINUTES must evenly divide 60. Got {CHUNK_MINUTES}")

        logger.info(f"Starting key discovery from {SOURCE_TABLE} for fixed date: {FIXED_DATE}")
        logger.info(f"Using database-side aggregation with customer filter: {CUSTOMER_ID}")
        logger.info(f"Using time-based chunking ({CHUNK_MINUTES}-minute chunks) with aggregation")

        int_keys = set()
        float_keys = set()

        # Calculate chunks per hour and total chunks
        chunks_per_hour = 60 // CHUNK_MINUTES
        total_chunks = 24 * chunks_per_hour
        successful_chunks = 0

        logger.info(f"Processing {total_chunks} chunks ({chunks_per_hour} chunks per hour)")

        for chunk_idx in range(total_chunks):
            hour = chunk_idx // chunks_per_hour
            chunk_within_hour = chunk_idx % chunks_per_hour
            minute = chunk_within_hour * CHUNK_MINUTES

            chunk_start = f"{FIXED_DATE} {hour:02d}:{minute:02d}:00"

            # End time is (chunk_minutes - 1) minutes 59 seconds later
            end_minute = minute + CHUNK_MINUTES - 1
            chunk_end = f"{FIXED_DATE} {hour:02d}:{end_minute:02d}:59"

            logger.info(f"Processing chunk {chunk_idx + 1}/{total_chunks}: {chunk_start} to {chunk_end}")

            # Database-side aggregation query - returns unique keys per column
            query = f"""
            SELECT
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup1)))) as int1_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup2)))) as int2_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup3)))) as int3_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup4)))) as int4_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup5)))) as int5_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup6)))) as int6_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup7)))) as int7_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup8)))) as int8_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup9)))) as int9_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup10)))) as int10_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup11)))) as int11_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup12)))) as int12_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup13)))) as int13_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup14)))) as int14_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricIntGroup15)))) as int15_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup1)))) as float1_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup2)))) as float2_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup3)))) as float3_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup4)))) as float4_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup5)))) as float5_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup6)))) as float6_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup7)))) as float7_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup8)))) as float8_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup9)))) as float9_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup10)))) as float10_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup11)))) as float11_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup12)))) as float12_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup13)))) as float13_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup14)))) as float14_keys,
                arrayDistinct(arrayFlatten(groupArray(mapKeys(metricFloatGroup15)))) as float15_keys
            FROM {SOURCE_TABLE}
            WHERE timestampMs >= '{chunk_start}'
              AND timestampMs <= '{chunk_end}'
              AND customerId = {CUSTOMER_ID}
            """

            try:
                result = self.source_conn.execute_query(query)

                if len(result.result_rows) != 1:
                    logger.warning(f"  Chunk {chunk_idx + 1}: Expected 1 row, got {len(result.result_rows)}")
                    continue

                row = result.result_rows[0]
                logger.info(f"  Chunk {chunk_idx + 1}: Database aggregation completed.")

                # Process int groups (15 columns: int1_keys to int15_keys)
                chunk_int_keys = 0
                for i in range(15):  # metricIntGroup1-15
                    keys = row[i] if row[i] else []
                    for key in keys:
                        if key and key.strip():
                            int_keys.add(key)
                            chunk_int_keys += 1

                # Process float groups (15 columns: float1_keys to float15_keys)
                chunk_float_keys = 0
                for i in range(15, 30):  # metricFloatGroup1-15
                    keys = row[i] if row[i] else []
                    for key in keys:
                        if key and key.strip():
                            float_keys.add(key)
                            chunk_float_keys += 1

                successful_chunks += 1
                logger.info(f"  Chunk {chunk_idx + 1} completed: +{chunk_int_keys} int, +{chunk_float_keys} float keys")
                logger.info(f"  Total so far: {len(int_keys)} int keys, {len(float_keys)} float keys")

            except Exception as e:
                logger.warning(f"  Chunk {chunk_idx + 1} failed: {e}")
                continue

        logger.info(f"Database aggregation processing completed:")
        logger.info(f"  Successful chunks: {successful_chunks}/{total_chunks}")
        logger.info(f"  Customer ID: {CUSTOMER_ID}")
        logger.info(f"  Final unique int keys: {len(int_keys)}")
        logger.info(f"  Final unique float keys: {len(float_keys)}")

        if successful_chunks == 0:
            raise Exception("No chunks processed successfully")

        sorted_int_keys = sorted(list(int_keys))
        sorted_float_keys = sorted(list(float_keys))

        logger.info(f"Discovered {len(sorted_int_keys)} unique integer keys and {len(sorted_float_keys)} unique float keys")

        return sorted_int_keys, sorted_float_keys

    def generate_nested_mapping(self, customer_keys: Dict[int, Tuple[List[str], List[str]]]) -> Dict:
        """
        Generate nested mapping with per-customer mappings (not global union).
        Each customer independently maps their metrics to int1, int2, int3...

        Args:
            customer_keys: Dict of customer_id -> (int_keys, float_keys)

        Returns:
            Nested mapping dict with 'max_columns' and per-customer mappings.
        """
        customers_block: Dict[str, Dict] = {}
        max_int_cols = 0
        max_float_cols = 0

        # Build per-customer mappings independently
        for cid, (int_keys, float_keys) in customer_keys.items():
            sorted_int = sorted(set(int_keys))
            sorted_float = sorted(set(float_keys))

            # Create customer-specific mapping (starting from int1, float1)
            int_mapping: Dict[str, str] = {}
            reverse_int_mapping: Dict[str, str] = {}
            for i, key in enumerate(sorted_int, 1):
                col = f"int{i}"
                int_mapping[key] = col
                reverse_int_mapping[col] = key

            float_mapping: Dict[str, str] = {}
            reverse_float_mapping: Dict[str, str] = {}
            for i, key in enumerate(sorted_float, 1):
                col = f"float{i}"
                float_mapping[key] = col
                reverse_float_mapping[col] = key

            customers_block[str(cid)] = {
                "int_keys": sorted_int,
                "float_keys": sorted_float,
                "int_mapping": int_mapping,
                "float_mapping": float_mapping,
                "reverse_int_mapping": reverse_int_mapping,
                "reverse_float_mapping": reverse_float_mapping,
                "int_columns": len(sorted_int),
                "float_columns": len(sorted_float)
            }

            # Track maximum columns needed
            max_int_cols = max(max_int_cols, len(sorted_int))
            max_float_cols = max(max_float_cols, len(sorted_float))

        nested = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "source_table": SOURCE_TABLE,
                "note": "Per-customer mappings; each customer independently maps metrics to column positions starting from int1/float1. Table uses max columns across all customers."
            },
            "max_columns": {
                "int_columns": max_int_cols,
                "float_columns": max_float_cols
            },
            "customers": customers_block
        }

        return nested

    def save_mapping(self, mapping: Dict, output_path: str):
        """Save the key mapping to a JSON file."""
        logger.info(f"Saving key mapping to {output_path}")
        with open(output_path, 'w') as f:
            json.dump(mapping, f, indent=2, sort_keys=True)
        logger.info("Key mapping saved successfully")

    def test_connection(self) -> bool:
        """Test connection to source database."""
        logger.info("Testing source database connection...")
        return self.source_conn.test_connection()


def main():
    """Main function for simple key discovery."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    discoverer = SimpleKeyDiscoverer()

    # Test connection first
    if not discoverer.test_connection():
        logger.error("Failed to connect to source database")
        return

    # Discover keys across one or more customers for a fixed date
    fixed_date = TRANSFORMATION_DATE  # Import from settings.py
    customer_ids = [str(cid) for cid in CUSTOMERS]  # Import from settings.py

    customer_keys: Dict[int, Tuple[List[str], List[str]]] = {}
    for cid in customer_ids:
        int_keys, float_keys = discoverer.discover_all_keys(fixed_date, cid)
        customer_keys[cid] = (int_keys, float_keys)
        logger.info(f"Customer {cid}: discovered {len(int_keys)} int keys, {len(float_keys)} float keys")

    # Generate nested mapping and save
    nested_mapping = discoverer.generate_nested_mapping(customer_keys)
    discoverer.save_mapping(nested_mapping, "output/mappings/key_mapping.json")

    logger.info("Key discovery completed successfully across customers!")
    logger.info(
        f"Max integer columns: {nested_mapping['max_columns']['int_columns']}, Max float columns: {nested_mapping['max_columns']['float_columns']}"
    )


if __name__ == "__main__":
    main()