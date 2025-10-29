"""
Data transformation pipeline for flattening Map columns to primitive columns.
"""

import json
import logging
from typing import Dict, List, Tuple, Any
from datetime import datetime, timedelta

from ..database.connection import ClickHouseConnection
from src.config.settings import SOURCE_DB, TARGET_DB, SOURCE_TABLE, TARGET_TABLE


logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms data from source to target schema."""

    def __init__(self, mapping_file: str):
        self.source_conn = ClickHouseConnection(SOURCE_DB)
        self.target_conn = ClickHouseConnection(TARGET_DB)
        self.mapping = self._load_mapping(mapping_file)
        # Validate per-customer mapping format
        if not isinstance(self.mapping, dict) or 'max_columns' not in self.mapping or 'customers' not in self.mapping:
            raise RuntimeError("Unsupported mapping format detected. Expected per-customer mapping with 'max_columns' and 'customers'.")
        # Store customers mapping for per-customer lookups during transformation
        self.customers_mapping = self.mapping.get('customers', {})
        self.max_int_cols = self.mapping['max_columns'].get('int_columns', 0)
        self.max_float_cols = self.mapping['max_columns'].get('float_columns', 0)

    def _load_mapping(self, mapping_file: str) -> Dict:
        """Load key mapping from JSON file."""
        logger.info(f"Loading key mapping from {mapping_file}")
        with open(mapping_file, 'r') as f:
            mapping = json.load(f)
        return mapping

    def transform_day_data(self, fixed_date: str, customer_id: int, truncate_target: bool = True, start_from_chunk: int = 0) -> Dict[str, Any]:
        """
        Transform data for a fixed day to ensure idempotency.
        Uses time-based chunking (10-minute intervals) with database-side aggregation.

        Args:
            fixed_date: Date to process (YYYY-MM-DD format)
            customer_id: Customer ID to process
            truncate_target: Whether to truncate target table before processing (default: True)
            start_from_chunk: Chunk index to start from (0-based, default: 0)

        Returns:
            Transformation statistics
        """
        FIXED_DATE = fixed_date
        CUSTOMER_ID = customer_id
        CHUNK_MINUTES = 1

        logger.info(f"Starting transformation for fixed date: {FIXED_DATE}")
        logger.info(f"Processing customer ID: {CUSTOMER_ID}")
        logger.info(f"Using {CHUNK_MINUTES}-minute chunks to manage memory usage")

        # Conditionally truncate target table
        if truncate_target:
            logger.info("Truncating destination table for fresh data load...")
            self.target_conn.execute_command(f"TRUNCATE TABLE {TARGET_TABLE}")
            logger.info("Destination table truncated successfully")
        else:
            logger.info("Skipping table truncation - continuing from existing data")

        stats = {
            "date": FIXED_DATE,
            "customer_id": CUSTOMER_ID,
            "chunk_minutes": CHUNK_MINUTES,
            "start_from_chunk": start_from_chunk,
            "truncated_target": truncate_target,
            "start_time": datetime.now().isoformat(),
            "source_rows_processed": 0,
            "target_rows_inserted": 0,
            "chunks_processed": 0,
            "raw_rows_before_agg": 0,
            "agg_rows_after_agg": 0,
            "compression_factor_overall": 0.0,
            "errors": []
        }

        try:
            chunks_per_hour = 60 // CHUNK_MINUTES
            total_chunks = 24 * chunks_per_hour

            logger.info(f"Starting from chunk {start_from_chunk + 1} (of {total_chunks} total chunks)")

            for chunk_idx in range(start_from_chunk, total_chunks):
                hour = chunk_idx // chunks_per_hour
                chunk_within_hour = chunk_idx % chunks_per_hour
                minute = chunk_within_hour * CHUNK_MINUTES

                start_time = f"{FIXED_DATE} {hour:02d}:{minute:02d}:00"

                # Calculate end time: start + chunk_minutes - 1 second
                end_minute = minute + CHUNK_MINUTES - 1
                end_time = f"{FIXED_DATE} {hour:02d}:{end_minute:02d}:59.999"

                logger.info(f"Processing chunk {chunk_idx + 1}/{total_chunks}: {start_time} to {end_time}")

                # Fetch data for this window and calculate stats
                raw_count = self._count_chunk_raw_rows(start_time, end_time, CUSTOMER_ID)
                chunk_rows = self._fetch_chunk_aggregated(start_time, end_time, CUSTOMER_ID)
                agg_count = len(chunk_rows)
                stats["raw_rows_before_agg"] += raw_count
                stats["agg_rows_after_agg"] += agg_count
                if agg_count > 0:
                    factor = raw_count / agg_count
                    logger.info(f"  Compression for chunk: {raw_count} raw → {agg_count} aggregated rows ({factor:.2f}x)")
                else:
                    logger.info(f"  Compression for chunk: {raw_count} raw → {agg_count} aggregated rows (N/A)")

                if not chunk_rows:
                    logger.info(f"  No data found for chunk {chunk_idx + 1}")
                    continue

                stats["source_rows_processed"] += len(chunk_rows)
                stats["chunks_processed"] += 1

                # Transform pre-aggregated data to target schema
                transformed_data = self._transform_chunk(chunk_rows)

                # Insert into target table
                if transformed_data:
                    self._insert_batch(transformed_data)
                    stats["target_rows_inserted"] += len(transformed_data)

                logger.info(f"  Chunk {chunk_idx + 1} completed: {len(chunk_rows)} source rows → {len(transformed_data)} target rows")

        except Exception as e:
            error_msg = f"Transformation failed: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)

        stats["end_time"] = datetime.now().isoformat()

        # Overall compression summary
        if stats["agg_rows_after_agg"] > 0:
            stats["compression_factor_overall"] = stats["raw_rows_before_agg"] / stats["agg_rows_after_agg"]
            logger.info(
                f"Overall compression: {stats['raw_rows_before_agg']} raw → {stats['agg_rows_after_agg']} aggregated rows "
                f"({stats['compression_factor_overall']:.2f}x)"
            )
        else:
            logger.info(
                f"Overall compression: {stats['raw_rows_before_agg']} raw → {stats['agg_rows_after_agg']} aggregated rows (N/A)"
            )

        logger.info(f"Transformation completed: {stats['chunks_processed']}/{total_chunks} chunks processed")
        return stats

    def _fetch_chunk_aggregated(self, start_time: str, end_time: str, customer_id: int) -> List[Tuple]:
        """Fetch all pre-aggregated data for a specific time chunk and customer."""
        query = f"""
        SELECT
            toStartOfMinute(timestampMs) as timestampMs_rounded,
            clientId,
            sessionId,
            customerId,
            any(inSession) as inSession,
            any(userSessionId) as userSessionId,
            any(inUserSession) as inUserSession,
            any(platform) as platform,
            any(platformSubcategory) as platformSubcategory,
            any(appName) as appName,
            any(appBuild) as appBuild,
            any(appVersion) as appVersion,
            any(browserName) as browserName,
            any(browserVersion) as browserVersion,
            any(userId) as userId,
            any(deviceManufacturer) as deviceManufacturer,
            any(deviceMarketingName) as deviceMarketingName,
            any(deviceModel) as deviceModel,
            any(deviceHardwareType) as deviceHardwareType,
            any(deviceName) as deviceName,
            any(deviceCategory) as deviceCategory,
            any(deviceOperatingSystem) as deviceOperatingSystem,
            any(deviceOperatingSystemVersion) as deviceOperatingSystemVersion,
            any(deviceOperatingSystemFamily) as deviceOperatingSystemFamily,
            any(country) as country,
            any(state) as state,
            any(city) as city,
            any(countryIso) as countryIso,
            any(sub1Iso) as sub1Iso,
            any(sub2Iso) as sub2Iso,
            any(cityGid) as cityGid,
            any(dma) as dma,
            any(postalCode) as postalCode,
            any(isp) as isp,
            any(netSpeed) as netSpeed,
            any(sensorVersion) as sensorVersion,
            any(appType) as appType,
            any(asn) as asn,
            any(timezoneOffsetMins) as timezoneOffsetMins,
            any(connType) as connType,
            any(watermarkMs) as watermarkMs,
            any(partitionId) as partitionId,
            any(retentionDate) as retentionDate,
            sumMap(metricIntGroup1) as metricIntGroup1,
            sumMap(metricIntGroup2) as metricIntGroup2,
            sumMap(metricIntGroup3) as metricIntGroup3,
            sumMap(metricIntGroup4) as metricIntGroup4,
            sumMap(metricIntGroup5) as metricIntGroup5,
            sumMap(metricIntGroup6) as metricIntGroup6,
            sumMap(metricIntGroup7) as metricIntGroup7,
            sumMap(metricIntGroup8) as metricIntGroup8,
            sumMap(metricIntGroup9) as metricIntGroup9,
            sumMap(metricIntGroup10) as metricIntGroup10,
            sumMap(metricIntGroup11) as metricIntGroup11,
            sumMap(metricIntGroup12) as metricIntGroup12,
            sumMap(metricIntGroup13) as metricIntGroup13,
            sumMap(metricIntGroup14) as metricIntGroup14,
            sumMap(metricIntGroup15) as metricIntGroup15,
            sumMap(metricFloatGroup1) as metricFloatGroup1,
            sumMap(metricFloatGroup2) as metricFloatGroup2,
            sumMap(metricFloatGroup3) as metricFloatGroup3,
            sumMap(metricFloatGroup4) as metricFloatGroup4,
            sumMap(metricFloatGroup5) as metricFloatGroup5,
            sumMap(metricFloatGroup6) as metricFloatGroup6,
            sumMap(metricFloatGroup7) as metricFloatGroup7,
            sumMap(metricFloatGroup8) as metricFloatGroup8,
            sumMap(metricFloatGroup9) as metricFloatGroup9,
            sumMap(metricFloatGroup10) as metricFloatGroup10,
            sumMap(metricFloatGroup11) as metricFloatGroup11,
            sumMap(metricFloatGroup12) as metricFloatGroup12,
            sumMap(metricFloatGroup13) as metricFloatGroup13,
            sumMap(metricFloatGroup14) as metricFloatGroup14,
            sumMap(metricFloatGroup15) as metricFloatGroup15
        FROM {SOURCE_TABLE}
        WHERE timestampMs >= '{start_time}'
          AND timestampMs <= '{end_time}'
          AND customerId = {customer_id}
        GROUP BY timestampMs_rounded, clientId, sessionId, customerId
        ORDER BY clientId, sessionId, timestampMs_rounded
        """

        result = self.source_conn.execute_query(query)
        logger.info(f"  Fetched {len(result.result_rows)} aggregated rows for time chunk")
        return result.result_rows

    def _count_chunk_raw_rows(self, start_time: str, end_time: str, customer_id: int) -> int:
        """Count raw (pre-aggregation) rows for a specific time chunk and customer."""
        query = f"""
        SELECT COUNT(*)
        FROM {SOURCE_TABLE}
        WHERE timestampMs >= '{start_time}'
          AND timestampMs <= '{end_time}'
          AND customerId = {customer_id}
        """

        result = self.source_conn.execute_query(query)
        return result.result_rows[0][0] if result.result_rows else 0

    def _transform_chunk(self, source_rows: List[Tuple]) -> List[Dict[str, Any]]:
        """
        Transform pre-aggregated source rows to target schema.
        Data is already aggregated by database, so just convert Map columns to primitive columns.
        Uses per-customer mapping for column assignment.
        """
        if not source_rows:
            return []

        # Get customer ID from first row (all rows in chunk are same customer)
        customer_id = source_rows[0][3]  # customerId is at position 3
        customer_mapping = self.customers_mapping.get(str(customer_id))

        if not customer_mapping:
            raise ValueError(f"No mapping found for customer {customer_id}")

        # Get customer-specific mappings
        int_mapping = customer_mapping.get('int_mapping', {})
        float_mapping = customer_mapping.get('float_mapping', {})

        transformed_rows = []

        for row in source_rows:
            # Build row dictionary with metadata
            row_dict = {
                "timestampMs": row[0],
                "clientId": row[1],
                "sessionId": row[2],
                "customerId": row[3],
                "inSession": row[4],
                "userSessionId": row[5],
                "inUserSession": row[6],
                "platform": row[7],
                "platformSubcategory": row[8],
                "appName": row[9],
                "appBuild": row[10],
                "appVersion": row[11],
                "browserName": row[12],
                "browserVersion": row[13],
                "userId": row[14],
                "deviceManufacturer": row[15],
                "deviceMarketingName": row[16],
                "deviceModel": row[17],
                "deviceHardwareType": row[18],
                "deviceName": row[19],
                "deviceCategory": row[20],
                "deviceOperatingSystem": row[21],
                "deviceOperatingSystemVersion": row[22],
                "deviceOperatingSystemFamily": row[23],
                "country": row[24],
                "state": row[25],
                "city": row[26],
                "countryIso": row[27],
                "sub1Iso": row[28],
                "sub2Iso": row[29],
                "cityGid": row[30],
                "dma": row[31],
                "postalCode": row[32],
                "isp": row[33],
                "netSpeed": row[34],
                "sensorVersion": row[35],
                "appType": row[36],
                "asn": row[37],
                "timezoneOffsetMins": row[38],
                "connType": row[39],
                "watermarkMs": row[40],
                "partitionId": row[41],
                "retentionDate": row[42]
            }

            # Initialize all primitive columns with default values (up to max across all customers)
            for i in range(1, self.max_int_cols + 1):
                row_dict[f"int{i}"] = 0
            for i in range(1, self.max_float_cols + 1):
                row_dict[f"float{i}"] = 0.0

            # Process all metric int groups (metricIntGroup1-15: positions 43-57)
            for i, map_data in enumerate(row[43:58], 1):  # metricIntGroup1-15
                if map_data:
                    for key, value in map_data.items():
                        if key in int_mapping:
                            column_name = int_mapping[key]
                            row_dict[column_name] = value

            # Process all metric float groups (metricFloatGroup1-15: positions 58-72)
            for i, map_data in enumerate(row[58:73], 1):  # metricFloatGroup1-15
                if map_data:
                    for key, value in map_data.items():
                        if key in float_mapping:
                            column_name = float_mapping[key]
                            row_dict[column_name] = value

            transformed_rows.append(row_dict)

        logger.info(f"Transformed {len(source_rows)} pre-aggregated rows into {len(transformed_rows)} target rows")
        return transformed_rows

    def _insert_batch(self, transformed_data: List[Dict[str, Any]]):
        """Insert transformed data batch into target table."""
        if not transformed_data:
            return

        logger.info(f"Inserting {len(transformed_data)} rows into target table")
        columns = list(transformed_data[0].keys())
        values_data = []
        for row in transformed_data:
            values_data.append([row[col] for col in columns])

        try:
            with self.target_conn.get_connection() as client:
                client.insert(TARGET_TABLE, values_data, column_names=columns)
            logger.info(f"Successfully inserted {len(transformed_data)} rows")
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            raise

    def test_connections(self) -> bool:
        """Test both source and target connections."""
        logger.info("Testing database connections...")
        source_ok = self.source_conn.test_connection()
        target_ok = self.target_conn.test_connection()

        if source_ok and target_ok:
            logger.info("All database connections successful")
            return True
        else:
            logger.error(f"Connection tests failed - Source: {source_ok}, Target: {target_ok}")
            return False


def main():
    """Main function for data transformation."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    transformer = DataTransformer("output/mappings/key_mapping.json")

    # Test connections
    if not transformer.test_connections():
        logger.error("Database connection tests failed")
        return

    # Run transformation with fixed date for idempotency
    stats = transformer.transform_day_data(limit=10000)

    # Save transformation stats
    stats_file = "output/reports/transformation_stats.json"
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Transformation completed! Stats saved to {stats_file}")
    logger.info(f"Processed {stats['source_rows_processed']} source rows")
    logger.info(f"Inserted {stats['target_rows_inserted']} target rows")
    logger.info(f"Processed {stats['batches_processed']} batches")

    if stats["errors"]:
        logger.error(f"Encountered {len(stats['errors'])} errors")
        for error in stats["errors"]:
            logger.error(f"  - {error}")


if __name__ == "__main__":
    main()