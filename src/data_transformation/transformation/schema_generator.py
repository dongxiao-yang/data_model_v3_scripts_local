"""
Schema generator for target table based on discovered keys.
"""

import json
import logging
from typing import Dict, List
from datetime import datetime

from ..database.connection import ClickHouseConnection
from src.config.settings import TARGET_DB, TARGET_TABLE, STANDARD_COLUMNS


logger = logging.getLogger(__name__)


class SchemaGenerator:
    """Generates target table schema based on key mapping."""

    def __init__(self, mapping_file: str, drop_before_create: bool = False):
        self.target_conn = ClickHouseConnection(TARGET_DB)
        self.mapping = self._load_mapping(mapping_file)
        self.drop_before_create = drop_before_create

    def _load_mapping(self, mapping_file: str) -> Dict:
        """Load key mapping from JSON file."""
        logger.info(f"Loading key mapping from {mapping_file}")
        with open(mapping_file, 'r') as f:
            mapping = json.load(f)
        # Validate per-customer format
        if not isinstance(mapping, dict) or "max_columns" not in mapping or "customers" not in mapping:
            raise RuntimeError("Unsupported mapping format detected. Expected per-customer mapping with 'max_columns' and 'customers'.")
        logger.info(
            f"Loaded per-customer mapping with max {mapping['max_columns'].get('int_columns', 0)} int columns and {mapping['max_columns'].get('float_columns', 0)} float columns"
        )
        return mapping

    def generate_create_table_ddl(self) -> str:
        """Generate CREATE TABLE DDL for target table."""
        logger.info(f"Generating CREATE TABLE DDL for {TARGET_TABLE}")

        # Start building DDL
        ddl_parts = [f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ("]

        # Add standard metadata columns (excluding flowId, flowStartTimeMs, tagGroups)
        standard_column_ddls = [
            "timestampMs DateTime64(3) CODEC(ZSTD(1))",
            "customerId Int32 CODEC(ZSTD(1))",
            "clientId String CODEC(ZSTD(1))",
            "sessionId UInt256 CODEC(ZSTD(1))",
            "inSession UInt8 CODEC(ZSTD(1))",
            "userSessionId Int64 CODEC(ZSTD(1))",
            "inUserSession UInt8 CODEC(ZSTD(1))",
            "platform LowCardinality(String) CODEC(ZSTD(1))",
            "platformSubcategory LowCardinality(String) CODEC(ZSTD(1))",
            "appName LowCardinality(String) CODEC(ZSTD(1))",
            "appBuild LowCardinality(String) CODEC(ZSTD(1))",
            "appVersion LowCardinality(String) CODEC(ZSTD(1))",
            "browserName LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "browserVersion LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "userId String DEFAULT '' CODEC(ZSTD(1))",
            "deviceManufacturer LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceMarketingName LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceModel LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceHardwareType LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceName LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceCategory LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceOperatingSystem LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceOperatingSystemVersion LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceOperatingSystemFamily LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "country Int64 DEFAULT 0 CODEC(ZSTD(1))",
            "state Int64 DEFAULT 0 CODEC(ZSTD(1))",
            "city Int64 DEFAULT 0 CODEC(ZSTD(1))",
            "countryIso LowCardinality(String) CODEC(ZSTD(1))",
            "sub1Iso LowCardinality(String) CODEC(ZSTD(1))",
            "sub2Iso String CODEC(ZSTD(1))",
            "cityGid Int32 CODEC(ZSTD(1))",
            "dma Int16 CODEC(ZSTD(1))",
            "postalCode String DEFAULT '' CODEC(ZSTD(1))",
            "isp Int32 DEFAULT 0 CODEC(ZSTD(1))",
            "netSpeed LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "sensorVersion LowCardinality(String) CODEC(ZSTD(1))",
            "appType LowCardinality(String) CODEC(ZSTD(1))",
            "asn Int32 DEFAULT 0 CODEC(ZSTD(1))",
            "timezoneOffsetMins Int32 DEFAULT 0 CODEC(ZSTD(1))",
            "connType Int32 DEFAULT 0 CODEC(ZSTD(1))",
            "watermarkMs DateTime64(3) CODEC(ZSTD(1))",
            "partitionId Int32 CODEC(ZSTD(1))",
            "retentionDate Date CODEC(ZSTD(1))"
        ]

        # Add primitive columns for integer metrics
        int_columns = int(self.mapping['max_columns'].get('int_columns', 0))
        for i in range(1, int_columns + 1):
            column_name = f"int{i}"
            standard_column_ddls.append(f"{column_name} Int32 DEFAULT 0 CODEC(ZSTD(1))")

        # Add primitive columns for float metrics (if any)
        float_columns = int(self.mapping['max_columns'].get('float_columns', 0))
        for i in range(1, float_columns + 1):
            column_name = f"float{i}"
            standard_column_ddls.append(f"{column_name} Float32 DEFAULT 0 CODEC(ZSTD(1))")

        # Join all column definitions
        ddl_parts.append("    " + ",\n    ".join(standard_column_ddls))

        # Close table definition and add engine/partitioning
        ddl_parts.extend([
            ") ENGINE = MergeTree()",
            "PARTITION BY toYYYYMM(timestampMs)",
            "ORDER BY (customerId, clientId, sessionId, timestampMs)",
            "SETTINGS index_granularity = 8192;"
        ])

        ddl = "\n".join(ddl_parts)
        logger.info(f"Generated DDL with {int_columns} int columns and {float_columns} float columns")

        return ddl

    def create_target_table(self) -> bool:
        """Create the target table in the destination database."""
        try:
            # Test connection first
            if not self.target_conn.test_connection():
                logger.error("Failed to connect to target database")
                return False

            # Optionally drop before create
            if self.drop_before_create:
                try:
                    self.target_conn.execute_command(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
                    logger.info("Dropped existing target table before create")
                except Exception as drop_err:
                    logger.warning(f"Failed to drop existing table (continuing): {drop_err}")

            # Generate and execute DDL
            ddl = self.generate_create_table_ddl()
            logger.info(f"Creating target table: {TARGET_TABLE}")

            # Save DDL to file for reference
            ddl_file = f"output/reports/create_table_{TARGET_TABLE.split('.')[-1]}.sql"
            with open(ddl_file, 'w') as f:
                f.write(ddl)
            logger.info(f"DDL saved to {ddl_file}")

            # Execute DDL
            self.target_conn.execute_command(ddl)
            logger.info("Target table created successfully")

            # Verify table exists
            verify_query = f"DESCRIBE TABLE {TARGET_TABLE}"
            result = self.target_conn.execute_query(verify_query)
            column_count = len(result.result_rows)
            logger.info(f"Verified table creation - {column_count} columns")

            return True

        except Exception as e:
            logger.error(f"Failed to create target table: {e}")
            return False

    def get_table_info(self) -> Dict:
        """Get information about the created table."""
        try:
            schema = self.target_conn.get_table_schema(TARGET_TABLE)
            return {
                "table_name": TARGET_TABLE,
                "total_columns": len(schema),
                "int_columns": len([col for col in schema if col['name'].startswith('int')]),
                "float_columns": len([col for col in schema if col['name'].startswith('float')]),
                "metadata_columns": len([col for col in schema if not (col['name'].startswith('int') or col['name'].startswith('float'))]),
                "created_at": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return {}


def main():
    """Main function for schema generation."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    generator = SchemaGenerator("output/mappings/key_mapping.json")

    # Create target table
    if generator.create_target_table():
        # Get and save table info
        table_info = generator.get_table_info()
        if table_info:
            info_file = "output/reports/table_info.json"
            with open(info_file, 'w') as f:
                json.dump(table_info, f, indent=2)
            logger.info(f"Table info saved to {info_file}")

        logger.info("Schema generation completed successfully!")
    else:
        logger.error("Schema generation failed!")


if __name__ == "__main__":
    main()