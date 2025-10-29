"""
Centralized Data Transformation Pipeline Orchestrator

This script provides a single entry point for running the complete data transformation pipeline.
Configure which phases to run by setting the flags below.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from src.config.settings import CUSTOMERS, TRANSFORMATION_DATE, KEY_MAPPING_FILE
from src.data_transformation.transformation.simple_key_discovery import SimpleKeyDiscoverer
from src.data_transformation.transformation.schema_generator import SchemaGenerator
from src.data_transformation.transformation.data_transformer import DataTransformer


def setup_logging():
    """Setup logging configuration."""
    log_file = "output/reports/transformation.log"  # Always use the same log file
    Path("output/reports").mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),  # 'w' mode overwrites previous log
            logging.StreamHandler(sys.stdout)
        ],
        force=True  # Force reconfiguration in case logging was already set up
    )
    return log_file


def run_pipeline():
    """Run the complete data transformation pipeline."""
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("STARTING DATA MODEL V3 TRANSFORMATION PIPELINE")
    logger.info("=" * 80)

    # ============================================================
    # PIPELINE CONFIGURATION - Change these to control execution
    # ============================================================
    RUN_KEY_DISCOVERY = True # Control whether to run key discovery (set to False if key mapping already exists)
    DROP_TARGET_TABLE_BEFORE_CREATE = True # Set to False to not drop target table before starting pipeline
    TRUNCATE_TARGET_TABLE = True  # Set to False to continue from where you left off
    START_FROM_CHUNK = 0  # Start from chunk N
    # ============================================================

    FIXED_DATE = TRANSFORMATION_DATE  # Import from settings.py
    CUSTOMER_IDS = [str(cid) for cid in CUSTOMERS]

    pipeline_stats = {
        "started_at": datetime.now().isoformat(),
        "phases": {},
        "overall_success": False
    }

    try:
        # Phase 1: Key Discovery (optional if mapping already exists)
        if RUN_KEY_DISCOVERY:
            logger.info("\n" + "=" * 50)
            logger.info("PHASE 1: KEY DISCOVERY")
            logger.info("=" * 50)

            discoverer = SimpleKeyDiscoverer()
            if not discoverer.test_connection():
                raise Exception("Source database connection failed")

            # Discover per-customer keys and build nested mapping
            customer_keys = {}
            for cid in CUSTOMER_IDS:
                int_keys, float_keys = discoverer.discover_all_keys(FIXED_DATE, cid)
                customer_keys[cid] = (int_keys, float_keys)
                logger.info(f"Customer {cid}: discovered {len(int_keys)} int keys, {len(float_keys)} float keys")

            nested_mapping = discoverer.generate_nested_mapping(customer_keys)
            discoverer.save_mapping(nested_mapping, KEY_MAPPING_FILE)

            pipeline_stats["phases"]["key_discovery"] = {
                "status": "SUCCESS",
                "customers": {str(cid): {"int_keys": len(customer_keys[cid][0]), "float_keys": len(customer_keys[cid][1])} for cid in CUSTOMER_IDS},
                "max_int_columns": nested_mapping["max_columns"]["int_columns"],
                "max_float_columns": nested_mapping["max_columns"]["float_columns"]
            }
            logger.info(
                f"✓ Key discovery completed across {len(CUSTOMER_IDS)} customers: max {nested_mapping['max_columns']['int_columns']} int cols, {nested_mapping['max_columns']['float_columns']} float cols"
            )
        else:
            logger.info("\n" + "=" * 50)
            logger.info("PHASE 1: KEY DISCOVERY - SKIPPED")
            logger.info("=" * 50)
            logger.info("✓ Using existing key mapping file")
            pipeline_stats["phases"]["key_discovery"] = {
                "status": "SKIPPED",
                "reason": "Using existing key mapping"
            }

        # Phase 2: Schema Generation
        logger.info("\n" + "=" * 50)
        logger.info("PHASE 2: SCHEMA GENERATION")
        logger.info("=" * 50)

        generator = SchemaGenerator(KEY_MAPPING_FILE, drop_before_create=DROP_TARGET_TABLE_BEFORE_CREATE)
        if not generator.create_target_table():
            raise Exception("Target table creation failed")

        table_info = generator.get_table_info()
        with open("output/reports/table_info.json", 'w') as f:
            json.dump(table_info, f, indent=2)

        pipeline_stats["phases"]["schema_generation"] = {
            "status": "SUCCESS",
            "total_columns": table_info.get("total_columns", 0),
            "int_columns": table_info.get("int_columns", 0),
            "float_columns": table_info.get("float_columns", 0)
        }
        logger.info(f"✓ Schema generation completed: {table_info.get('total_columns', 0)} total columns")

        # Phase 3: Data Transformation
        logger.info("\n" + "=" * 50)
        logger.info("PHASE 3: DATA TRANSFORMATION")
        logger.info("=" * 50)

        transformer = DataTransformer(KEY_MAPPING_FILE)
        if not transformer.test_connections():
            raise Exception("Database connection tests failed")

        # Transform data for each customer sequentially
        all_transform_stats = {}
        for idx, cid in enumerate(CUSTOMER_IDS):
            truncate = TRUNCATE_TARGET_TABLE if idx == 0 else False
            logger.info("\n" + "-" * 50)
            logger.info(f"Processing customer {cid} (truncate_target={truncate})")
            stats = transformer.transform_day_data(
                FIXED_DATE,
                customer_id=cid,
                truncate_target=truncate,
                start_from_chunk=START_FROM_CHUNK
            )
            all_transform_stats[str(cid)] = stats

        with open("output/reports/transformation_stats.json", 'w') as f:
            json.dump(all_transform_stats, f, indent=2)

        # Aggregate summary for pipeline phase
        total_source = sum(s.get("source_rows_processed", 0) for s in all_transform_stats.values())
        total_target = sum(s.get("target_rows_inserted", 0) for s in all_transform_stats.values())
        total_chunks = sum(s.get("chunks_processed", 0) for s in all_transform_stats.values())
        total_errors = sum(len(s.get("errors", [])) for s in all_transform_stats.values())

        pipeline_stats["phases"]["data_transformation"] = {
            "status": "SUCCESS" if total_errors == 0 else "COMPLETED_WITH_ERRORS",
            "customers": list(map(str, CUSTOMER_IDS)),
            "source_rows_processed": total_source,
            "target_rows_inserted": total_target,
            "chunks_processed": total_chunks,
            "errors": total_errors
        }
        logger.info(f"✓ Data transformation completed across {len(CUSTOMER_IDS)} customers: {total_source} source rows → {total_target} target rows")

        pipeline_stats["overall_success"] = True

    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}")
        pipeline_stats["error"] = str(e)
        pipeline_stats["overall_success"] = False

    finally:
        pipeline_stats["completed_at"] = datetime.now().isoformat()

        # Save final pipeline stats
        with open("output/reports/pipeline_stats.json", 'w') as f:
            json.dump(pipeline_stats, f, indent=2)

        # Print final summary
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)

        for phase_name, phase_stats in pipeline_stats.get("phases", {}).items():
            status_icon = "✓" if phase_stats["status"] == "SUCCESS" else ("⚠" if "ERROR" in phase_stats["status"] else "!")
            logger.info(f"{status_icon} {phase_name.upper().replace('_', ' ')}: {phase_stats['status']}")

        if pipeline_stats["overall_success"]:
            logger.info(f"\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        else:
            logger.error(f"\n💥 PIPELINE FAILED!")

        logger.info(f"\nDetailed reports available in: output/reports/")
        logger.info("=" * 80)

    return pipeline_stats


def main():
    """Main function."""
    log_file = setup_logging()
    logger = logging.getLogger(__name__)

    logger.info(f"Log file: {log_file}")
    logger.info("Data Model V3 Physical Schema Transformation")

    # Run complete pipeline
    stats = run_pipeline()

    # Exit with appropriate code
    sys.exit(0 if stats["overall_success"] else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print()
        print("❌ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print()
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
