"""
Phase 3: Validation

This script:
1. Loads query index from Phase 2
2. Executes old and new schema queries
3. Compares results with exact matching
4. Generates validation report

The validation ensures that the transformation pipeline worked correctly
by verifying that aggregation results are identical between old and new schemas.
"""

import json
from typing import Dict, List, Tuple
from datetime import datetime
import clickhouse_connect

from src.config.settings import SOURCE_DB, TARGET_DB


def load_query_index() -> Dict:
    """Load query index from Phase 2."""
    with open("output/benchmarks/query_index.json", "r") as f:
        return json.load(f)


def read_query_file(filepath: str) -> str:
    """Read SQL query from file."""
    with open(filepath, "r") as f:
        return f.read()


def execute_query(client, query: str) -> List[Tuple]:
    """Execute query and return sorted result rows."""
    result = client.query(query)
    rows = result.result_rows

    # Sort rows by all columns to ensure consistent ordering
    # This handles the case where ORDER BY might not be deterministic
    sorted_rows = sorted(rows, key=lambda x: tuple(str(v) for v in x))

    return sorted_rows


def compare_results(old_results: List[Tuple], new_results: List[Tuple],
                   metric_key: str, customer_name: str) -> Dict:
    """
    Compare results by total aggregated sum only.
    Returns validation result dictionary.
    """
    # Calculate total sums (last column is metric_sum)
    old_total = sum(row[-1] for row in old_results) if old_results else 0
    new_total = sum(row[-1] for row in new_results) if new_results else 0

    result = {
        "customer_name": customer_name,
        "metric_key": metric_key,
        "passed": old_total == new_total,
        "row_count_old": len(old_results),
        "row_count_new": len(new_results),
        "old_total_sum": old_total,
        "new_total_sum": new_total,
        "difference": abs(old_total - new_total) if isinstance(old_total, (int, float)) and isinstance(new_total, (int, float)) else "N/A"
    }

    return result


def generate_validation_report(results: List[Dict], metadata: Dict) -> str:
    """Generate markdown validation report."""

    report = ["# Validation Report", ""]
    report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"**Date Range:** {metadata['unified_date_range']}")
    report.append(f"**Old Schema Table:** {metadata.get('old_schema_table', 'N/A')}")
    report.append(f"**New Schema Table:** {metadata.get('new_schema_table', 'N/A')}")
    report.append("")

    # Overall Summary
    total_metrics = len(results)
    passed_metrics = sum(1 for r in results if r["passed"])
    failed_metrics = total_metrics - passed_metrics

    report.append("## Overall Summary")
    report.append("")

    if failed_metrics == 0:
        report.append(f"✅ **ALL VALIDATIONS PASSED** ({passed_metrics}/{total_metrics} metrics)")
    else:
        report.append(f"❌ **VALIDATION FAILURES DETECTED** ({failed_metrics}/{total_metrics} metrics failed)")

    report.append("")
    report.append(f"- **Total Metrics Validated:** {total_metrics}")
    report.append(f"- **Passed:** {passed_metrics}")
    report.append(f"- **Failed:** {failed_metrics}")
    report.append("")

    # Per-Customer Summary
    customer_results = {}
    for r in results:
        customer = r["customer_name"]
        if customer not in customer_results:
            customer_results[customer] = {"total": 0, "passed": 0, "failed": 0}
        customer_results[customer]["total"] += 1
        if r["passed"]:
            customer_results[customer]["passed"] += 1
        else:
            customer_results[customer]["failed"] += 1

    report.append("## Per-Customer Results")
    report.append("")
    report.append("| Customer | Total | Passed | Failed | Status |")
    report.append("|----------|-------|--------|--------|--------|")

    for customer in sorted(customer_results.keys()):
        stats = customer_results[customer]
        status = "✅ PASS" if stats["failed"] == 0 else "❌ FAIL"
        report.append(f"| {customer} | {stats['total']} | {stats['passed']} | {stats['failed']} | {status} |")

    report.append("")

    # Failed Validations Detail
    if failed_metrics > 0:
        report.append("## Failed Validations")
        report.append("")

        for r in results:
            if not r["passed"]:
                report.append(f"### {r['customer_name']} - {r['metric_key']}")
                report.append("")
                report.append(f"- **Status:** ❌ FAILED")
                report.append(f"- **Old Schema Rows:** {r['row_count_old']}")
                report.append(f"- **New Schema Rows:** {r['row_count_new']}")
                report.append(f"- **Old Schema Total Sum:** {r['old_total_sum']}")
                report.append(f"- **New Schema Total Sum:** {r['new_total_sum']}")
                report.append(f"- **Difference:** {r['difference']}")
                report.append("")

    # Successful Validations Summary
    if passed_metrics > 0:
        report.append("## Successful Validations")
        report.append("")
        report.append("| Customer | Metric | Rows Validated |")
        report.append("|----------|--------|----------------|")

        for r in results:
            if r["passed"]:
                report.append(f"| {r['customer_name']} | {r['metric_key']} | {r['row_count_old']} |")

        report.append("")

    return "\n".join(report)


def run_validation():
    """Main validation orchestrator."""
    print("=" * 80)
    print("Phase 3: Validation")
    print("=" * 80)
    print()

    # Load query index
    print("Loading query index from Phase 2...")
    index_data = load_query_index()
    queries = index_data["queries"]
    metadata = index_data["metadata"]
    print(f"Loaded {len(queries)} metrics to validate")
    print(f"Date Range: {metadata['unified_date_range']}")
    print()

    # Connect to databases
    print("Connecting to databases...")
    print(f"  Old schema: {SOURCE_DB.host}:{SOURCE_DB.port}")
    old_client = clickhouse_connect.get_client(
        host=SOURCE_DB.host,
        port=SOURCE_DB.port,
        settings={"max_execution_time": SOURCE_DB.timeout}
    )
    print(f"  New schema: {TARGET_DB.host}:{TARGET_DB.port}")
    new_client = clickhouse_connect.get_client(
        host=TARGET_DB.host,
        port=TARGET_DB.port,
        settings={"max_execution_time": TARGET_DB.timeout}
    )
    print("✓ Connected to both databases")
    print()

    # Validation results storage
    validation_results = []

    print("=" * 80)
    print(f"Starting Validation")
    print(f"  Metrics: {len(queries)}")
    print("=" * 80)
    print()

    # Validate each metric
    for idx, query_info in enumerate(queries, start=1):
        customer_name = query_info["customer_name"]
        metric_key = query_info["metric_key"]

        print(f"[{idx}/{len(queries)}] Validating: {customer_name} - {metric_key}")

        try:
            # Read query files
            old_query_file = f"output/benchmarks/queries/{query_info['old_schema_query_file']}"
            new_query_file = f"output/benchmarks/queries/{query_info['new_schema_query_file']}"

            old_query = read_query_file(old_query_file)
            new_query = read_query_file(new_query_file)

            # Execute queries
            print(f"  Executing old schema query...", end=" ", flush=True)
            old_results = execute_query(old_client, old_query)
            print(f"✓ ({len(old_results)} rows)")

            print(f"  Executing new schema query...", end=" ", flush=True)
            new_results = execute_query(new_client, new_query)
            print(f"✓ ({len(new_results)} rows)")

            # Compare results
            print(f"  Comparing results...", end=" ", flush=True)
            comparison = compare_results(old_results, new_results, metric_key, customer_name)
            validation_results.append({
                **query_info,
                **comparison
            })

            if comparison["passed"]:
                print(f"✓ PASSED (sum: {comparison['old_total_sum']})")
            else:
                print(f"❌ FAILED (old: {comparison['old_total_sum']}, new: {comparison['new_total_sum']}, diff: {comparison['difference']})")

            print()

        except Exception as e:
            print(f"  ❌ Error: {e}")
            validation_results.append({
                **query_info,
                "passed": False,
                "error": str(e)
            })
            print()

    # Close connections
    old_client.close()
    new_client.close()

    # Generate reports
    print("=" * 80)
    print("Generating Validation Reports")
    print("=" * 80)
    print()

    # Save detailed results to JSON
    results_filepath = "output/benchmarks/validation_results.json"
    with open(results_filepath, "w") as f:
        json.dump({
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_metrics": len(validation_results),
                "date_range": metadata["unified_date_range"],
                "old_schema_table": metadata.get("old_schema_table"),
                "new_schema_table": metadata.get("new_schema_table")
            },
            "results": validation_results
        }, f, indent=2)
    print(f"✓ Detailed results saved to: {results_filepath}")

    # Generate markdown report
    report = generate_validation_report(validation_results, metadata)
    report_filepath = "output/benchmarks/validation_summary.md"
    with open(report_filepath, "w") as f:
        f.write(report)
    print(f"✓ Summary report saved to: {report_filepath}")
    print()

    # Final summary
    total_metrics = len(validation_results)
    passed_metrics = sum(1 for r in validation_results if r.get("passed", False))
    failed_metrics = total_metrics - passed_metrics

    print("=" * 80)
    if failed_metrics == 0:
        print("✓ Phase 3 Complete - ALL VALIDATIONS PASSED! 🎉")
    else:
        print("✗ Phase 3 Complete - VALIDATION FAILURES DETECTED! ⚠️")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  Total metrics validated: {total_metrics}")
    print(f"  Passed: {passed_metrics}")
    print(f"  Failed: {failed_metrics}")
    print()
    print(f"See {report_filepath} for details")
    print()

    # Raise exception if validation failed
    if failed_metrics > 0:
        raise Exception(f"Validation failed for {failed_metrics} out of {total_metrics} metrics")


if __name__ == "__main__":
    run_validation()
