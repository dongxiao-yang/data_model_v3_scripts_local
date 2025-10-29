"""
Phase 4: Analysis & Reporting

This script:
1. Loads raw timing data from CSV
2. Calculates summary statistics for each metric
3. Analyzes performance by customer and metric type
4. Generates markdown performance report
5. Saves analysis results to JSON
"""

import csv
import json
import statistics
from collections import defaultdict
from typing import Dict, List, Tuple
from datetime import datetime



def load_timing_data() -> List[Dict]:
    """Load raw timing data from CSV."""
    with open("output/benchmarks/raw_timing_data.csv", "r") as f:
        reader = csv.DictReader(f)
        data = []
        for row in reader:
            row["customer_id"] = int(row["customer_id"])
            row["run_number"] = int(row["run_number"])
            row["execution_time_seconds"] = float(row["execution_time_seconds"])
            row["row_count"] = int(row["row_count"])
            data.append(row)
    return data


def calculate_metric_statistics(data: List[Dict]) -> Dict:
    """Calculate statistics for each metric (old vs new)."""
    # Group by customer_id, metric_key, schema_type
    grouped = defaultdict(list)

    for row in data:
        key = (row["customer_id"], row["customer_name"], row["metric_key"], row["schema_type"])
        grouped[key].append(row["execution_time_seconds"])

    stats = []
    for key, times in grouped.items():
        customer_id, customer_name, metric_key, schema_type = key
        stats.append({
            "customer_id": customer_id,
            "customer_name": customer_name,
            "metric_key": metric_key,
            "schema_type": schema_type,
            "min": min(times),
            "max": max(times),
            "avg": statistics.mean(times),
            "median": statistics.median(times),
            "stdev": statistics.stdev(times) if len(times) > 1 else 0,
            "runs": len(times)
        })

    return stats


def calculate_speedups(stats: List[Dict]) -> List[Dict]:
    """Calculate speedup for each metric (old vs new)."""
    # Group by customer and metric
    grouped = defaultdict(dict)

    for stat in stats:
        key = (stat["customer_id"], stat["customer_name"], stat["metric_key"])
        grouped[key][stat["schema_type"]] = stat

    speedups = []
    for key, schemas in grouped.items():
        customer_id, customer_name, metric_key = key

        old_avg = schemas["old"]["avg"]
        new_avg = schemas["new"]["avg"]
        speedup = old_avg / new_avg if new_avg > 0 else 0

        speedups.append({
            "customer_id": customer_id,
            "customer_name": customer_name,
            "metric_key": metric_key,
            "old_avg": old_avg,
            "new_avg": new_avg,
            "speedup": speedup,
            "improvement_percent": ((old_avg - new_avg) / old_avg * 100) if old_avg > 0 else 0
        })

    return speedups


def analyze_by_customer(speedups: List[Dict]) -> Dict:
    """Analyze performance by customer."""
    by_customer = defaultdict(list)

    for item in speedups:
        by_customer[item["customer_name"]].append(item)

    customer_analysis = {}
    for customer_name, items in by_customer.items():
        speedup_values = [item["speedup"] for item in items]
        customer_analysis[customer_name] = {
            "metrics_count": len(items),
            "avg_speedup": statistics.mean(speedup_values),
            "min_speedup": min(speedup_values),
            "max_speedup": max(speedup_values),
            "metrics": items
        }

    return customer_analysis


def analyze_by_metric_type(speedups: List[Dict]) -> Dict:
    """Analyze performance by metric type (success, duration, other)."""
    by_type = defaultdict(list)

    for item in speedups:
        metric_key = item["metric_key"]
        if "success" in metric_key:
            metric_type = "success_count"
        elif "total_duration" in metric_key or "time" in metric_key:
            metric_type = "duration"
        else:
            metric_type = "other"

        by_type[metric_type].append(item)

    type_analysis = {}
    for metric_type, items in by_type.items():
        speedup_values = [item["speedup"] for item in items]
        type_analysis[metric_type] = {
            "metrics_count": len(items),
            "avg_speedup": statistics.mean(speedup_values),
            "min_speedup": min(speedup_values),
            "max_speedup": max(speedup_values)
        }

    return type_analysis


def calculate_overall_stats(data: List[Dict]) -> Dict:
    """Calculate overall statistics across all queries."""
    old_times = [row["execution_time_seconds"] for row in data if row["schema_type"] == "old"]
    new_times = [row["execution_time_seconds"] for row in data if row["schema_type"] == "new"]

    old_avg = statistics.mean(old_times)
    new_avg = statistics.mean(new_times)

    return {
        "old_schema": {
            "total_runs": len(old_times),
            "avg": old_avg,
            "min": min(old_times),
            "max": max(old_times),
            "median": statistics.median(old_times),
            "stdev": statistics.stdev(old_times)
        },
        "new_schema": {
            "total_runs": len(new_times),
            "avg": new_avg,
            "min": min(new_times),
            "max": max(new_times),
            "median": statistics.median(new_times),
            "stdev": statistics.stdev(new_times)
        },
        "overall_speedup": old_avg / new_avg if new_avg > 0 else 0,
        "improvement_percent": ((old_avg - new_avg) / old_avg * 100) if old_avg > 0 else 0
    }


def generate_markdown_report(overall_stats: Dict, speedups: List[Dict],
                             customer_analysis: Dict, type_analysis: Dict,
                             context: Dict) -> str:
    """Generate markdown performance report."""

    report = ["# Aggregation Performance Benchmark Results", ""]
    report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    report.append(f"**Date Range:** {context.get('date_range', 'N/A')}")
    report.append(f"**Old Schema Table:** {context.get('old_schema_table', 'N/A')}")
    report.append(f"**New Schema Table:** {context.get('new_schema_table', 'N/A')}")
    report.append("")

    # Overall Statistics
    report.append("## Overall Performance Statistics")
    report.append("")
    report.append("### Old Schema (Map Columns)")
    report.append("")
    report.append(f"- **Average:** {overall_stats['old_schema']['avg']:.3f}s")
    report.append(f"- **Median:** {overall_stats['old_schema']['median']:.3f}s")
    report.append(f"- **Min:** {overall_stats['old_schema']['min']:.3f}s")
    report.append(f"- **Max:** {overall_stats['old_schema']['max']:.3f}s")
    report.append(f"- **Std Dev:** {overall_stats['old_schema']['stdev']:.3f}s")
    report.append("")

    report.append("### New Schema (Primitive Columns)")
    report.append("")
    report.append(f"- **Average:** {overall_stats['new_schema']['avg']:.3f}s")
    report.append(f"- **Median:** {overall_stats['new_schema']['median']:.3f}s")
    report.append(f"- **Min:** {overall_stats['new_schema']['min']:.3f}s")
    report.append(f"- **Max:** {overall_stats['new_schema']['max']:.3f}s")
    report.append(f"- **Std Dev:** {overall_stats['new_schema']['stdev']:.3f}s")
    report.append("")

    # Per-Customer Analysis
    report.append("## Per-Customer Analysis")
    report.append("")

    for customer_name in sorted(customer_analysis.keys()):
        analysis = customer_analysis[customer_name]
        report.append(f"### {customer_name}")
        report.append("")
        report.append(f"- **Metrics Tested:** {analysis['metrics_count']}")
        report.append(f"- **Average Speedup:** {analysis['avg_speedup']:.2f}x")
        report.append(f"- **Min Speedup:** {analysis['min_speedup']:.2f}x")
        report.append(f"- **Max Speedup:** {analysis['max_speedup']:.2f}x")
        report.append("")

        # Table of metrics
        report.append("| Metric | Old Avg (s) | New Avg (s) | Speedup |")
        report.append("|--------|-------------|-------------|---------|")
        for metric in sorted(analysis['metrics'], key=lambda x: x['speedup'], reverse=True):
            report.append(f"| {metric['metric_key']} | {metric['old_avg']:.3f} | {metric['new_avg']:.3f} | {metric['speedup']:.2f}x |")
        report.append("")

    # Per-Metric Type Analysis
    report.append("## Per-Metric Type Analysis")
    report.append("")

    for metric_type, analysis in sorted(type_analysis.items()):
        type_name = metric_type.replace("_", " ").title()
        report.append(f"### {type_name}")
        report.append("")
        report.append(f"- **Metrics Count:** {analysis['metrics_count']}")
        report.append(f"- **Average Speedup:** {analysis['avg_speedup']:.2f}x")
        report.append(f"- **Min Speedup:** {analysis['min_speedup']:.2f}x")
        report.append(f"- **Max Speedup:** {analysis['max_speedup']:.2f}x")
        report.append("")

    # Detailed Results Table
    report.append("## Detailed Results")
    report.append("")
    report.append("| Customer | Metric | Old Avg (s) | New Avg (s) | Speedup | Improvement |")
    report.append("|----------|--------|-------------|-------------|---------|-------------|")

    for item in sorted(speedups, key=lambda x: x["speedup"], reverse=True):
        report.append(f"| {item['customer_name']} | {item['metric_key']} | "
                     f"{item['old_avg']:.3f} | {item['new_avg']:.3f} | "
                     f"{item['speedup']:.2f}x | {item['improvement_percent']:.1f}% |")

    report.append("")

    return "\n".join(report)


def run_analysis():
    """Main analysis process."""
    print("=" * 80)
    print("Phase 4: Analysis & Reporting")
    print("=" * 80)
    print()

    # Load data
    print("Loading raw timing data...")
    data = load_timing_data()
    print(f"Loaded {len(data)} measurement data points")
    print()

    # Calculate statistics
    print("Calculating summary statistics...")
    stats = calculate_metric_statistics(data)
    print(f"Calculated statistics for {len(stats)} metric-schema combinations")
    print()

    # Calculate speedups
    print("Calculating speedups...")
    speedups = calculate_speedups(stats)
    print(f"Calculated speedups for {len(speedups)} metrics")
    print()

    # Overall statistics
    print("Calculating overall statistics...")
    overall_stats = calculate_overall_stats(data)
    print(f"Overall speedup: {overall_stats['overall_speedup']:.2f}x")
    print()

    # Customer analysis
    print("Analyzing by customer...")
    customer_analysis = analyze_by_customer(speedups)
    print(f"Analyzed {len(customer_analysis)} customers")
    print()

    # Metric type analysis
    print("Analyzing by metric type...")
    type_analysis = analyze_by_metric_type(speedups)
    print(f"Analyzed {len(type_analysis)} metric types")
    print()

    # Generate report
    print("Generating markdown report...")
    # Load context (tables/date range) from query index metadata
    with open("output/benchmarks/query_index.json", "r") as _f:
        _index_meta = json.load(_f).get("metadata", {})
    context = {
        "date_range": _index_meta.get("unified_date_range", "N/A"),
        "old_schema_table": _index_meta.get("old_schema_table", "N/A"),
        "new_schema_table": _index_meta.get("new_schema_table", "N/A"),
    }

    report = generate_markdown_report(overall_stats, speedups, customer_analysis, type_analysis, context)
    report_path = "output/benchmarks/aggregation_benchmark_results.md"
    with open(report_path, "w") as f:
        f.write(report)
    print(f"✓ Report saved to: {report_path}")
    print()

    # Save analysis results
    print("Saving analysis results to JSON...")
    results = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "total_measurements": len(data),
            "metrics_tested": len(speedups),
            "date_range": context.get("date_range"),
            "old_schema_table": context.get("old_schema_table"),
            "new_schema_table": context.get("new_schema_table")
        },
        "overall_stats": overall_stats,
        "speedups": speedups,
        "customer_analysis": customer_analysis,
        "metric_type_analysis": type_analysis
    }

    results_path = "output/benchmarks/analysis_results.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"✓ Analysis results saved to: {results_path}")
    print()

    print("=" * 80)
    print("✓ Phase 4 Complete!")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  Overall speedup: {overall_stats['overall_speedup']:.2f}x")
    print(f"  Old schema avg: {overall_stats['old_schema']['avg']:.3f}s")
    print(f"  New schema avg: {overall_stats['new_schema']['avg']:.3f}s")
    print(f"  Improvement: {overall_stats['improvement_percent']:.1f}%")
    print()


if __name__ == "__main__":
    run_analysis()
