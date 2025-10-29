"""
Phase 4: Analysis & Reporting for View Benchmarking

This script:
1. Loads raw timing data from Phase 3
2. Calculates statistics per metric and access type
3. Calculates view overhead metrics
4. Generates human-readable report (view_benchmark_results.md)
5. Generates machine-readable results (analysis_results.json)
"""

import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List
import statistics

from src.config.settings import CUSTOMER_NAMES


def load_timing_data() -> List[Dict]:
    """Load raw timing data from Phase 3."""
    results = []
    with open("output/view_benchmarks/raw_timing_data.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append({
                "customer_id": int(row["customer_id"]),
                "customer_name": row["customer_name"],
                "metric_key": row["metric_key"],
                "access_type": row["access_type"],
                "run_number": int(row["run_number"]),
                "execution_time_seconds": float(row["execution_time_seconds"]),
                "row_count": int(row["row_count"]),
                "query_date": row["query_date"]
            })
    return results


def calculate_statistics(times: List[float]) -> Dict:
    """Calculate statistics for a list of execution times."""
    if not times:
        return {}

    sorted_times = sorted(times)
    return {
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times),
        "std_dev": statistics.stdev(times) if len(times) > 1 else 0.0,
        "p95": sorted_times[int(len(sorted_times) * 0.95)],
        "p99": sorted_times[int(len(sorted_times) * 0.99)]
    }


def analyze_data(data: List[Dict]) -> Dict:
    """Analyze timing data and calculate metrics."""
    # Group by customer + metric + access_type
    grouped = defaultdict(lambda: defaultdict(list))

    for row in data:
        key = (row["customer_id"], row["customer_name"], row["metric_key"])
        grouped[key][row["access_type"]].append(row["execution_time_seconds"])

    # Calculate statistics for each metric
    metric_results = []

    for (customer_id, customer_name, metric_key), access_types in grouped.items():
        direct_times = access_types.get("direct_table", [])
        view_times = access_types.get("through_view", [])

        direct_stats = calculate_statistics(direct_times)
        view_stats = calculate_statistics(view_times)

        # Calculate overhead
        direct_avg = direct_stats["mean"]
        view_avg = view_stats["mean"]
        overhead_absolute = view_avg - direct_avg
        overhead_percentage = (overhead_absolute / direct_avg * 100) if direct_avg > 0 else 0

        metric_results.append({
            "customer_id": customer_id,
            "customer_name": customer_name,
            "metric_key": metric_key,
            "direct_table": direct_stats,
            "through_view": view_stats,
            "overhead_absolute_seconds": overhead_absolute,
            "overhead_percentage": overhead_percentage
        })

    # Calculate overall statistics
    all_direct = [r["direct_table"]["mean"] for r in metric_results]
    all_view = [r["through_view"]["mean"] for r in metric_results]

    overall_stats = {
        "direct_table": calculate_statistics(all_direct),
        "through_view": calculate_statistics(all_view),
        "average_overhead_percentage": statistics.mean([r["overhead_percentage"] for r in metric_results])
    }

    # Calculate per-customer statistics
    customer_stats = defaultdict(lambda: {"direct": [], "view": [], "overhead": []})
    for r in metric_results:
        customer_stats[r["customer_name"]]["direct"].append(r["direct_table"]["mean"])
        customer_stats[r["customer_name"]]["view"].append(r["through_view"]["mean"])
        customer_stats[r["customer_name"]]["overhead"].append(r["overhead_percentage"])

    return {
        "metric_results": sorted(metric_results, key=lambda x: x["overhead_percentage"], reverse=True),
        "overall_stats": overall_stats,
        "customer_stats": customer_stats
    }


def generate_markdown_report(analysis: Dict):
    """Generate human-readable markdown report."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    overall = analysis["overall_stats"]
    metrics = analysis["metric_results"]

    report = f"""# View-Based Access Performance Benchmark Results

**Generated:** {timestamp}

## Overall Performance Statistics

### Direct Table Access

- **Average:** {overall["direct_table"]["mean"]:.3f}s
- **Median:** {overall["direct_table"]["median"]:.3f}s
- **Min:** {overall["direct_table"]["min"]:.3f}s
- **Max:** {overall["direct_table"]["max"]:.3f}s
- **Std Dev:** {overall["direct_table"]["std_dev"]:.3f}s
- **P95:** {overall["direct_table"]["p95"]:.3f}s
- **P99:** {overall["direct_table"]["p99"]:.3f}s

### Through View Access

- **Average:** {overall["through_view"]["mean"]:.3f}s
- **Median:** {overall["through_view"]["median"]:.3f}s
- **Min:** {overall["through_view"]["min"]:.3f}s
- **Max:** {overall["through_view"]["max"]:.3f}s
- **Std Dev:** {overall["through_view"]["std_dev"]:.3f}s
- **P95:** {overall["through_view"]["p95"]:.3f}s
- **P99:** {overall["through_view"]["p99"]:.3f}s

## Per-Customer Analysis

"""

    # Per-customer sections
    for customer_name in sorted(analysis["customer_stats"].keys()):
        cust_data = analysis["customer_stats"][customer_name]
        cust_metrics = [m for m in metrics if m["customer_name"] == customer_name]

        avg_direct = statistics.mean(cust_data["direct"])
        avg_view = statistics.mean(cust_data["view"])
        avg_overhead = statistics.mean(cust_data["overhead"])

        report += f"""### {customer_name}

- **Metrics Tested:** {len(cust_metrics)}
- **Average Direct:** {avg_direct:.3f}s
- **Average View:** {avg_view:.3f}s
- **Average Overhead:** {avg_overhead:.2f}%

| Metric | Direct Avg (s) | View Avg (s) | Overhead |
|--------|----------------|--------------|----------|
"""

        for m in sorted(cust_metrics, key=lambda x: x["overhead_percentage"], reverse=True):
            report += f"| {m['metric_key']} | {m['direct_table']['mean']:.3f} | {m['through_view']['mean']:.3f} | {m['overhead_percentage']:+.2f}% |\n"

        report += "\n"

    # Detailed results table
    report += """## Detailed Results

| Customer | Metric | Direct Avg (s) | View Avg (s) | Overhead |
|----------|--------|----------------|--------------|----------|
"""

    for m in metrics:
        report += f"| {m['customer_name']} | {m['metric_key']} | {m['direct_table']['mean']:.3f} | {m['through_view']['mean']:.3f} | {m['overhead_percentage']:+.2f}% |\n"

    # Save report
    output_path = "output/view_benchmarks/view_benchmark_results.md"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        f.write(report)

    print(f"✓ Markdown report saved to: {output_path}")


def generate_json_results(analysis: Dict):
    """Generate machine-readable JSON results."""
    output_path = "output/view_benchmarks/analysis_results.json"

    with open(output_path, "w") as f:
        json.dump(analysis, f, indent=2)

    print(f"✓ JSON results saved to: {output_path}")


def run_analysis():
    """Main analysis process."""
    print("=" * 80)
    print("Phase 4: Analysis & Reporting for View Benchmarking")
    print("=" * 80)
    print()

    # Load timing data
    print("Loading timing data from Phase 3...")
    data = load_timing_data()
    print(f"Loaded {len(data)} measurement runs")
    print()

    # Analyze data
    print("Analyzing performance data...")
    analysis = analyze_data(data)
    print(f"Analyzed {len(analysis['metric_results'])} metrics")
    print()

    # Generate reports
    print("Generating reports...")
    generate_markdown_report(analysis)
    generate_json_results(analysis)
    print()

    # Print summary
    overall = analysis["overall_stats"]
    print("=" * 80)
    print("✓ Phase 4 Complete!")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  Direct table average: {overall['direct_table']['mean']:.3f}s")
    print(f"  Through view average: {overall['through_view']['mean']:.3f}s")
    print(f"  Average overhead: {overall['average_overhead_percentage']:+.2f}%")
    print()
    print("Output files:")
    print("  - output/view_benchmarks/view_benchmark_results.md")
    print("  - output/view_benchmarks/analysis_results.json")
    print()


if __name__ == "__main__":
    run_analysis()
