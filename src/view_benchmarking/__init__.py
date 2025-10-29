"""
View Benchmarking Package

This package implements a pipeline to benchmark query performance when accessing
the new schema through per-customer views vs direct table access.

Phases:
1. View Generation: Create per-customer views with readable metric names
2. Query Generation: Generate SQL queries for both access patterns
3. Benchmark Execution: Execute queries and measure performance
4. Analysis & Reporting: Analyze results and generate reports
5. Cleanup: Drop created views (optional)
"""

__all__ = [
    'phase1_view_generation',
    'phase2_query_generation',
    'phase3_benchmark_execution',
    'phase4_analysis',
    'phase5_cleanup'
]
