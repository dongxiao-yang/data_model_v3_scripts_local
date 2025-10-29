"""
Centralized View Benchmark Pipeline Orchestrator

This script provides a single entry point for running the complete view benchmarking pipeline.
Configure which phases to run by setting the flags below.
"""

import sys
import time
from pathlib import Path

from src.config.settings import KEY_MAPPING_FILE

# ============================================================
# PIPELINE CONFIGURATION - Change these to control execution
# ============================================================
RUN_PHASE_1 = True   # View Generation
RUN_PHASE_2 = True   # Query Generation
RUN_PHASE_3 = True   # Benchmark Execution
RUN_PHASE_4 = True   # Analysis & Reporting
# ============================================================


# Expected output files from each phase
PHASE_OUTPUTS = {
    1: ["output/view_benchmarks/view_definitions.json"],
    2: ["output/view_benchmarks/query_index.json", "output/view_benchmarks/queries"],
    3: ["output/view_benchmarks/raw_timing_data.csv"],
    4: ["output/view_benchmarks/view_benchmark_results.md", "output/view_benchmarks/analysis_results.json"]
}


def print_header(text: str, char: str = "="):
    """Print a formatted header."""
    print()
    print(char * 80)
    print(text)
    print(char * 80)
    print()


def print_phase_header(phase_num: int, phase_name: str, will_run: bool):
    """Print phase status header."""
    status = "RUNNING" if will_run else "SKIPPED"
    symbol = "▶" if will_run else "⏭"
    print()
    print(f"{symbol} Phase {phase_num}: {phase_name} - {status}")
    print("-" * 80)


def validate_phase_inputs(phase_num: int) -> bool:
    """
    Validate that required input files exist for a phase.
    Returns True if all inputs exist, False otherwise.
    """
    if phase_num == 1:
        # Phase 1 requires key mapping from transformation pipeline
        if not Path(KEY_MAPPING_FILE).exists():
            print(f"  ❌ ERROR: Required input file missing: {KEY_MAPPING_FILE}")
            print(f"  → You must run the transformation pipeline first to generate key mappings")
            return False
        return True

    # Check outputs from previous phase
    previous_phase = phase_num - 1
    required_files = PHASE_OUTPUTS[previous_phase]

    missing = []
    for file_path in required_files:
        path = Path(file_path)
        if not path.exists():
            missing.append(file_path)

    if missing:
        print(f"  ❌ ERROR: Required input files missing:")
        for file_path in missing:
            print(f"     - {file_path}")
        print(f"  → You must run Phase {previous_phase} first or set RUN_PHASE_{previous_phase} = True")
        return False

    print(f"  ✓ Using existing data from Phase {previous_phase}")
    return True


def run_phase_1():
    """Execute Phase 1: View Generation."""
    from src.view_benchmarking.phase1_view_generation import run_view_generation
    run_view_generation()


def run_phase_2():
    """Execute Phase 2: Query Generation."""
    from src.view_benchmarking.phase2_query_generation import generate_queries
    generate_queries()


def run_phase_3():
    """Execute Phase 3: Benchmark Execution."""
    from src.view_benchmarking.phase3_benchmark_execution import run_benchmark
    run_benchmark()


def run_phase_4():
    """Execute Phase 4: Analysis & Reporting."""
    from src.view_benchmarking.phase4_analysis import run_analysis
    run_analysis()


def run_pipeline():
    """Main pipeline orchestration logic."""

    # Display pipeline configuration
    print_header("View Benchmark Pipeline Orchestrator")

    print("Pipeline Configuration:")
    print(f"  Phase 1 (View Generation):       {'✓ ENABLED' if RUN_PHASE_1 else '✗ DISABLED'}")
    print(f"  Phase 2 (Query Generation):      {'✓ ENABLED' if RUN_PHASE_2 else '✗ DISABLED'}")
    print(f"  Phase 3 (Benchmark Execution):   {'✓ ENABLED' if RUN_PHASE_3 else '✗ DISABLED'}")
    print(f"  Phase 4 (Analysis & Reporting):  {'✓ ENABLED' if RUN_PHASE_4 else '✗ DISABLED'}")

    if not any([RUN_PHASE_1, RUN_PHASE_2, RUN_PHASE_3, RUN_PHASE_4]):
        print()
        print("⚠️  No phases enabled. Nothing to do.")
        return

    print_header("Starting Pipeline Execution", "=")

    start_time = time.time()
    phases_executed = []
    phases_skipped = []

    # Phase 1: View Generation
    print_phase_header(1, "View Generation", RUN_PHASE_1)
    if RUN_PHASE_1:
        if not validate_phase_inputs(1):
            sys.exit(1)
        try:
            phase_start = time.time()
            run_phase_1()
            phase_duration = time.time() - phase_start
            phases_executed.append(("Phase 1: View Generation", phase_duration))
        except Exception as e:
            print()
            print(f"❌ Phase 1 failed with error: {e}")
            sys.exit(1)
    else:
        if not validate_phase_inputs(1):
            sys.exit(1)
        phases_skipped.append("Phase 1: View Generation")

    # Phase 2: Query Generation
    print_phase_header(2, "Query Generation", RUN_PHASE_2)
    if RUN_PHASE_2:
        if not validate_phase_inputs(2):
            sys.exit(1)
        try:
            phase_start = time.time()
            run_phase_2()
            phase_duration = time.time() - phase_start
            phases_executed.append(("Phase 2: Query Generation", phase_duration))
        except Exception as e:
            print()
            print(f"❌ Phase 2 failed with error: {e}")
            sys.exit(1)
    else:
        if not validate_phase_inputs(2):
            sys.exit(1)
        phases_skipped.append("Phase 2: Query Generation")

    # Phase 3: Benchmark Execution
    print_phase_header(3, "Benchmark Execution", RUN_PHASE_3)
    if RUN_PHASE_3:
        if not validate_phase_inputs(3):
            sys.exit(1)
        try:
            phase_start = time.time()
            run_phase_3()
            phase_duration = time.time() - phase_start
            phases_executed.append(("Phase 3: Benchmark Execution", phase_duration))
        except Exception as e:
            print()
            print(f"❌ Phase 3 failed with error: {e}")
            sys.exit(1)
    else:
        if not validate_phase_inputs(3):
            sys.exit(1)
        phases_skipped.append("Phase 3: Benchmark Execution")

    # Phase 4: Analysis & Reporting
    print_phase_header(4, "Analysis & Reporting", RUN_PHASE_4)
    if RUN_PHASE_4:
        if not validate_phase_inputs(4):
            sys.exit(1)
        try:
            phase_start = time.time()
            run_phase_4()
            phase_duration = time.time() - phase_start
            phases_executed.append(("Phase 4: Analysis & Reporting", phase_duration))
        except Exception as e:
            print()
            print(f"❌ Phase 4 failed with error: {e}")
            sys.exit(1)
    else:
        if not validate_phase_inputs(4):
            sys.exit(1)
        phases_skipped.append("Phase 4: Analysis & Reporting")

    # Pipeline complete
    total_duration = time.time() - start_time

    print_header("Pipeline Complete! 🎉", "=")

    print("Execution Summary:")
    print(f"  Total time: {total_duration:.1f}s")
    print()

    if phases_executed:
        print("Phases Executed:")
        for phase_name, duration in phases_executed:
            print(f"  ✓ {phase_name} ({duration:.1f}s)")
        print()

    if phases_skipped:
        print("Phases Skipped:")
        for phase_name in phases_skipped:
            print(f"  ⏭ {phase_name}")
        print()

    # Show key outputs
    print("Key Outputs:")
    if RUN_PHASE_1 or (not RUN_PHASE_1 and Path("output/view_benchmarks/view_definitions.json").exists()):
        print("  📄 output/view_benchmarks/view_definitions.json")
    if RUN_PHASE_2 or (not RUN_PHASE_2 and Path("output/view_benchmarks/query_index.json").exists()):
        print("  📄 output/view_benchmarks/query_index.json")
        print("  📁 output/view_benchmarks/queries/")
    if RUN_PHASE_3 or (not RUN_PHASE_3 and Path("output/view_benchmarks/raw_timing_data.csv").exists()):
        print("  📄 output/view_benchmarks/raw_timing_data.csv")
    if RUN_PHASE_4 or (not RUN_PHASE_4 and Path("output/view_benchmarks/view_benchmark_results.md").exists()):
        print("  📄 output/view_benchmarks/view_benchmark_results.md")
        print("  📄 output/view_benchmarks/analysis_results.json")

    # Show quick results if Phase 4 ran
    if RUN_PHASE_4:
        print()
        print("Quick Results Preview:")
        try:
            import json
            with open("output/view_benchmarks/analysis_results.json", "r") as f:
                results = json.load(f)
                overall = results["overall_stats"]
                print(f"  📊 Direct Table Avg: {overall['direct_table']['mean']:.3f}s")
                print(f"  📊 Through View Avg: {overall['through_view']['mean']:.3f}s")
                overhead_pct = overall['average_overhead_percentage']
                if overhead_pct >= 0:
                    print(f"  📈 View Overhead: +{overhead_pct:.2f}%")
                else:
                    print(f"  📈 View Improvement: {abs(overhead_pct):.2f}% faster")
        except Exception:
            pass

    print()


if __name__ == "__main__":
    try:
        run_pipeline()
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
