"""
Parse transformation logs to generate statistics.

This script processes transformation.log files to extract timing and performance
statistics for Phase 3: DATA TRANSFORMATION.
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict


@dataclass
class CustomerStats:
    """Statistics for a single customer."""
    customer_id: int
    total_raw_rows: int
    total_agg_rows: int
    total_chunks: int
    avg_db_agg_seconds: float
    avg_python_transform_seconds: float
    avg_db_insert_seconds: float
    avg_total_seconds: float
    avg_compression_ratio: float
    total_time_seconds: float


class LogParser:
    """Streaming log parser for transformation logs."""

    # Regex patterns for log parsing
    TIMESTAMP_PATTERN = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})'
    CUSTOMER_PATTERN = r'Processing customer (\d+)'
    CHUNK_START_PATTERN = r'Processing chunk (\d+)/\d+: (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) to (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
    COMPRESSION_PATTERN = r'Compression for chunk: (\d+) raw → (\d+) aggregated rows \((\d+\.\d+)x\)'
    FETCHED_PATTERN = r'Fetched (\d+) aggregated rows'
    TRANSFORMED_PATTERN = r'Transformed (\d+) pre-aggregated rows'
    INSERTING_PATTERN = r'Inserting (\d+) rows into target table'
    INSERTED_PATTERN = r'Successfully inserted (\d+) rows'
    CHUNK_COMPLETE_PATTERN = r'Chunk (\d+) completed'

    def __init__(self, log_file: str):
        self.log_file = log_file
        self.phase3_start_line = None

        # State tracking
        self.current_customer_id: Optional[int] = None
        self.current_chunk_num: Optional[int] = None
        self.chunk_start_time: Optional[datetime] = None
        self.fetched_time: Optional[datetime] = None
        self.transformed_time: Optional[datetime] = None
        self.inserting_time: Optional[datetime] = None
        self.inserted_time: Optional[datetime] = None
        self.chunk_start_period: Optional[str] = None
        self.chunk_raw_row_count: Optional[int] = None
        self.chunk_agg_row_count: Optional[int] = None
        self.chunk_compression_ratio: Optional[float] = None

        # Statistics storage
        self.customer_stats: Dict[int, Dict] = defaultdict(lambda: {
            'total_raw_rows': 0,
            'total_agg_rows': 0,
            'total_chunks': 0,
            'db_agg_times': [],
            'python_transform_times': [],
            'db_insert_times': [],
            'total_times': [],
            'compression_ratios': [],
            'first_chunk_time': None,
            'last_chunk_time': None
        })

    def parse_timestamp(self, line: str) -> Optional[datetime]:
        """Extract timestamp from log line."""
        match = re.match(self.TIMESTAMP_PATTERN, line)
        if match:
            timestamp_str = match.group(1)
            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
        return None

    def parse_line(self, line: str, line_num: int):
        """Parse a single log line and update state."""
        timestamp = self.parse_timestamp(line)
        if not timestamp:
            return

        # Check for customer marker
        customer_match = re.search(self.CUSTOMER_PATTERN, line)
        if customer_match:
            self.current_customer_id = int(customer_match.group(1))
            print(f"  Found customer: {self.current_customer_id}")
            return

        # Check for chunk start
        chunk_start_match = re.search(self.CHUNK_START_PATTERN, line)
        if chunk_start_match:
            self.current_chunk_num = int(chunk_start_match.group(1))
            start_time = chunk_start_match.group(2)
            end_time = chunk_start_match.group(3)
            self.chunk_start_period = f"{start_time[11:16]}-{end_time[11:16]}"  # Extract HH:MM
            self.chunk_start_time = timestamp

            # Record first chunk time for customer
            if self.customer_stats[self.current_customer_id]['first_chunk_time'] is None:
                self.customer_stats[self.current_customer_id]['first_chunk_time'] = timestamp
            return

        # Check for compression (appears before fetched)
        compression_match = re.search(self.COMPRESSION_PATTERN, line)
        if compression_match:
            self.chunk_raw_row_count = int(compression_match.group(1))
            self.chunk_agg_row_count = int(compression_match.group(2))
            self.chunk_compression_ratio = float(compression_match.group(3))
            return

        # Check for fetched (DB aggregation complete)
        fetched_match = re.search(self.FETCHED_PATTERN, line)
        if fetched_match:
            self.fetched_time = timestamp
            return

        # Check for transformed (Python transformation complete)
        transformed_match = re.search(self.TRANSFORMED_PATTERN, line)
        if transformed_match:
            self.transformed_time = timestamp
            return

        # Check for inserting (DB insert start)
        inserting_match = re.search(self.INSERTING_PATTERN, line)
        if inserting_match:
            self.inserting_time = timestamp
            return

        # Check for inserted (DB insert complete)
        inserted_match = re.search(self.INSERTED_PATTERN, line)
        if inserted_match:
            self.inserted_time = timestamp
            return

        # Check for chunk complete
        chunk_complete_match = re.search(self.CHUNK_COMPLETE_PATTERN, line)
        if chunk_complete_match:
            self._finalize_chunk(timestamp)
            return

    def _finalize_chunk(self, complete_time: datetime):
        """Finalize current chunk and record statistics."""
        if not all([self.current_customer_id, self.current_chunk_num,
                   self.chunk_start_time, self.fetched_time,
                   self.transformed_time, self.inserting_time, self.inserted_time]):
            print(f"  Warning: Incomplete chunk data for chunk {self.current_chunk_num}")
            return

        # Calculate timings
        db_agg_seconds = (self.fetched_time - self.chunk_start_time).total_seconds()
        python_transform_seconds = (self.transformed_time - self.fetched_time).total_seconds()
        db_insert_seconds = (self.inserted_time - self.inserting_time).total_seconds()
        total_seconds = (complete_time - self.chunk_start_time).total_seconds()

        # Store in customer stats
        stats = self.customer_stats[self.current_customer_id]
        stats['total_raw_rows'] += self.chunk_raw_row_count or 0
        stats['total_agg_rows'] += self.chunk_agg_row_count or 0
        stats['total_chunks'] += 1
        stats['db_agg_times'].append(db_agg_seconds)
        stats['python_transform_times'].append(python_transform_seconds)
        stats['db_insert_times'].append(db_insert_seconds)
        stats['total_times'].append(total_seconds)
        if self.chunk_compression_ratio is not None:
            stats['compression_ratios'].append(self.chunk_compression_ratio)
        stats['last_chunk_time'] = complete_time

        # Reset chunk state
        self.current_chunk_num = None
        self.chunk_start_time = None
        self.fetched_time = None
        self.transformed_time = None
        self.inserting_time = None
        self.inserted_time = None
        self.chunk_start_period = None
        self.chunk_raw_row_count = None
        self.chunk_agg_row_count = None
        self.chunk_compression_ratio = None

    def parse_file(self):
        """Parse the entire log file starting from Phase 3."""
        print(f"Parsing log file: {self.log_file}")

        # Find Phase 3 start marker
        print("Searching for Phase 3 start marker...")
        with open(self.log_file, 'r') as f:
            for line_num, line in enumerate(f, start=1):
                if "PHASE 3: DATA TRANSFORMATION" in line:
                    self.phase3_start_line = line_num
                    print(f"Found Phase 3 start at line {self.phase3_start_line}")
                    break

        if self.phase3_start_line is None:
            raise RuntimeError("Could not find 'PHASE 3: DATA TRANSFORMATION' marker in log file. Cannot proceed.")

        with open(self.log_file, 'r') as f:
            for line_num, line in enumerate(f, start=1):
                # Skip until we reach Phase 3
                if line_num < self.phase3_start_line:
                    continue

                self.parse_line(line, line_num)

                # Progress update every 5000 lines
                if line_num % 5000 == 0:
                    print(f"  Processed {line_num} lines...")

        print(f"Parsing complete. Processed {line_num} total lines.")

    def compute_customer_stats(self) -> List[CustomerStats]:
        """Compute final statistics for each customer."""
        results = []

        for customer_id, stats in self.customer_stats.items():
            if stats['total_chunks'] == 0:
                continue

            # Calculate total time for this customer
            if stats['first_chunk_time'] and stats['last_chunk_time']:
                total_time_seconds = (stats['last_chunk_time'] - stats['first_chunk_time']).total_seconds()
            else:
                total_time_seconds = sum(stats['total_times'])

            # Calculate average compression ratio
            avg_compression = 0.0
            if stats['compression_ratios']:
                avg_compression = sum(stats['compression_ratios']) / len(stats['compression_ratios'])

            customer_stat = CustomerStats(
                customer_id=customer_id,
                total_raw_rows=stats['total_raw_rows'],
                total_agg_rows=stats['total_agg_rows'],
                total_chunks=stats['total_chunks'],
                avg_db_agg_seconds=sum(stats['db_agg_times']) / len(stats['db_agg_times']),
                avg_python_transform_seconds=sum(stats['python_transform_times']) / len(stats['python_transform_times']),
                avg_db_insert_seconds=sum(stats['db_insert_times']) / len(stats['db_insert_times']),
                avg_total_seconds=sum(stats['total_times']) / len(stats['total_times']),
                avg_compression_ratio=avg_compression,
                total_time_seconds=total_time_seconds
            )
            results.append(customer_stat)

        # Sort by customer ID
        results.sort(key=lambda x: x.customer_id)
        return results


def format_seconds(seconds: float) -> str:
    """Format seconds as human-readable string (e.g., '1m 23s')."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    return f"{minutes}m {remaining_seconds:.0f}s"


def generate_markdown_report(customer_stats: List[CustomerStats], output_file: str):
    """Generate markdown report with statistics."""
    with open(output_file, 'w') as f:
        f.write("# Transformation Statistics Report\n\n")

        # Per-customer summary table
        f.write("## Per-Customer Summary\n\n")
        f.write("| Customer ID | Raw Rows | Agg Rows | Total Chunks | Avg Compression | Avg DB Agg | Avg Python Transform | Avg DB Insert | Avg Total Time | Total Time |\n")
        f.write("|-------------|----------|----------|--------------|-----------------|------------|----------------------|---------------|----------------|------------|\n")

        for stats in customer_stats:
            f.write(f"| {stats.customer_id} | {stats.total_raw_rows:,} | {stats.total_agg_rows:,} | {stats.total_chunks} | "
                   f"{stats.avg_compression_ratio:.2f}x | "
                   f"{stats.avg_db_agg_seconds:.2f}s | {stats.avg_python_transform_seconds:.2f}s | "
                   f"{stats.avg_db_insert_seconds:.2f}s | "
                   f"{stats.avg_total_seconds:.2f}s | "
                   f"{format_seconds(stats.total_time_seconds)} |\n")

    print(f"Markdown report written to: {output_file}")


def generate_json_report(customer_stats: List[CustomerStats], output_file: str):
    """Generate JSON report with statistics."""
    data = {
        'customers': [asdict(stats) for stats in customer_stats]
    }

    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2, default=str)

    print(f"JSON report written to: {output_file}")


def main():
    """Main entry point."""
    LOG_FILE = "output/reports/transformation.log"
    MARKDOWN_OUTPUT = "output/reports/transformation_stats.md"
    JSON_OUTPUT = "output/reports/transformation_stats.json"

    # Parse log file (phase3_start_line will be detected automatically)
    parser = LogParser(LOG_FILE)
    parser.parse_file()

    # Compute statistics
    print("\nComputing statistics...")
    customer_stats = parser.compute_customer_stats()
    print(f"Found {len(customer_stats)} customers")

    # Generate reports
    print("\nGenerating reports...")
    generate_markdown_report(customer_stats, MARKDOWN_OUTPUT)
    generate_json_report(customer_stats, JSON_OUTPUT)

    print("\nDone!")


if __name__ == '__main__':
    main()