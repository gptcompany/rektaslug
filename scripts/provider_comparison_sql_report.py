#!/usr/bin/env python3
"""Query provider_comparison_* tables and print a compact historical report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.validation.constants import VALIDATION_DB_PATH

DEFAULT_DB_PATH = Path(VALIDATION_DB_PATH)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="DuckDB path. Defaults to the validation DuckDB.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="How many recent runs / rows to show in each section.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of human-readable text.",
    )
    return parser.parse_args()


def fetch_rows(conn, query: str, params: list[object]) -> list[dict[str, object]]:
    """Execute a query and return dictionaries keyed by column name."""
    cursor = conn.execute(query, params)
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def build_report(conn, db_path: Path, limit: int) -> dict[str, object]:
    """Build the SQL-backed summary report."""
    recent_runs = fetch_rows(
        conn,
        """
        SELECT
            run_id,
            created_at,
            report_path,
            (
                SELECT COUNT(*)
                FROM provider_comparison_datasets datasets
                WHERE datasets.run_id = runs.run_id
            ) AS dataset_count,
            (
                SELECT COUNT(*)
                FROM provider_comparison_pairs pairs
                WHERE pairs.run_id = runs.run_id
            ) AS pair_count
        FROM provider_comparison_runs runs
        ORDER BY created_at DESC
        LIMIT ?
        """,
        [limit],
    )

    latest_datasets = fetch_rows(
        conn,
        """
        WITH ranked AS (
            SELECT
                datasets.*,
                runs.created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY provider
                    ORDER BY runs.created_at DESC
                ) AS provider_rank
            FROM provider_comparison_datasets datasets
            JOIN provider_comparison_runs runs
              ON runs.run_id = datasets.run_id
        )
        SELECT
            run_id,
            created_at,
            provider,
            dataset_kind,
            structure,
            unit,
            symbol,
            exchange,
            timeframe,
            bucket_count,
            total_long,
            total_short,
            peak_long,
            peak_short
        FROM ranked
        WHERE provider_rank = 1
        ORDER BY provider
        LIMIT ?
        """,
        [limit],
    )

    recent_pairs = fetch_rows(
        conn,
        """
        SELECT
            pairs.run_id,
            runs.created_at,
            pairs.left_provider,
            pairs.right_provider,
            pairs.dataset_kind_match,
            pairs.structure_match,
            pairs.unit_match,
            pairs.timeframe_match,
            pairs.bucket_count_ratio,
            pairs.long_total_ratio,
            pairs.short_total_ratio,
            pairs.long_peak_ratio,
            pairs.short_peak_ratio
        FROM provider_comparison_pairs pairs
        JOIN provider_comparison_runs runs
          ON runs.run_id = pairs.run_id
        ORDER BY runs.created_at DESC, pairs.left_provider, pairs.right_provider
        LIMIT ?
        """,
        [limit],
    )

    return {
        "db_path": str(db_path),
        "recent_runs": recent_runs,
        "latest_datasets_by_provider": latest_datasets,
        "recent_pairwise_comparisons": recent_pairs,
    }


def render_text(report: dict[str, object]) -> str:
    """Render the SQL report in plain text."""
    lines: list[str] = []
    lines.append(f"duckdb: {report['db_path']}")
    lines.append("")
    lines.append("Recent runs")
    for row in report["recent_runs"]:
        lines.append(
            f"- {row['run_id']} | {row['created_at']} | "
            f"datasets={row['dataset_count']} pairs={row['pair_count']}"
        )

    lines.append("")
    lines.append("Latest datasets by provider")
    for row in report["latest_datasets_by_provider"]:
        lines.append(
            f"- {row['provider']} | {row['run_id']} | {row['dataset_kind']} | "
            f"{row['structure']} | {row['unit']} | buckets={row['bucket_count']} | "
            f"long={row['total_long']} short={row['total_short']}"
        )

    lines.append("")
    lines.append("Recent pairwise comparisons")
    for row in report["recent_pairwise_comparisons"]:
        lines.append(
            f"- {row['run_id']} | {row['left_provider']} vs {row['right_provider']} | "
            f"unit_match={row['unit_match']} timeframe_match={row['timeframe_match']} | "
            f"long_ratio={row['long_total_ratio']} short_ratio={row['short_total_ratio']}"
        )

    return "\n".join(lines)


def main() -> int:
    """CLI entry point."""
    args = parse_args()
    try:
        import duckdb
    except ImportError as exc:
        print(f"error: duckdb is required ({exc})")
        return 1

    if not args.db_path.exists():
        print(f"error: DuckDB not found: {args.db_path}")
        return 1

    conn = duckdb.connect(str(args.db_path), read_only=True)
    try:
        report = build_report(conn, args.db_path, args.limit)
    finally:
        conn.close()

    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=True))
    else:
        print(render_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
