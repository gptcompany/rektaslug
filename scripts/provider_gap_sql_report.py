#!/usr/bin/env python3
"""Query provider_gap_analysis_* tables and print a compact historical report."""

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
                FROM provider_gap_analysis_scenarios scenarios
                WHERE scenarios.run_id = runs.run_id
            ) AS scenario_count,
            (
                SELECT COUNT(*)
                FROM provider_gap_analysis_leverage leverage
                WHERE leverage.run_id = runs.run_id
            ) AS leverage_row_count
        FROM provider_gap_analysis_runs runs
        ORDER BY created_at DESC
        LIMIT ?
        """,
        [limit],
    )

    latest_scenarios = fetch_rows(
        conn,
        """
        WITH ranked AS (
            SELECT
                scenarios.*,
                runs.created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY scenario_name
                    ORDER BY runs.created_at DESC
                ) AS scenario_rank
            FROM provider_gap_analysis_scenarios scenarios
            JOIN provider_gap_analysis_runs runs
              ON runs.run_id = scenarios.run_id
        )
        SELECT
            run_id,
            created_at,
            scenario_name,
            left_provider,
            right_provider,
            left_bucket_count,
            right_bucket_count,
            total_ratio,
            long_ratio,
            short_ratio,
            left_price_step,
            right_price_step,
            comparison_step,
            shape_cosine,
            distribution_overlap,
            matched_bucket_ratio,
            common_tiers_json
        FROM ranked
        WHERE scenario_rank = 1
        ORDER BY scenario_name
        LIMIT ?
        """,
        [max(limit * 10, 10)],
    )

    latest_internal_alignment = fetch_rows(
        conn,
        """
        WITH latest_run AS (
            SELECT run_id, created_at
            FROM provider_gap_analysis_runs
            ORDER BY created_at DESC
            LIMIT 1
        )
        SELECT
            scenarios.run_id,
            latest_run.created_at,
            scenarios.scenario_name,
            scenarios.left_provider,
            scenarios.right_provider,
            scenarios.total_ratio,
            scenarios.long_ratio,
            scenarios.short_ratio,
            scenarios.shape_cosine,
            scenarios.distribution_overlap
        FROM provider_gap_analysis_scenarios scenarios
        JOIN latest_run
          ON latest_run.run_id = scenarios.run_id
        WHERE scenarios.left_provider = 'internal'
           OR scenarios.right_provider = 'internal'
        ORDER BY scenarios.scenario_name
        LIMIT ?
        """,
        [max(limit * 10, 10)],
    )

    latest_leverage = fetch_rows(
        conn,
        """
        WITH ranked AS (
            SELECT
                leverage.*,
                runs.created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY provider, leverage
                    ORDER BY runs.created_at DESC
                ) AS leverage_rank
            FROM provider_gap_analysis_leverage leverage
            JOIN provider_gap_analysis_runs runs
              ON runs.run_id = leverage.run_id
        )
        SELECT
            run_id,
            created_at,
            provider,
            leverage,
            total_value,
            share_ratio
        FROM ranked
        WHERE leverage_rank = 1
        ORDER BY provider, leverage
        LIMIT ?
        """,
        [limit * 10],
    )

    latest_best_basis = fetch_rows(
        conn,
        """
        SELECT
            scenarios.run_id,
            runs.created_at,
            scenarios.left_provider,
            scenarios.right_provider,
            scenarios.scenario_name,
            scenarios.total_ratio,
            scenarios.long_ratio,
            scenarios.short_ratio,
            scenarios.shape_cosine,
            scenarios.distribution_overlap,
            scenarios.matched_bucket_ratio
        FROM provider_gap_analysis_scenarios scenarios
        JOIN provider_gap_analysis_runs runs
          ON runs.run_id = scenarios.run_id
        WHERE scenarios.scenario_name = 'coinank_common_tiers_rebinned'
        ORDER BY runs.created_at DESC
        LIMIT ?
        """,
        [limit],
    )

    return {
        "db_path": str(db_path),
        "recent_runs": recent_runs,
        "latest_scenarios": latest_scenarios,
        "latest_internal_alignment": latest_internal_alignment,
        "latest_leverage_by_provider": latest_leverage,
        "latest_best_basis_runs": latest_best_basis,
    }


def render_text(report: dict[str, object]) -> str:
    """Render the SQL report in plain text."""
    lines: list[str] = []
    lines.append(f"duckdb: {report['db_path']}")
    lines.append("")
    lines.append("Recent gap-analysis runs")
    for row in report["recent_runs"]:
        lines.append(
            f"- {row['run_id']} | {row['created_at']} | "
            f"scenarios={row['scenario_count']} leverage_rows={row['leverage_row_count']}"
        )

    lines.append("")
    lines.append("Latest scenarios")
    for row in report["latest_scenarios"]:
        lines.append(
            f"- {row['scenario_name']} | {row['run_id']} | "
            f"{row['left_provider']} vs {row['right_provider']} | "
            f"total_ratio={row['total_ratio']} | cosine={row['shape_cosine']} | "
            f"overlap={row['distribution_overlap']}"
        )

    lines.append("")
    lines.append("Latest internal alignment")
    for row in report["latest_internal_alignment"]:
        lines.append(
            f"- {row['scenario_name']} | {row['run_id']} | "
            f"{row['left_provider']} vs {row['right_provider']} | "
            f"total_ratio={row['total_ratio']} long_ratio={row['long_ratio']} "
            f"short_ratio={row['short_ratio']}"
        )

    lines.append("")
    lines.append("Latest leverage composition")
    for row in report["latest_leverage_by_provider"]:
        lines.append(
            f"- {row['provider']} | {row['leverage']}x | "
            f"value={row['total_value']} share={row['share_ratio']}"
        )

    lines.append("")
    lines.append("Latest best-basis runs (coinank_common_tiers_rebinned)")
    for row in report["latest_best_basis_runs"]:
        lines.append(
            f"- {row['run_id']} | {row['left_provider']} vs {row['right_provider']} | "
            f"total_ratio={row['total_ratio']} long_ratio={row['long_ratio']} "
            f"short_ratio={row['short_ratio']}"
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
        print(json.dumps(report, indent=2, ensure_ascii=True, default=str))
    else:
        print(render_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
