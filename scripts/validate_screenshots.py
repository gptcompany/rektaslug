#!/usr/bin/env python3
"""Validate Coinglass heatmap screenshots against our API predictions.

This script extracts price levels from Coinglass screenshots using OCR
and compares them against our heatmap API to calculate hit_rate metrics.

Usage:
    # Single screenshot validation
    uv run python scripts/validate_screenshots.py \
        --screenshots /path/to/screenshot.png \
        --verbose

    # Batch validation
    uv run python scripts/validate_screenshots.py \
        --screenshots /path/to/screenshots/ \
        --output validation_results.jsonl \
        --workers 8

    # CI integration (exit code 1 if below threshold)
    uv run python scripts/validate_screenshots.py \
        --screenshots /path/to/screenshots/ \
        --threshold 0.70 \
        --fail-below-threshold
"""

import argparse
import asyncio
import gc
import json
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob
from pathlib import Path

from tqdm import tqdm

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from liquidationheatmap.validation import (
    AggregateMetrics,
    OCRExtractor,
    Screenshot,
    ValidationResult,
    ZoneComparator,
    calculate_aggregate_metrics,
)

# Chunk size for memory management in batch mode
CHUNK_SIZE = 100

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate Coinglass screenshots against our heatmap API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--screenshots",
        type=str,
        required=True,
        help="Path to screenshot file or directory",
    )

    parser.add_argument(
        "--api-url",
        type=str,
        default=os.environ.get("HEATMAP_API_URL", "http://localhost:8002"),
        help="Heatmap API base URL (default: http://localhost:8002, override with HEATMAP_API_URL)",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="data/validation/validation_results.jsonl",
        help="Output JSONL file path (default: data/validation/validation_results.jsonl)",
    )

    parser.add_argument(
        "--tolerance",
        type=float,
        default=1.0,
        help="Price match tolerance percentage (default: 1.0%%)",
    )

    parser.add_argument(
        "--threshold",
        type=float,
        default=0.70,
        help="Pass/fail hit_rate threshold (default: 0.70)",
    )

    parser.add_argument(
        "--fail-below-threshold",
        action="store_true",
        help="Exit with code 1 if avg_hit_rate < threshold",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1, use 8 for batch)",
    )

    parser.add_argument(
        "--filter-symbol",
        type=str,
        choices=["BTC", "ETH"],
        help="Only validate screenshots for specific symbol",
    )

    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show aggregate summary after validation",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List screenshots without processing",
    )

    return parser.parse_args()


def find_screenshots(path: str, filter_symbol: str | None = None) -> list[str]:
    """Find Coinglass screenshots in path.

    Args:
        path: File or directory path
        filter_symbol: Optional symbol filter ("BTC" or "ETH")

    Returns:
        List of absolute paths to screenshots
    """
    path = os.path.abspath(path)

    if os.path.isfile(path):
        return [path]

    if not os.path.isdir(path):
        raise FileNotFoundError(f"Path not found: {path}")

    # Find all coinglass PNG files
    pattern = os.path.join(path, "coinglass_*.png")
    files = sorted(glob(pattern))

    # Filter by symbol if specified
    if filter_symbol:
        filter_symbol = filter_symbol.lower()
        files = [f for f in files if f"coinglass_{filter_symbol}_" in f.lower()]

    return files


def validate_single_screenshot(
    screenshot_path: str,
    api_url: str,
    tolerance_pct: float,
) -> ValidationResult:
    """Validate a single screenshot (for multiprocessing).

    Args:
        screenshot_path: Path to screenshot file
        api_url: Heatmap API base URL
        tolerance_pct: Price match tolerance

    Returns:
        ValidationResult
    """
    start_time = time.time()

    try:
        # Parse screenshot metadata
        screenshot = Screenshot.from_path(screenshot_path)

        # Extract price levels with OCR
        extractor = OCRExtractor(use_easyocr_fallback=True)
        ocr_result = extractor.extract(
            screenshot_path,
            symbol=screenshot.symbol,
        )

        # Compare with API (run async in sync context)
        comparator = ZoneComparator(
            api_url=api_url,
            tolerance_pct=tolerance_pct,
        )

        result = asyncio.run(
            comparator.compare(
                ocr_result=ocr_result,
                screenshot_timestamp=screenshot.timestamp,
                symbol=screenshot.symbol,
            )
        )

        return result

    except Exception as e:
        processing_time_ms = int((time.time() - start_time) * 1000)
        return ValidationResult(
            screenshot_path=screenshot_path,
            timestamp=None,
            symbol="",
            status="error",
            processing_time_ms=processing_time_ms,
            error=str(e),
        )


def write_result_jsonl(result: ValidationResult, output_path: str) -> None:
    """Append result to JSONL file.

    Args:
        result: ValidationResult to write
        output_path: Path to output file
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "a") as f:
        json.dump(result.to_dict(), f)
        f.write("\n")


def write_summary_json(metrics: AggregateMetrics, output_dir: str) -> str:
    """Write aggregate summary to JSON file.

    Args:
        metrics: AggregateMetrics to write
        output_dir: Directory for output file

    Returns:
        Path to written file
    """
    summary_path = os.path.join(output_dir, "validation_summary.json")
    with open(summary_path, "w") as f:
        json.dump(metrics.to_dict(), f, indent=2)
    return summary_path


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Find screenshots
    try:
        screenshots = find_screenshots(args.screenshots, args.filter_symbol)
    except FileNotFoundError as e:
        logger.error(str(e))
        return 1

    if not screenshots:
        logger.error("No screenshots found matching criteria")
        return 1

    logger.info(f"Found {len(screenshots)} screenshots to validate")

    if args.dry_run:
        for path in screenshots[:10]:
            logger.info(f"  {os.path.basename(path)}")
        if len(screenshots) > 10:
            logger.info(f"  ... and {len(screenshots) - 10} more")
        return 0

    # Clear output file if exists
    if os.path.exists(args.output):
        os.remove(args.output)

    # Process screenshots
    results: list[ValidationResult] = []
    total = len(screenshots)

    if args.workers > 1 and total > 1:
        # Parallel processing with tqdm and chunked memory management
        logger.info(f"Processing with {args.workers} workers in chunks of {CHUNK_SIZE}...")

        # Process in chunks to manage memory
        for chunk_start in range(0, total, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, total)
            chunk = screenshots[chunk_start:chunk_end]
            chunk_num = chunk_start // CHUNK_SIZE + 1
            total_chunks = (total + CHUNK_SIZE - 1) // CHUNK_SIZE

            logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} screenshots)")

            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                futures = {
                    executor.submit(
                        validate_single_screenshot,
                        path,
                        args.api_url,
                        args.tolerance,
                    ): path
                    for path in chunk
                }

                with tqdm(total=len(chunk), desc=f"Chunk {chunk_num}", unit="img") as pbar:
                    for future in as_completed(futures):
                        path = futures[future]
                        try:
                            result = future.result()
                            results.append(result)
                            write_result_jsonl(result, args.output)

                            status = "✓" if result.status == "success" else "✗"
                            pbar.set_postfix(
                                hit=f"{result.hit_rate:.0%}",
                                status=status,
                            )
                        except Exception as e:
                            logger.error(f"Failed {path}: {e}")
                        pbar.update(1)

            # Clear OCR cache between chunks to manage memory
            gc.collect()

    else:
        # Sequential processing with tqdm
        with tqdm(total=total, desc="Validating", unit="img") as pbar:
            for path in screenshots:
                result = validate_single_screenshot(path, args.api_url, args.tolerance)
                results.append(result)
                write_result_jsonl(result, args.output)

                status = "✓" if result.status == "success" else "✗"
                pbar.set_postfix(
                    hit=f"{result.hit_rate:.0%}",
                    conf=f"{result.ocr_confidence:.0%}",
                    status=status,
                )
                pbar.update(1)

    # Calculate aggregate metrics
    metrics = calculate_aggregate_metrics(results, threshold=args.threshold)

    # Write summary
    output_dir = os.path.dirname(args.output) or "data/validation"
    summary_path = write_summary_json(metrics, output_dir)

    # Display summary
    logger.info("=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total screenshots: {metrics.total_screenshots}")
    logger.info(f"Successfully processed: {metrics.processed}")
    logger.info(f"OCR failures: {metrics.ocr_failures}")
    logger.info(f"API failures: {metrics.api_failures}")
    logger.info("-" * 40)
    logger.info(f"Average hit_rate: {metrics.avg_hit_rate:.2%}")
    logger.info(f"Median hit_rate: {metrics.median_hit_rate:.2%}")
    logger.info(f"Pass rate (>={args.threshold:.0%}): {metrics.pass_rate:.2%}")
    logger.info("-" * 40)
    logger.info(f"Results written to: {args.output}")
    logger.info(f"Summary written to: {summary_path}")

    # Check threshold for CI
    if args.fail_below_threshold:
        if metrics.avg_hit_rate < args.threshold:
            logger.error(
                f"FAILED: avg_hit_rate {metrics.avg_hit_rate:.2%} < threshold {args.threshold:.0%}"
            )
            return 1
        else:
            logger.info(
                f"PASSED: avg_hit_rate {metrics.avg_hit_rate:.2%} >= threshold {args.threshold:.0%}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
