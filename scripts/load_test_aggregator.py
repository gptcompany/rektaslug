#!/usr/bin/env python3
"""T088-T089: Load testing script for ExchangeAggregator.

Tests aggregator performance under concurrent client load.
Target: Handle 100 concurrent clients without degradation.

Usage:
    python scripts/load_test_aggregator.py --clients 100 --duration 60
"""

import argparse
import asyncio
import logging
import os
import statistics
import time
from datetime import datetime, timezone

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class LoadTestClient:
    """Simulates a client polling the heatmap API."""

    def __init__(self, client_id: int, base_url: str):
        self.client_id = client_id
        self.base_url = base_url
        self.request_times: list[float] = []
        self.errors: list[str] = []
        self.requests_made = 0

    async def run(self, duration_seconds: int) -> dict:
        """Run load test client for specified duration.

        Args:
            duration_seconds: How long to run

        Returns:
            Client stats
        """
        end_time = time.time() + duration_seconds

        async with aiohttp.ClientSession() as session:
            while time.time() < end_time:
                start = time.time()
                try:
                    async with session.get(
                        f"{self.base_url}/liquidations/heatmap-timeseries",
                        params={"symbol": "BTCUSDT", "timeframe": 60},
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as resp:
                        if resp.status == 200:
                            await resp.json()
                            self.request_times.append(time.time() - start)
                        else:
                            self.errors.append(f"HTTP {resp.status}")
                except asyncio.TimeoutError:
                    self.errors.append("Timeout")
                except Exception as e:
                    self.errors.append(str(e))

                self.requests_made += 1

                # Small delay to prevent overwhelming
                await asyncio.sleep(0.5)

        return self.get_stats()

    def get_stats(self) -> dict:
        """Get client statistics.

        Returns:
            Dict with request stats
        """
        if not self.request_times:
            return {
                "client_id": self.client_id,
                "requests": self.requests_made,
                "successful": 0,
                "errors": len(self.errors),
                "avg_latency_ms": 0,
                "p50_ms": 0,
                "p95_ms": 0,
                "p99_ms": 0,
            }

        sorted_times = sorted(self.request_times)
        p50_idx = int(len(sorted_times) * 0.5)
        p95_idx = int(len(sorted_times) * 0.95)
        p99_idx = int(len(sorted_times) * 0.99)

        return {
            "client_id": self.client_id,
            "requests": self.requests_made,
            "successful": len(self.request_times),
            "errors": len(self.errors),
            "avg_latency_ms": statistics.mean(self.request_times) * 1000,
            "p50_ms": sorted_times[p50_idx] * 1000 if sorted_times else 0,
            "p95_ms": sorted_times[min(p95_idx, len(sorted_times) - 1)] * 1000,
            "p99_ms": sorted_times[min(p99_idx, len(sorted_times) - 1)] * 1000,
        }


class LoadTestRunner:
    """Runs load test with multiple concurrent clients."""

    def __init__(self, base_url: str, num_clients: int):
        self.base_url = base_url
        self.num_clients = num_clients
        self.clients: list[LoadTestClient] = []

    async def run(self, duration_seconds: int) -> dict:
        """Run load test with all clients.

        Args:
            duration_seconds: Test duration

        Returns:
            Aggregated results
        """
        logger.info(f"Starting load test: {self.num_clients} clients for {duration_seconds}s")
        logger.info(f"Target URL: {self.base_url}")

        self.clients = [LoadTestClient(i, self.base_url) for i in range(self.num_clients)]

        start_time = time.time()

        # Run all clients concurrently
        tasks = [client.run(duration_seconds) for client in self.clients]
        results = await asyncio.gather(*tasks)

        total_time = time.time() - start_time

        # Aggregate results
        return self._aggregate_results(results, total_time)

    def _aggregate_results(self, client_results: list[dict], total_time: float) -> dict:
        """Aggregate results from all clients.

        Args:
            client_results: List of per-client stats
            total_time: Total test duration

        Returns:
            Aggregated stats
        """
        total_requests = sum(r["requests"] for r in client_results)
        total_successful = sum(r["successful"] for r in client_results)
        total_errors = sum(r["errors"] for r in client_results)

        all_latencies = []
        for client in self.clients:
            all_latencies.extend([t * 1000 for t in client.request_times])

        if all_latencies:
            sorted_latencies = sorted(all_latencies)
            avg_latency = statistics.mean(all_latencies)
            p50 = sorted_latencies[int(len(sorted_latencies) * 0.5)]
            p95 = sorted_latencies[
                min(int(len(sorted_latencies) * 0.95), len(sorted_latencies) - 1)
            ]
            p99 = sorted_latencies[
                min(int(len(sorted_latencies) * 0.99), len(sorted_latencies) - 1)
            ]
        else:
            avg_latency = p50 = p95 = p99 = 0

        return {
            "test_time": datetime.now(timezone.utc).isoformat(),
            "num_clients": self.num_clients,
            "duration_seconds": total_time,
            "total_requests": total_requests,
            "successful_requests": total_successful,
            "failed_requests": total_errors,
            "success_rate_pct": (total_successful / total_requests * 100) if total_requests else 0,
            "requests_per_second": total_requests / total_time if total_time else 0,
            "avg_latency_ms": avg_latency,
            "p50_latency_ms": p50,
            "p95_latency_ms": p95,
            "p99_latency_ms": p99,
        }


async def main():
    parser = argparse.ArgumentParser(description="Load test the aggregator API")
    parser.add_argument(
        "--clients",
        type=int,
        default=100,
        help="Number of concurrent clients (default: 100)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=os.environ.get("HEATMAP_API_URL", "http://localhost:8002"),
        help="Base URL of the API (default: HEATMAP_API_URL or http://localhost:8002)",
    )
    args = parser.parse_args()

    runner = LoadTestRunner(args.url, args.clients)
    results = await runner.run(args.duration)

    # Report results
    print("\n" + "=" * 60)
    print("LOAD TEST RESULTS")
    print("=" * 60)
    print(f"Clients: {results['num_clients']}")
    print(f"Duration: {results['duration_seconds']:.1f}s")
    print(f"Total Requests: {results['total_requests']}")
    print(f"Successful: {results['successful_requests']}")
    print(f"Failed: {results['failed_requests']}")
    print(f"Success Rate: {results['success_rate_pct']:.1f}%")
    print(f"Requests/sec: {results['requests_per_second']:.1f}")
    print("-" * 60)
    print("LATENCY")
    print(f"  Average: {results['avg_latency_ms']:.1f}ms")
    print(f"  P50: {results['p50_latency_ms']:.1f}ms")
    print(f"  P95: {results['p95_latency_ms']:.1f}ms")
    print(f"  P99: {results['p99_latency_ms']:.1f}ms")
    print("=" * 60)

    # Pass/fail based on targets
    target_p95_ms = 7000  # <7s from spec
    if results["p95_latency_ms"] <= target_p95_ms and results["success_rate_pct"] >= 95:
        print("PASS: Performance meets targets")
    else:
        print(f"FAIL: P95 {results['p95_latency_ms']:.0f}ms (target: {target_p95_ms}ms)")


if __name__ == "__main__":
    asyncio.run(main())
