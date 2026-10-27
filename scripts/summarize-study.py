#!/usr/bin/env python3
"""Verify raw latency records against JSON, then summarize separate local trials."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from pathlib import Path


def nearest_rank(values: list[int], percentile: int) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile / 100 * len(ordered)) - 1)] / 1e6


def summarize(paths: list[Path]) -> dict:
    trials = []
    pooled_latencies = []
    total_seconds = total_success = 0
    workload = None
    for directory in paths:
        summary_path = directory / "summary.json"
        sample_path = directory / "requests.csv"
        report = json.loads(summary_path.read_text())
        assert report["valid"] is True, f"invalid measurement: {directory}"
        parameters = {key: report[key] for key in (
            "node_count", "clients", "key_count", "value_bytes", "requested_duration_seconds",
            "snapshot_threshold", "election_minimum_ms", "election_maximum_exclusive_ms",
            "heartbeat_interval_ms", "client_logical_request_budget_ms", "client_per_rpc_cap_ms")}
        if workload is None:
            workload = parameters
        assert parameters == workload, f"inconsistent workload or timer settings: {directory}"
        with sample_path.open(newline="") as stream:
            samples = list(csv.DictReader(stream))
        successful = [s for s in samples if s["success"] == "true"]
        latencies = [int(s["latency_nanos"]) for s in successful]
        assert len(samples) == report["attempted_requests"]
        assert len(successful) == report["successful_requests"]
        assert len(samples) - len(successful) == report["failed_requests"] == 0
        assert len({int(s["sequence"]) for s in samples}) == len(samples)
        assert all(int(s["latency_nanos"]) > 0 for s in samples)
        rpc_attempts = sum(int(s["rpc_attempts"]) for s in samples)
        retry_attempts = sum(int(s["retry_rpc_attempts"]) for s in samples)
        assert all(int(s["rpc_attempts"]) >= 1 for s in samples)
        assert all(int(s["retry_rpc_attempts"]) == int(s["rpc_attempts"]) - 1 for s in samples)
        assert rpc_attempts == report["client_rpc_attempts"]
        assert retry_attempts == report["client_retry_rpc_attempts"]
        assert math.isclose(sum(int(s["backoff_nanos"]) for s in samples)/1e6,
                            report["client_backoff_total_ms"], abs_tol=1e-8)
        elapsed = report["actual_elapsed_seconds"]
        assert elapsed >= report["requested_duration_seconds"]
        assert math.isclose(len(successful) / elapsed, report["successful_writes_per_second"], rel_tol=1e-10)
        assert math.isclose(nearest_rank(latencies, 99), report["latency_p99_success_ms"], abs_tol=1e-9)
        assert report["all_five_maps_and_sessions_match"] is True
        assert report["node_count"] == 5
        assert report["simultaneously_stopped_nodes"] == 2
        trials.append({
            "directory": str(directory),
            "successful_requests": len(successful),
            "failed_requests": 0,
            "actual_elapsed_seconds": elapsed,
            "writes_per_second": report["successful_writes_per_second"],
            "p99_success_ms": report["latency_p99_success_ms"],
            "leader_stop_to_confirmed_write_ms": report["leader_stop_to_confirmed_write_ms"],
            "client_rpc_attempts": rpc_attempts,
            "client_retry_rpc_attempts": retry_attempts,
            "successful_requests_with_retry": report["successful_requests_with_retry"],
            "timed_phase_term_increase": report["timed_phase_term_increase"],
            "csv_sha256": hashlib.sha256(sample_path.read_bytes()).hexdigest(),
            "summary_sha256": hashlib.sha256(summary_path.read_bytes()).hexdigest(),
        })
        total_seconds += elapsed
        total_success += len(successful)
        pooled_latencies.extend(latencies)
    return {
        "trials": trials,
        "workload": workload,
        "trial_count": len(trials),
        "successful_requests": total_success,
        "failed_requests": 0,
        "client_rpc_attempts": sum(t["client_rpc_attempts"] for t in trials),
        "client_retry_rpc_attempts": sum(t["client_retry_rpc_attempts"] for t in trials),
        "successful_requests_with_retry": sum(t["successful_requests_with_retry"] for t in trials),
        "pooled_elapsed_seconds": total_seconds,
        "pooled_writes_per_second": total_success / total_seconds,
        "median_trial_writes_per_second": statistics.median(t["writes_per_second"] for t in trials),
        "min_trial_writes_per_second": min(t["writes_per_second"] for t in trials),
        "max_trial_writes_per_second": max(t["writes_per_second"] for t in trials),
        "pooled_p99_success_ms": nearest_rank(pooled_latencies, 99),
        "worst_trial_p99_success_ms": max(t["p99_success_ms"] for t in trials),
        "min_observed_leader_stop_to_confirmed_write_ms": min(t["leader_stop_to_confirmed_write_ms"] for t in trials),
        "max_observed_leader_stop_to_confirmed_write_ms": max(t["leader_stop_to_confirmed_write_ms"] for t in trials),
        "scope": "Local, one JVM with five real-gRPC nodes; closed-loop PUT only. Failover observations use explicit stop/start, not process kills or a failover percentile.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("trials", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.trials)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result, indent=2))
