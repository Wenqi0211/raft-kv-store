#!/usr/bin/env python3
"""Validate independent-JVM raw evidence before aggregating local fault trials."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def rows(path: Path) -> list[dict]:
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def close(actual: float, expected: float, label: str) -> None:
    require(math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-8), f"incorrect {label}")


def percentile(latencies: list[int], percent: int) -> float:
    ordered = sorted(latencies)
    return ordered[max(0, math.ceil(percent / 100 * len(ordered)) - 1)] / 1e6


def verify(directory: Path) -> tuple[dict, list[int], dict]:
    report = json.loads((directory / "summary.json").read_text())
    require(report["valid"] is True, "run is invalid")
    require(report["schema_version"] == 2 and report["node_count"] == report["raft_node_processes"] == 5,
            "expected five independent node processes")
    require(report["forced_process_exits"] == 1, "expected one forced leader exit")
    samples = rows(directory / "requests.csv")
    require(bool(samples), "missing measured requests")
    require(all(s["success"] == s["response_success"] == "true" for s in samples), "failed or incorrect request")
    require(len(samples) == report["attempted_requests"] == report["successful_requests"], "request count mismatch")
    require(report["failed_requests"] == 0, "reported request failures")
    require({int(s["sequence"]) for s in samples} == set(range(1, len(samples) + 1)), "missing or duplicate request sequence")
    require({int(s["client"]) for s in samples} == set(range(report["clients"])), "missing client traffic")
    latencies = [int(s["latency_nanos"]) for s in samples]
    elapsed = report["actual_elapsed_seconds"]
    kill = report["kill_offset_nanos"]
    require(elapsed >= report["requested_duration_seconds"], "short throughput denominator")
    require(report["requested_kill_after_seconds"] * 1e9 <= kill < report["requested_duration_seconds"] * 1e9,
            "kill did not occur inside the requested traffic window")
    for sample, latency in zip(samples, latencies):
        start = int(sample["start_offset_nanos"])
        client = int(sample["client"])
        key = int(sample["key"].removeprefix("k"))
        attempts = int(sample["rpc_attempts"])
        require(sample["key"] == f"k{key}" and 0 <= key < report["key_count"]
                and key % report["clients"] == client, "key ownership mismatch")
        require(start >= 0 and latency > 0 and start + latency <= elapsed * 1e9 + 1000, "invalid request timing")
        require(attempts >= 1 and int(sample["retry_rpc_attempts"]) == attempts - 1, "invalid retry counters")
        require(int(sample["backoff_nanos"]) >= 0, "negative backoff")
    close(len(samples) / elapsed, report["successful_writes_per_second"], "throughput denominator")
    for percent in (50, 99):
        close(percentile(latencies, percent), report[f"latency_p{percent}_success_ms"], f"p{percent}")
    require(sum(int(s["rpc_attempts"]) for s in samples) == report["client_rpc_attempts"], "RPC count mismatch")
    require(sum(int(s["retry_rpc_attempts"]) for s in samples) == report["client_retry_rpc_attempts"], "retry count mismatch")
    require(sum(int(s["retry_rpc_attempts"]) > 0 for s in samples) == report["successful_requests_with_retry"], "retried success mismatch")
    close(sum(int(s["backoff_nanos"]) for s in samples) / 1e6, report["client_backoff_total_ms"], "backoff")
    before = sum(int(s["start_offset_nanos"]) + int(s["latency_nanos"]) < kill for s in samples)
    after = sum(int(s["start_offset_nanos"]) >= kill for s in samples)
    require(before == report["pre_kill_successful_requests"] and before > 0, "missing pre-kill traffic")
    require(after == report["post_kill_started_successful_requests"] and after > 0, "missing post-kill traffic")

    timeline = rows(directory / "events.csv")
    names = ["leader_confirmed", "kill_invoked", "process_terminated", "replacement_put_started", "replacement_put_confirmed",
             "delete_retry_confirmed", "restart_invoked", "maps_and_sessions_converged"]
    require([e["event"] for e in timeline] == names, "missing, duplicated, or reordered fault events")
    times = [int(e["offset_nanos"]) for e in timeline]
    require(times == sorted(times) and times[1] == kill, "invalid fault event timestamps")
    require(all(e["node"] == report["killed_leader"] for e in timeline), "fault node mismatch")
    require(all(int(e["pid"]) == report["killed_leader_pid"] for e in timeline[:-1]), "killed PID mismatch")
    require(int(timeline[-1]["pid"]) == report["restarted_leader_pid"] != report["killed_leader_pid"], "restart PID mismatch")
    close((times[4] - times[1]) / 1e6, report["leader_kill_to_confirmed_replacement_put_ms"], "failover interval")
    close((times[-1] - times[-2]) / 1e6, report["restart_to_complete_convergence_ms"], "convergence interval")
    observed = json.loads(report["pre_kill_leader_status_json"])
    require(observed["role"] == "LEADER" and observed["running"] is True
            and observed["id"] == observed["leaderId"] == report["killed_leader"]
            and observed["term"] == report["pre_kill_leader_term"] > 0, "invalid pre-kill leader evidence")
    require(report["discarded_committed_delete_replies"] == 1 and report["delete_rpc_attempts"] >= 2,
            "missing discarded reply or DELETE retry")
    for flag in ("delete_request_identity_preserved", "delete_returned_original_value",
                 "replacement_survived_duplicate_delete", "all_five_maps_and_sessions_match",
                 "restarted_node_snapshot_advanced"):
        require(report[flag] is True, f"failed recovery guarantee: {flag}")

    states = rows(directory / "states.csv")
    require(len(states) == 5 and {s["node"] for s in states} == {f"n{i}" for i in range(1, 6)}, "missing node state")
    require(len({s["pid"] for s in states}) == 5, "node states are not from five processes")
    for field in ("kv_sha256", "sessions_sha256"):
        require(len({s[field] for s in states}) == 1, f"divergent {field}")
        require(all(len(s[field]) == 64 and all(c in "0123456789abcdef" for c in s[field]) for s in states), "invalid state hash")
    def value(sequence: int) -> str:
        return "v" + str(sequence).zfill(report["value_bytes"] - 1)
    expected = {f"k{i}": value(i) for i in range(report["key_count"])}
    for sample in sorted(samples, key=lambda s: int(s["sequence"])):
        expected[sample["key"]] = value(int(sample["sequence"]) + report["key_count"])
    expected.update({"canary": "stable", "retry-delete": "after-crash"})
    # Java TreeMap.toString for this generated ASCII workload; the live driver also
    # compares the actual maps, independently of these retained fingerprints.
    canonical = "{" + ", ".join(key + "=" + expected[key] for key in sorted(expected)) + "}"
    expected_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    require(all(s["kv_sha256"] == expected_hash for s in states), "KV state does not match successful requests")
    require(all(int(s["key_count"]) == report["verified_final_key_count"] == report["key_count"] + 2 for s in states), "key count mismatch")
    require(all(int(s["session_count"]) == report["verified_final_session_count"] for s in states), "session count mismatch")
    require(all(int(s["applied_index"]) >= report["verified_target_applied_index"] for s in states), "node has unapplied operations")
    restarted = next(s for s in states if s["node"] == report["killed_leader"])
    require(int(restarted["pid"]) == report["restarted_leader_pid"], "restarted node state PID mismatch")
    require(int(restarted["snapshot_index"]) == report["restarted_node_snapshot_index"] > report["killed_node_applied_index"],
            "restarted node did not install an advanced snapshot")
    workload = {k: report[k] for k in ("clients", "key_count", "value_bytes", "requested_duration_seconds",
                "requested_kill_after_seconds", "snapshot_threshold", "election_minimum_ms",
                "election_maximum_exclusive_ms", "heartbeat_interval_ms", "client_logical_request_budget_ms",
                "client_per_rpc_cap_ms", "fault_probe_logical_request_budget_ms")}
    workload["election_timer_policy"] = report.get("election_timer_policy", "legacy rejected-vote deadline reset")
    trial = {k: report[k] for k in ("successful_requests", "failed_requests", "actual_elapsed_seconds",
             "successful_writes_per_second", "latency_p99_success_ms", "client_retry_rpc_attempts",
             "successful_requests_with_retry", "leader_kill_to_confirmed_replacement_put_ms",
             "restart_to_complete_convergence_ms")}
    trial["directory"] = str(directory)
    trial["input_sha256"] = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                             for name in ("summary.json", "requests.csv", "events.csv", "states.csv")}
    return trial, latencies, workload


def summarize(directories: list[Path]) -> dict:
    require(len({p.resolve() for p in directories}) == len(directories), "duplicate trial directory")
    trials, latencies, workload = [], [], None
    for directory in directories:
        trial, measured, parameters = verify(directory)
        require(workload is None or parameters == workload, "inconsistent trial workload")
        workload = parameters
        trials.append(trial)
        latencies.extend(measured)
    require(bool(trials), "no trials")
    elapsed = sum(t["actual_elapsed_seconds"] for t in trials)
    return {
        "valid": True, "trial_count": len(trials), "workload": workload, "trials": trials,
        "successful_requests": len(latencies), "failed_requests": 0,
        "pooled_elapsed_seconds": elapsed, "pooled_writes_per_second": len(latencies) / elapsed,
        "pooled_p99_success_ms": percentile(latencies, 99),
        "min_observed_kill_to_replacement_put_ms": min(t["leader_kill_to_confirmed_replacement_put_ms"] for t in trials),
        "max_observed_kill_to_replacement_put_ms": max(t["leader_kill_to_confirmed_replacement_put_ms"] for t in trials),
        "scope": "One machine, five independent JVMs, active PUT traffic across one forced leader-process exit per trial. Separate committed DELETE reply discarded by a test transport. Observed intervals are not a failover percentile or multi-host result.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trials", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.trials)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result, indent=2))
