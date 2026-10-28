#!/usr/bin/env python3
"""Replay retained evidence and prove corrupted measurements cannot enter a study."""
import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("process_study", HERE / "verify-process-study.py")
STUDY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STUDY)


class ProcessEvidenceValidationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        source = HERE.parent / "reports/grpc-process-failover-trial-6"
        for name in ("summary.json", "requests.csv", "events.csv", "states.csv"):
            (self.directory / name).write_bytes((source / name).read_bytes())

    def change_report(self, key, value):
        path = self.directory / "summary.json"
        report = json.loads(path.read_text())
        report[key] = value
        path.write_text(json.dumps(report))

    def change_row(self, filename, key, value):
        path = self.directory / filename
        with path.open(newline="") as stream:
            reader = csv.DictReader(stream)
            fields, values = reader.fieldnames, list(reader)
        values[0][key] = str(value)
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(values)

    def test_replays_real_retained_request_event_and_state_evidence(self):
        trial, latencies, workload = STUDY.verify(self.directory)
        self.assertEqual(trial["successful_requests"], len(latencies))
        self.assertEqual(4, workload["clients"])

    def test_rejects_incorrect_elapsed_time_denominator(self):
        self.change_report("successful_writes_per_second", 9999)
        with self.assertRaisesRegex(ValueError, "throughput"):
            STUDY.verify(self.directory)

    def test_rejects_a_fabricated_latency_percentile(self):
        self.change_report("latency_p99_success_ms", 0.001)
        with self.assertRaisesRegex(ValueError, "p99"):
            STUDY.verify(self.directory)

    def test_rejects_request_retry_counter_corruption(self):
        self.change_row("requests.csv", "retry_rpc_attempts", 900)
        with self.assertRaisesRegex(ValueError, "retry counters"):
            STUDY.verify(self.directory)

    def test_rejects_a_missing_process_kill_event(self):
        self.change_row("events.csv", "event", "unrelated_event")
        with self.assertRaisesRegex(ValueError, "fault events"):
            STUDY.verify(self.directory)

    def test_rejects_divergent_durable_session_maps(self):
        self.change_row("states.csv", "sessions_sha256", "0" * 64)
        with self.assertRaisesRegex(ValueError, "divergent"):
            STUDY.verify(self.directory)

    def test_rejects_five_matching_kv_maps_with_the_wrong_final_values(self):
        path = self.directory / "states.csv"
        with path.open(newline="") as stream:
            original = next(csv.DictReader(stream))["kv_sha256"]
        path.write_text(path.read_text().replace(original, "0" * 64))
        with self.assertRaisesRegex(ValueError, "KV state does not match"):
            STUDY.verify(self.directory)

    def test_rejects_missing_delete_retry_guarantee(self):
        self.change_report("replacement_survived_duplicate_delete", False)
        with self.assertRaisesRegex(ValueError, "recovery guarantee"):
            STUDY.verify(self.directory)

    def test_rejects_failed_raw_requests_even_if_json_says_valid(self):
        self.change_row("requests.csv", "success", "false")
        with self.assertRaisesRegex(ValueError, "failed or incorrect"):
            STUDY.verify(self.directory)

    def test_rejects_duplicating_a_trial_in_the_aggregate(self):
        with self.assertRaisesRegex(ValueError, "duplicate trial"):
            STUDY.summarize([self.directory, self.directory])


if __name__ == "__main__":
    unittest.main()
