"""Focused tests for the consolidated current-format recovery contract."""
import json
import os
import time
from pathlib import Path

import yaml
from click.testing import CliRunner

from pocket_coffea.scripts import check_jobs


def make_jobs(tmp_path, executor="condor@lxplus"):
    (tmp_path / "logs").mkdir()
    submission = {
        "format_version": 1,
        "executor": executor,
        "requires_grid_certificate": False,
        "proxy_transfer_path": None,
        "proxy_source": None,
        "supports_queue_escalation": executor == "condor@lxplus",
    }
    state = {
        "0": {
            "chunksize": 100,
            "request_cpus": 1,
            "request_memory": "4GB",
            "resubmissions": 0,
        }
    }
    if executor == "condor@lxplus":
        state["0"].update({
            "queue": "espresso",
            "base_cpus": 1,
            "base_memory": "4GB",
            "resources_scaled": False,
        })
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump({
        "submission": submission,
        "jobs_list": {"job_0": {"filesets": {}}},
    }))
    (tmp_path / "job_state.json").write_text(json.dumps(state))
    for name in ("job.sh", "inner_run_options.yaml", "resubmit.sub"):
        (tmp_path / name).write_text("")
    (tmp_path / "config_job_0.pkl").write_bytes(b"placeholder")
    (tmp_path / "job_0.sub").write_text("queue\n")
    (tmp_path / "job_0.running").touch()
    return state


def test_current_contract_requires_all_artifacts(tmp_path):
    make_jobs(tmp_path)
    loaded = check_jobs.load_current_contract(tmp_path)
    assert loaded[2]["executor"] == "condor@lxplus"
    (tmp_path / "resubmit.sub").unlink()
    result = CliRunner().invoke(check_jobs.check_jobs, ["-j", str(tmp_path), "--once", "--by", "none"])
    assert result.exit_code != 0
    assert "predates the consolidated" in result.output


def test_save_job_state_is_atomic_and_authoritative(tmp_path):
    state_file = tmp_path / "job_state.json"
    state = {"0": {"resubmissions": 2}}
    check_jobs.save_job_state(state_file, state)
    assert json.loads(state_file.read_text()) == state
    assert not list(tmp_path.glob(".job_state.json.*.tmp"))


def test_passive_monitor_does_not_materialize_log_failure(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    marker = tmp_path / "job_0.running"
    stamp = time.strftime("%m/%d %H:%M:%S", time.localtime(time.time() + 2))
    (tmp_path / "logs" / "job_123.log").write_text(
        f"009 (123.0.000) {stamp} Job was aborted.\n"
        "Job removed by SYSTEM_PERIODIC_REMOVE due to wall time exceeded.\n"
    )
    snapshots = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (tmp_path / "job_state.json", tmp_path / "job_0.sub", marker)
    }
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (_ for _ in ()).throw(AssertionError()))
    result = CliRunner().invoke(
        check_jobs.check_jobs, ["-j", str(tmp_path), "--once", "--by", "none"])
    assert result.exit_code == 0, result.output
    assert not (tmp_path / "job_0.failed").exists()
    assert not (tmp_path / ".check_jobs.lock").exists()
    for path, snapshot in snapshots.items():
        assert (path.read_bytes(), path.stat().st_mtime_ns) == snapshot


def test_rendered_recovery_uses_exact_state(tmp_path):
    make_jobs(tmp_path)
    (tmp_path / "resubmit.sub").write_text(
        "Executable = job.sh\n"
        "RequestCpus = $(CPUS)\n"
        "RequestMemory = $(MEMORY)\n"
        "arguments = $(PROC) $(CHUNKSIZE) $(CPUS)\n"
    )
    state = json.loads((tmp_path / "job_state.json").read_text())
    state["0"].update({
        "queue": "workday", "request_cpus": 3, "request_memory": "12GB",
        "chunksize": 150,
    })
    check_jobs.materialize_job_submit_state(tmp_path, "0", state)
    text = (tmp_path / "job_0.sub").read_text()
    assert "0 workday 150 3 12GB" in text


def test_unknown_current_queue_is_terminal_fallback(tmp_path):
    state = {
        "0": {"queue": "site-specific", "base_cpus": 1, "base_memory": "4GB",
              "request_cpus": 1, "request_memory": "4GB", "resources_scaled": False,
              "chunksize": 1, "resubmissions": 0},
    }
    make_jobs(tmp_path)
    check_jobs.save_job_state(tmp_path / "job_state.json", state)
    _, _, submission, _, _ = check_jobs.load_current_contract(tmp_path)
    candidate = check_jobs.candidate_state(state["0"], submission, 1, 2)
    assert candidate["queue"] == "nextweek"


def test_negative_max_resubmit_is_rejected(tmp_path):
    result = CliRunner().invoke(
        check_jobs.check_jobs, ["-j", str(tmp_path), "--max-resubmit", "-1"])
    assert result.exit_code != 0


def test_explicit_active_recreate_has_nonzero_cli_exit(tmp_path):
    make_jobs(tmp_path)
    result = CliRunner().invoke(
        check_jobs.check_jobs, ["-j", str(tmp_path), "--recreate", "0"])
    assert result.exit_code != 0


def test_xrootd_blocklist_value_is_rejected(tmp_path):
    make_jobs(tmp_path)
    result = CliRunner().invoke(
        check_jobs.check_jobs,
        ["-j", str(tmp_path), "--recreate", "auto", "--blocklist-sites", "root://bad"],
    )
    assert result.exit_code != 0
    assert "CMS/Rucio site names" in result.output


def test_aggregate_basic_counts():
    from pocket_coffea.utils.job_progress import aggregate_by_group

    groups = {"TT": ["job_0", "job_1"], "DATA": ["job_2", "job_3"]}
    result = aggregate_by_group(groups, ["job_1"], [], ["job_0"], ["job_2"])
    assert result["TT"]["pct_done"] == 50.0
    assert result["DATA"]["failed"] == 1


def test_aggregate_empty_group_percentage():
    from pocket_coffea.utils.job_progress import aggregate_by_group

    result = aggregate_by_group({"empty": []}, [], [], [], [])
    assert result["empty"]["pct_done"] == 0.0


def test_aggregate_overlapping_groups():
    from pocket_coffea.utils.job_progress import aggregate_by_group

    result = aggregate_by_group({"A": ["job_0"], "B": ["job_0"]}, [], [], ["job_0"], [])
    assert result["A"]["done"] == result["B"]["done"] == 1


def test_aggregate_100_percent_completion():
    from pocket_coffea.utils.job_progress import aggregate_by_group

    result = aggregate_by_group({"A": ["job_0"]}, [], [], ["job_0"], [])
    assert result["A"]["pct_done"] == 100.0


def test_load_job_to_group_map(tmp_path):
    from pocket_coffea.utils.job_progress import load_job_to_group_map

    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump({
        "jobs_list": {"job_0": {"filesets": {
            "TT_2023": {"metadata": {"sample": "TT"}, "files": []}
        }}}
    }))
    samples, datasets = load_job_to_group_map(str(tmp_path))
    assert samples == {"TT": ["job_0"]}
    assert datasets == {"TT_2023": ["job_0"]}


def test_progress_bar_rendering():
    from pocket_coffea.utils.job_progress import render_progress_bar

    bar = render_progress_bar({"total": 2, "done": 1, "running": 1,
                               "idle": 0, "failed": 0, "pct_done": 50.0}, width=10)
    assert bar.count("█") == 10


def test_progress_table_overlap_presentation():
    table = check_jobs.get_progress_table(
        {"A": {"total": 1, "idle": 0, "running": 0, "done": 1,
                "failed": 0, "pct_done": 100.0}},
        "sample", multi_sample_overlap=True)
    assert "multiple samples" in str(table.title)
