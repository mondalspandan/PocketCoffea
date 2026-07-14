"""Unit tests for the per-sample / per-dataset progress logic in
`pocket-coffea check-jobs`.

The pure aggregator and the jobs_config.yaml loader live in
`pocket_coffea/utils/job_progress.py` precisely so they can be tested
without pulling in rucio / coffea / rich. The progress-bar renderer
itself does need `rich.markup` strings, but it's pure-string and tested
here directly from check_jobs.py.
"""
import os
import json

import pytest
from click.testing import CliRunner

from pocket_coffea.utils.job_progress import (
    aggregate_by_group,
    load_job_to_group_map,
)


def test_condor_job_state_preserves_per_job_chunksizes():
    from pocket_coffea.executors.executors_lxplus import build_condor_job_state

    state = build_condor_job_state([100, 250], "espresso", 2, "4GB")

    assert state["0"]["chunksize"] == 100
    assert state["1"]["chunksize"] == 250
    assert state["0"]["queue"] == "espresso"
    assert state["0"]["base_cpus"] == state["0"]["request_cpus"] == 2
    assert state["0"]["base_memory"] == state["0"]["request_memory"] == "4GB"


def test_condor_shutdown_policy_requests_five_minute_sigterm_window():
    from pocket_coffea.executors.executors_lxplus import build_condor_shutdown_policy

    policy = build_condor_shutdown_policy()

    assert "JobCurrentStartDate" in policy["periodic_remove"]
    assert "EnteredCurrentStatus" not in policy["periodic_remove"]
    assert "MaxRuntime - 300" in policy["periodic_remove"]
    assert policy["want_graceful_removal"] is True
    assert policy["job_max_vacate_time"] == 300
    assert policy["kill_sig"] == 15


# ----------------------- aggregate_by_group -----------------------

def test_aggregate_basic_counts():
    """Mixed statuses across three samples; verify totals and percentages."""
    group_to_jobs = {
        "TT":  ["job_0", "job_1", "job_2", "job_3"],
        "ttH": ["job_4", "job_5"],
        "DATA":["job_6", "job_7", "job_8", "job_9"],
    }
    idle    = ["job_3", "job_5"]
    running = ["job_2"]
    done    = ["job_0", "job_1", "job_4", "job_6", "job_7"]
    failed  = ["job_8", "job_9"]

    res = aggregate_by_group(group_to_jobs, idle, running, done, failed)

    assert res["TT"]   == {"total": 4, "idle": 1, "running": 1, "done": 2, "failed": 0, "pct_done": 50.0}
    assert res["ttH"]  == {"total": 2, "idle": 1, "running": 0, "done": 1, "failed": 0, "pct_done": 50.0}
    assert res["DATA"] == {"total": 4, "idle": 0, "running": 0, "done": 2, "failed": 2, "pct_done": 50.0}


def test_aggregate_empty_group_gives_zero_pct():
    res = aggregate_by_group({"EmptySample": []}, [], [], [], [])
    assert res["EmptySample"] == {"total": 0, "idle": 0, "running": 0, "done": 0, "failed": 0, "pct_done": 0.0}


def test_aggregate_overlap_counts_under_each_group():
    """A job that touches two samples (uniform-split case) must be counted
    under both — that's the documented behaviour, called out in the table
    title via `multi_sample_overlap`."""
    group_to_jobs = {"A": ["job_0", "job_1"], "B": ["job_0", "job_2"]}
    done = ["job_0"]
    res = aggregate_by_group(group_to_jobs, [], [], done, [])
    assert res["A"]["done"] == 1
    assert res["B"]["done"] == 1
    # Each group still sees the job once even though it's listed in both.
    assert res["A"]["total"] == 2
    assert res["B"]["total"] == 2


def test_aggregate_pct_done_full_completion():
    group_to_jobs = {"ttH": ["job_0", "job_1", "job_2"]}
    done = ["job_0", "job_1", "job_2"]
    res = aggregate_by_group(group_to_jobs, [], [], done, [])
    assert res["ttH"]["pct_done"] == 100.0


# ----------------------- load_job_to_group_map -----------------------

def test_load_returns_none_when_yaml_missing(tmp_path):
    sample_map, ds_map = load_job_to_group_map(str(tmp_path))
    assert sample_map is None
    assert ds_map is None


def test_load_builds_reverse_maps(tmp_path):
    import yaml as pyyaml
    cfg = {
        "jobs_list": {
            "job_0": {"filesets": {"TT_2018":  {"metadata": {"sample": "TT"},  "files": []}}},
            "job_1": {"filesets": {"TT_2018":  {"metadata": {"sample": "TT"},  "files": []}}},
            "job_2": {"filesets": {"ttH_2018": {"metadata": {"sample": "ttH"}, "files": []}}},
            # uniform-split: one job touching two samples
            "job_3": {"filesets": {
                "TT_2018":  {"metadata": {"sample": "TT"},  "files": []},
                "ttH_2018": {"metadata": {"sample": "ttH"}, "files": []},
            }},
        }
    }
    yaml_path = tmp_path / "jobs_config.yaml"
    yaml_path.write_text(pyyaml.dump(cfg))

    sample_map, ds_map = load_job_to_group_map(str(tmp_path))

    assert set(sample_map["TT"])  == {"job_0", "job_1", "job_3"}
    assert set(sample_map["ttH"]) == {"job_2", "job_3"}
    assert set(ds_map["TT_2018"])  == {"job_0", "job_1", "job_3"}
    assert set(ds_map["ttH_2018"]) == {"job_2", "job_3"}


# ----------------------- _render_progress_bar -----------------------

def test_progress_bar_renders_each_status_color():
    """The bar renderer composes rich markup. Verify each colour appears
    when its share is > 0 and the total width is preserved."""
    from pocket_coffea.utils.job_progress import render_progress_bar as _render_progress_bar

    counts = {"total": 10, "done": 4, "running": 2, "idle": 3, "failed": 1, "pct_done": 40.0}
    bar = _render_progress_bar(counts, width=20)

    # All four colour tags must be present
    for color in ("green", "magenta", "blue", "red"):
        assert f"[{color}]" in bar, f"missing colour {color} in bar: {bar!r}"

    # Total filled characters equal `width` regardless of rounding
    filled = bar.count("█")
    assert filled == 20


def test_progress_bar_empty_group_placeholder():
    from pocket_coffea.utils.job_progress import render_progress_bar as _render_progress_bar

    counts = {"total": 0, "done": 0, "running": 0, "idle": 0, "failed": 0, "pct_done": 0.0}
    bar = _render_progress_bar(counts, width=20)
    assert "·" in bar
    # No coloured segments
    for color in ("green", "magenta", "blue", "red"):
        assert f"[{color}]" not in bar


def test_progress_bar_skips_zero_segments():
    from pocket_coffea.utils.job_progress import render_progress_bar as _render_progress_bar

    counts = {"total": 4, "done": 4, "running": 0, "idle": 0, "failed": 0, "pct_done": 100.0}
    bar = _render_progress_bar(counts, width=12)

    # Only green segment
    assert "[green]" in bar
    for color in ("magenta", "blue", "red"):
        assert f"[{color}]" not in bar
    assert bar.count("█") == 12


def test_check_jobs_logs_reports_timeout_marker(tmp_path):
    from pocket_coffea.scripts.check_jobs import check_jobs_logs

    (tmp_path / "job_0.timeout").write_text("")

    idle, running, done, failed, timeout = check_jobs_logs(tmp_path)

    assert idle == []
    assert running == []
    assert done == []
    assert failed == []
    assert timeout == ["job_0"]


def test_convert_timeout_jobs_bumps_queue_and_marks_failed(tmp_path):
    from pocket_coffea.scripts.check_jobs import convert_timeout_jobs

    (tmp_path / "job_0.timeout").write_text("")
    (tmp_path / "job_0.running").write_text("")
    state_file = tmp_path / "job_state.json"
    job_state = {
        "0": {
            "queue": "espresso",
            "chunksize": 100,
            "base_cpus": 2,
            "base_memory": "4GB",
            "request_cpus": 2,
            "request_memory": "4GB",
            "resources_scaled": False,
            "resubmissions": 0,
        }
    }
    state_file.write_text(json.dumps(job_state))
    log_text = []

    converted = convert_timeout_jobs(
        tmp_path, ["job_0"], ["job_0"], [], [], queue_shift=1, ncpu=3,
        job_state=job_state, state_file=state_file, log_text=log_text,
    )

    assert converted == 1
    assert not (tmp_path / "job_0.timeout").exists()
    assert not (tmp_path / "job_0.running").exists()
    assert (tmp_path / "job_0.failed").exists()
    persisted = json.loads(state_file.read_text())
    assert persisted["0"]["queue"] == "microcentury"
    assert persisted["0"]["request_cpus"] == 6
    assert persisted["0"]["request_memory"] == "12GB"
    assert "time limit" in log_text[-1]


def test_bump_jobqueue_scales_resources_only_once(tmp_path):
    from pocket_coffea.scripts.check_jobs import bump_jobqueue

    state_file = tmp_path / "job_state.json"
    job_state = {
        "0": {
            "queue": "espresso",
            "base_cpus": 2,
            "base_memory": "7.5GB",
            "request_cpus": 2,
            "request_memory": "7.5GB",
            "resources_scaled": False,
        },
        "1": {
            "queue": "espresso",
            "base_cpus": 1,
            "base_memory": "4GB",
            "request_cpus": 1,
            "request_memory": "4GB",
            "resources_scaled": False,
        },
    }

    bump_jobqueue("0", job_state, state_file, shift=1, ncpu=2)
    bump_jobqueue("0", job_state, state_file, shift=1, ncpu=4)

    assert job_state["0"]["queue"] == "longlunch"
    assert job_state["0"]["request_cpus"] == 4
    assert job_state["0"]["request_memory"] == "15GB"
    assert job_state["1"]["queue"] == "espresso"
    assert job_state["1"]["request_memory"] == "4GB"


def test_ordinary_refailure_shifts_only_after_first_resubmission():
    from pocket_coffea.scripts.check_jobs import should_shift_for_refailure

    job_state = {"0": {"resubmissions": 0}}
    assert not should_shift_for_refailure("0", job_state, "job_0", set())

    job_state["0"]["resubmissions"] = 1
    assert should_shift_for_refailure("0", job_state, "job_0", set())
    assert not should_shift_for_refailure("0", job_state, "job_0", {"job_0"})
    assert not should_shift_for_refailure("999", job_state, "job_999", set())


def test_xrootd_exhaustion_log_is_detected():
    from pocket_coffea.scripts.check_jobs import is_xrootd_exhaustion_log

    assert is_xrootd_exhaustion_log(
        "XRootD failure found at root://xrootd-cms.infn.it, but no config URLs changed."
    )
    assert is_xrootd_exhaustion_log(
        "Reached the maximum number of XRootD recovery attempts (10)."
    )
    assert not is_xrootd_exhaustion_log("Exception: VOMS proxy expirend or non-existing")


def test_submit_resubmit_jobs_builds_one_dynamic_batch(tmp_path, monkeypatch):
    from pocket_coffea.scripts.check_jobs import submit_resubmit_jobs

    (tmp_path / "resubmit.sub").write_text("Executable = job.sh\n")
    state_file = tmp_path / "job_state.json"
    job_state = {
        "0": {
            "queue": "workday", "chunksize": 100, "request_cpus": 4,
            "request_memory": "8GB", "resubmissions": 0,
        },
        "2": {
            "queue": "tomorrow", "chunksize": 250, "request_cpus": 8,
            "request_memory": "16GB", "resubmissions": 1,
        },
    }
    state_file.write_text(json.dumps(job_state))

    class SubmitOutput:
        def read(self):
            return "2 job(s) submitted to cluster 123.\n"

    commands = []
    monkeypatch.setattr(os, "popen", lambda command, mode: commands.append(command) or SubmitOutput())
    monkeypatch.setattr(os, "system", lambda command: 0)

    assert submit_resubmit_jobs(
        tmp_path, ["0", "2", "0"], job_state, state_file, []
    )

    resubmit_now = (tmp_path / "resubmit_now.sub").read_text()
    assert "queue PROC, QUEUE, CHUNKSIZE, CPUS, MEMORY from (" in resubmit_now
    assert "0 workday 100 4 8GB" in resubmit_now
    assert "2 tomorrow 250 8 16GB" in resubmit_now
    assert len(commands) == 1
    assert job_state["0"]["resubmissions"] == 1
    assert job_state["2"]["resubmissions"] == 2


def test_ncpu_option_defaults_to_one_and_rejects_zero():
    from pocket_coffea.scripts.check_jobs import check_jobs

    ncpu = next(param for param in check_jobs.params if param.name == "ncpu")
    assert ncpu.default == 1
    result = CliRunner().invoke(check_jobs, ["--jobs-folder", "/unused", "--ncpu", "0"])
    assert result.exit_code != 0
    assert "not in the range" in result.output


def test_check_jobs_checks_proxy_only_when_resubmitting(tmp_path, monkeypatch):
    from pocket_coffea.scripts import check_jobs as check_jobs_module

    (tmp_path / "job_state.json").write_text("{}")
    called = []

    monkeypatch.setattr(check_jobs_module, "get_proxy_path", lambda: called.append(True) or "/tmp/x509")
    monkeypatch.setenv("HOME", str(tmp_path))
    copied = []
    monkeypatch.setattr(check_jobs_module.os, "system", lambda cmd: copied.append(cmd) or 0)

    result = CliRunner().invoke(check_jobs_module.check_jobs, ["--jobs-folder", str(tmp_path)])
    assert result.exit_code == 0
    assert called == []
    assert copied == []

    result = CliRunner().invoke(
        check_jobs_module.check_jobs,
        ["--jobs-folder", str(tmp_path), "--resubmit"],
    )
    assert result.exit_code == 0
    assert called == [True]
    assert any("scp /tmp/x509" in cmd for cmd in copied)


def test_latest_job_out_uses_out_logs(tmp_path):
    from pocket_coffea.scripts.check_jobs import latest_job_out

    logs = tmp_path / "logs"
    logs.mkdir()
    old = logs / "job_10.0.out"
    new = logs / "job_11.0.out"
    old.write_text("old")
    new.write_text("new")
    os.utime(old, (1, 1))
    os.utime(new, (2, 2))

    assert latest_job_out(tmp_path, "job_0") == str(new)


def test_recreate_queue_synchronizes_dynamic_state(tmp_path):
    from pocket_coffea.scripts.check_jobs import sync_dynamic_queue

    state_file = tmp_path / "job_state.json"
    state_file.write_text(json.dumps({"0": {"queue": "espresso"}}))

    assert sync_dynamic_queue(tmp_path, "job_0", "workday")
    assert json.loads(state_file.read_text())["0"]["queue"] == "workday"


def test_recreate_queue_sync_is_legacy_safe(tmp_path):
    from pocket_coffea.scripts.check_jobs import sync_dynamic_queue

    assert not sync_dynamic_queue(tmp_path, "job_0", "workday")
