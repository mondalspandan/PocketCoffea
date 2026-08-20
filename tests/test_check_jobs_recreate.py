"""Offline tests for the one-shot `check-jobs --recreate` pass.

These exercise `pocket_coffea.scripts.check_jobs.recreate_jobs_oneshot` against a
synthetic jobs_dir (jobs_config.yaml + config_job_i.pkl + job_i.sub + flag files),
with all network / condor side effects stubbed:

- `get_xrootd_sites_map` and `get_rucio_client` are monkeypatched,
- `site_rewrite._query_replicas` returns a controllable LFN -> [sites] table,
- `dry_run=True` skips the real `condor_submit` (one test uses a fake os.system
  to check the submit/flag-flip commands instead).

Importing check_jobs pulls in pocket_coffea.utils.rucio (imports the rucio
package); stub it when unavailable so this stays offline.
"""
import sys
import types
import json

import pytest
import yaml
import cloudpickle


def _ensure_rucio_importable():
    try:
        import rucio.client  # noqa: F401
        import rucio.common.client  # noqa: F401
        return
    except (ImportError, OSError):
        rucio = types.ModuleType("rucio")
        client = types.ModuleType("rucio.client")
        client.Client = object
        common = types.ModuleType("rucio.common")
        common_client = types.ModuleType("rucio.common.client")
        common_client.detect_client_location = lambda *a, **k: {}
        rucio.client = client
        rucio.common = common
        common.client = common_client
        sys.modules.setdefault("rucio", rucio)
        sys.modules.setdefault("rucio.client", client)
        sys.modules.setdefault("rucio.common", common)
        sys.modules.setdefault("rucio.common.client", common_client)


_ensure_rucio_importable()

from pocket_coffea.scripts import check_jobs  # noqa: E402
from pocket_coffea.utils import site_rewrite  # noqa: E402


SITEA = "root://siteA.example//"
SITEC = "root://siteC.example//"
SITEMAP = {"T2_X_SITEA": SITEA, "T2_X_SITEC": SITEC}
REDIR = site_rewrite.GLOBAL_XROOTD_REDIRECTOR


class FakeConfigurator:
    """Minimal stand-in for pocket_coffea Configurator (no coffea needed)."""

    def __init__(self, filesets):
        self.filesets = filesets

    def set_filesets_manually(self, filesets):
        self.filesets = filesets


def _fs(files):
    return {"datasetA": {"files": list(files), "metadata": {"sample": "A", "nevents": "1"}}}


PLACEHOLDER_FS = {"__placeholder__": {"files": [], "metadata": {}}}


def _make_jobs_dir(tmp_path, jobs):
    """Build a synthetic jobs_dir.

    `jobs` maps job_name -> {"filesets": <fileset>, "flag": failed|running|idle|None,
    "flavour": <+JobFlavour>}. The per-job pickle is seeded with a *placeholder*
    fileset (not the real one) so a passing assertion on the reloaded pickle proves
    the recreate pass sourced the fileset from jobs_config.yaml and persisted it.
    """
    d = tmp_path / "job"
    d.mkdir()
    (d / "logs").mkdir()
    jobs_list = {}
    for jn, spec in jobs.items():
        cfg = d / f"config_{jn}.pkl"
        with open(cfg, "wb") as f:
            cloudpickle.dump(FakeConfigurator(dict(PLACEHOLDER_FS)), f)
        jobs_list[jn] = {
            "filesets": spec["filesets"],
            "config_file": str(cfg),
            "output_file": str(d / f"output_{jn}.coffea"),
        }
        flavour = spec.get("flavour", "espresso")
        (d / f"{jn}.sub").write_text(
            "executable = job.sh\n"
            f'+JobFlavour="{flavour}"\n'
            "RequestCpus = 2\n"
            "RequestMemory = 4GB\n"
            f"arguments = 0 config_{jn}.pkl 100 2\n"
            f"transfer_input_files = {cfg}\n"
            "queue\n"
        )
        flag = spec.get("flag")
        if flag:
            (d / f"{jn}.{flag}").write_text("")
    (d / "jobs_config.yaml").write_text(yaml.safe_dump({
        "job_name": "job",
        "job_dir": str(d),
        "jobs_list": jobs_list,
    }))
    # job.sh with the `--chunksize $3` anchor the skip-bad-files patcher looks for
    (d / "job.sh").write_text(
        "pocket-coffea run --cfg $2 -o output --executor iterative --chunksize $3\n"
    )
    return d


def _load_files(cfg_path):
    with open(cfg_path, "rb") as f:
        return cloudpickle.load(f).filesets["datasetA"]["files"]


@pytest.fixture
def replicas(monkeypatch):
    """Stub the sitemap / rucio client / replica lookup. Returns the mutable
    LFN -> [site names] table used by find_other_file."""
    monkeypatch.setattr(check_jobs, "get_xrootd_sites_map", lambda: SITEMAP)
    monkeypatch.setattr(check_jobs, "get_rucio_client", lambda: None)
    table = {}

    def fake_query(lfn, client=None, scope="cms"):
        return list(table.get(lfn, []))

    monkeypatch.setattr(site_rewrite, "_query_replicas", fake_query)
    return table


def test_recreate_redirector_rewrites_all_files(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [REDIR + "store/data/foo.root"]


def test_recreate_queue_forced(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed", "flavour": "espresso"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     recreate_queue="longlunch", dry_run=True)
    assert '+JobFlavour="longlunch"' in (d / "job_0.sub").read_text()


def test_recreate_queue_accepts_custom_value(tmp_path, replicas, capsys):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(
        d, "0", use_redirector=True, recreate_queue="customqueue", dry_run=True
    )
    assert '+JobFlavour="customqueue"' in (d / "job_0.sub").read_text()
    assert "not in the known" in capsys.readouterr().out


def test_queue_only_recreate_restores_original_fileset(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=True)
    assert _load_files(d / "config_job_0.pkl")[0].startswith(REDIR)

    check_jobs.recreate_jobs_oneshot(d, "0", recreate_queue="workday", dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [f]


def test_recreate_running_job_keeps_queue_without_timeout(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "running", "flavour": "espresso"}})
    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True,
                                     remove_running=True, dry_run=True)
    assert '+JobFlavour="espresso"' in (d / "job_0.sub").read_text()


def test_recreate_timeout_scales_resources_and_queue(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "timeout"}})
    state = {"0": {"queue": "espresso", "chunksize": 100, "base_cpus": 2,
                     "base_memory": "4GB", "request_cpus": 2,
                     "request_memory": "4GB", "resources_scaled": False,
                     "resubmissions": 0}}
    (d / "job_state.json").write_text(json.dumps(state))
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, ncpu=3, dry_run=True)
    persisted = json.loads((d / "job_state.json").read_text())
    assert persisted["0"]["queue"] == "espresso"
    assert persisted["0"]["request_cpus"] == 2
    assert persisted["0"]["request_memory"] == "4GB"
    sub = (d / "job_0.sub").read_text()
    assert "RequestCpus = 6" in sub
    assert "arguments = 0 config_job_0.pkl 100 6" in sub


def test_timeout_explicit_queue_override_keeps_resource_escalation(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "timeout"}})
    state = {"0": {"queue": "espresso", "chunksize": 100, "base_cpus": 2,
                     "base_memory": "4GB", "request_cpus": 2,
                     "request_memory": "4GB", "resources_scaled": False,
                     "resubmissions": 0}}
    (d / "job_state.json").write_text(json.dumps(state))
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, ncpu=3,
                                     recreate_queue="longlunch", dry_run=True)
    assert '+JobFlavour="longlunch"' in (d / "job_0.sub").read_text()
    assert "RequestCpus = 6" in (d / "job_0.sub").read_text()


def test_recreate_blocklist_rewrites_to_alt_site(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    replicas["/store/data/foo.root"] = ["T2_X_SITEA", "T2_X_SITEC"]
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", blocklist_sites={"T2_X_SITEA"}, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [SITEC + "/store/data/foo.root"]


def test_recreate_reads_xrootd_failure_from_out(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    replicas["/store/data/foo.root"] = ["T2_X_SITEA", "T2_X_SITEC"]
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    (d / "logs" / "job_123.0.out").write_text(
        f"OSError: XRootD error\nfile: {f}\n"
    )

    check_jobs.recreate_jobs_oneshot(d, "0", dry_run=True)

    assert _load_files(d / "config_job_0.pkl") == [SITEC + "/store/data/foo.root"]


def test_recreate_redirector_precedence_over_blocklist(tmp_path, replicas):
    # both set -> redirector wins, no Rucio lookup happens (empty replica table).
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     blocklist_sites={"T2_X_SITEA"}, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [REDIR + "store/data/foo.root"]


def test_recreate_selector_only_touches_selected_jobs(tmp_path, replicas):
    fA = SITEA + "/store/data/a.root"
    fB = SITEA + "/store/data/b.root"
    d = _make_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([fA]), "flag": "failed"},
        "job_1": {"filesets": _fs([fB]), "flag": "failed"},
    })
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [REDIR + "store/data/a.root"]
    # job_1 was not selected -> its pickle keeps the seeded placeholder fileset
    with open(d / "config_job_1.pkl", "rb") as fh:
        assert cloudpickle.load(fh).filesets == PLACEHOLDER_FS


def test_recreate_selector_accepts_job_prefix_and_bare_ids(tmp_path, replicas):
    fA = SITEA + "/store/data/a.root"
    fB = SITEA + "/store/data/b.root"
    d = _make_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([fA]), "flag": "failed"},
        "job_1": {"filesets": _fs([fB]), "flag": "failed"},
    })
    check_jobs.recreate_jobs_oneshot(d, "0,job_1", use_redirector=True, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [REDIR + "store/data/a.root"]
    assert _load_files(d / "config_job_1.pkl") == [REDIR + "store/data/b.root"]


def test_recreate_skip_bad_files_materializes_inner_yaml(tmp_path, replicas):
    pytest.importorskip("coffea")  # write_inner_run_options lives next to the executor
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     skip_bad_files=True, dry_run=True)
    inner = d / "inner_run_options.yaml"
    assert inner.exists()
    assert yaml.safe_load(inner.read_text()).get("skip-bad-files") is True
    assert "inner_run_options.yaml" in (d / "job.sh").read_text()
    assert "inner_run_options.yaml" in (d / "job_0.sub").read_text()


def test_recreate_dry_run_false_submits_and_flips_flags(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda folder, submit: (True, "1 job submitted"))
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=False)
    assert (d / "job_0.idle").exists()
    assert not (d / "job_0.failed").exists()


def test_recreate_missing_jobs_config_is_graceful(tmp_path, replicas):
    d = tmp_path / "job"
    d.mkdir()
    # no jobs_config.yaml -> should return without raising
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=True)


def test_condor_rm_job_constraint(monkeypatch):
    captured = {}

    class _R:
        returncode = 0
        stdout = "1 job removed\n"

    def fake_run(args, **kwargs):
        captured["args"] = args
        return _R()

    monkeypatch.setattr(check_jobs.subprocess, "run", fake_run)
    ok, out = check_jobs.condor_rm_job("job_7")
    assert ok
    assert captured["args"][:2] == ["condor_rm", "-constraint"]
    assert captured["args"][2] == check_jobs.condor_job_constraint("job_7")
    assert 'regexp("config_job_7\\.pkl", Args)' == captured["args"][2]
    assert out == "1 job removed"


def test_recreate_remove_running_kills_queued_jobs(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([f]), "flag": "running"},
        "job_1": {"filesets": _fs([f]), "flag": "failed"},
        "job_2": {"filesets": _fs([f]), "flag": "idle"},
    })
    removed = []
    monkeypatch.setattr(check_jobs, "condor_rm_job",
                        lambda job: removed.append(job) or (True, "1 job removed"))
    monkeypatch.setattr(check_jobs, "wait_for_condor_job_removal", lambda job: True)
    monkeypatch.setattr(check_jobs, "condor_submit_job",
                        lambda folder, submit: (True, "1 job submitted"))
    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True,
                                     remove_running=True, dry_run=False)
    # only the running + idle (queued) jobs are condor_rm'd; the failed one is not
    assert sorted(removed) == ["job_0", "job_2"]


def test_recreate_remove_running_dry_run_skips_condor_rm(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "running"}})
    removed = []
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: removed.append(job) or "")
    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True,
                                     remove_running=True, dry_run=True)
    assert removed == []


def test_recreate_remove_running_off_by_default(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "running"}})
    removed = []
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: removed.append(job) or "")
    monkeypatch.setattr(check_jobs.os, "system", lambda cmd: 0)
    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True, dry_run=False)
    assert removed == []


def test_failed_condor_rm_keeps_active_job_and_skips_submit(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "running"}})
    submitted = []
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: (False, "permission denied"))
    monkeypatch.setattr(check_jobs, "condor_submit_job",
                        lambda *args: submitted.append(args) or (True, "unexpected"))

    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True,
                                     remove_running=True, dry_run=False)

    assert submitted == []
    assert (d / "job_0.running").exists()
    assert not (d / "job_0.idle").exists()


def test_active_recreate_proxy_failure_does_not_remove_job(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "running"}})
    removed = []
    monkeypatch.setattr(check_jobs, "prepare_proxy_for_jobs",
                        lambda folder: (_ for _ in ()).throw(RuntimeError("proxy expired")))
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: removed.append(job))

    result = check_jobs.recreate_jobs_oneshot(
        d, "0", use_redirector=True, remove_running=True, dry_run=False
    )

    assert removed == []
    assert "job_0" in result["failed"]
    assert (d / "job_0.running").exists()


def test_active_recreate_rewrite_failure_does_not_remove_job(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "running"}})
    removed = []
    monkeypatch.setattr(check_jobs, "rewrite_fileset_to_redirector",
                        lambda fileset: (_ for _ in ()).throw(RuntimeError("rewrite failed")))
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: removed.append(job))

    result = check_jobs.recreate_jobs_oneshot(
        d, "0", use_redirector=True, remove_running=True, dry_run=False
    )

    assert removed == []
    assert "rewrite failed" in result["failed"]["job_0"]
    assert (d / "job_0.running").exists()


def test_recreate_clears_all_markers_and_creates_one_idle(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    for marker in ("idle", "running", "done", "failed", "timeout"):
        (d / f"job_0.{marker}").write_text("")
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: (True, "removed"))
    monkeypatch.setattr(check_jobs, "wait_for_condor_job_removal", lambda job: True)
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda folder, submit: (True, "submitted"))

    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     remove_running=True, dry_run=False)

    assert [p.name for p in d.glob("job_0.*") if p.suffix[1:] in check_jobs.JOB_MARKERS] == ["job_0.idle"]


def test_recreate_auto_includes_timeout_jobs(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "timeout"}})
    submitted = []
    monkeypatch.setattr(check_jobs, "condor_submit_job",
                        lambda folder, submit: submitted.append(submit) or (True, "submitted"))

    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True, dry_run=False)

    assert submitted == ["job_0.sub"]
    assert (d / "job_0.idle").exists()


def test_recreate_waits_for_condor_removal_before_submit(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_1": {"filesets": _fs([f]), "flag": "running"}})
    events = []
    monkeypatch.setattr(check_jobs, "condor_rm_job",
                        lambda job: events.append(("rm", job)) or (True, "removed"))
    states = iter(["job_10\njob_1\n", "job_1\n", ""])
    def fake_run(args, **kwargs):
        if args[0] == "condor_q":
            events.append(("q", args[2]))
            return type("Result", (), {"returncode": 0, "stdout": next(states)})()
        events.append(("submit", args[1]))
        return type("Result", (), {"returncode": 0, "stdout": "submitted"})()
    monkeypatch.setattr(check_jobs.subprocess, "run", fake_run)
    check_jobs.recreate_jobs_oneshot(d, "1", use_redirector=True,
                                     remove_running=True, dry_run=False)
    assert [event[0] for event in events] == ["rm", "q", "q", "q", "submit"]
    assert events[1][1] == check_jobs.condor_job_constraint("job_1")
    assert (d / "job_1.idle").exists()


def test_failed_removal_and_timeout_leave_old_markers_without_submit(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "running"}})
    submitted = []
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: (True, "removed"))
    monkeypatch.setattr(check_jobs, "wait_for_condor_job_removal", lambda job: False)
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: submitted.append(args))
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     remove_running=True, dry_run=False)
    assert submitted == []
    assert (d / "job_0.running").exists()


def test_failed_replacement_has_only_failed_marker(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (False, "bad submit"))
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=False)
    assert [p.name for p in d.glob("job_0.*") if p.suffix[1:] in check_jobs.JOB_MARKERS] == ["job_0.failed"]


def test_multi_job_recreation_keeps_one_shared_job_state(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([f]), "flag": "failed"},
        "job_1": {"filesets": _fs([f]), "flag": "timeout"},
    })
    state = {
        "0": {"queue": "espresso", "chunksize": 100, "base_cpus": 2,
              "base_memory": "4GB", "request_cpus": 2, "request_memory": "4GB",
              "resources_scaled": False, "resubmissions": 0},
        "1": {"queue": "espresso", "chunksize": 100, "base_cpus": 2,
              "base_memory": "4GB", "request_cpus": 2, "request_memory": "4GB",
              "resources_scaled": False, "resubmissions": 0},
    }
    (d / "job_state.json").write_text(json.dumps(state))
    check_jobs.recreate_jobs_oneshot(
        d, "0,1", use_redirector=True, recreate_queue="workday",
        ncpu=3, dry_run=True,
    )
    persisted = json.loads((d / "job_state.json").read_text())
    assert persisted["0"]["queue"] == persisted["1"]["queue"] == "espresso"
    assert persisted["1"]["request_cpus"] == 2
    assert persisted["1"]["request_memory"] == "4GB"
    for job in ("job_0", "job_1"):
        sub = (d / f"{job}.sub").read_text()
        assert '+JobFlavour="workday"' in sub
    assert "RequestCpus = 6" in (d / "job_1.sub").read_text()


def test_legacy_timeout_resources_scale_only_once(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "timeout"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, ncpu=2, dry_run=True)
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, ncpu=2, dry_run=True)
    sub = (d / "job_0.sub").read_text()
    assert "RequestCpus = 4" in sub
    assert "RequestMemory = 8GB" in sub
    assert "arguments = 0 config_job_0.pkl 100 4" in sub
    assert sub.count(check_jobs.RESOURCE_SCALED_MARKER) == 1
    assert '+JobFlavour="longlunch"' in sub


def test_legacy_timeout_ncpu_one_records_scaling_decision(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "timeout"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, ncpu=1, dry_run=True)
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, ncpu=3, dry_run=True)
    sub = (d / "job_0.sub").read_text()
    assert "RequestCpus = 2" in sub
    assert "RequestMemory = 4GB" in sub
    assert "arguments = 0 config_job_0.pkl 100 2" in sub
    assert sub.count(check_jobs.RESOURCE_SCALED_MARKER) == 1


def _write_submission_contract(directory, **submission):
    (directory / "jobs_config.yaml").write_text(yaml.safe_dump({"submission": submission}))


def test_no_grid_certificate_recreation_skips_proxy_discovery(tmp_path, monkeypatch):
    _write_submission_contract(
        tmp_path,
        requires_grid_certificate=False,
        proxy_transfer_path=None,
        proxy_source=None,
    )
    monkeypatch.setattr(check_jobs, "get_proxy_path", lambda: pytest.fail("proxy lookup"))
    assert check_jobs.prepare_proxy_for_jobs(tmp_path) is None


def test_default_proxy_contract_refreshes_recorded_path(tmp_path, monkeypatch):
    source = tmp_path / "current.proxy"
    target = tmp_path / "transfer.proxy"
    source.write_text("fresh")
    _write_submission_contract(
        tmp_path,
        requires_grid_certificate=True,
        proxy_transfer_path=str(target),
        proxy_source="default",
    )
    monkeypatch.setattr(check_jobs, "get_proxy_path", lambda: str(source))
    assert check_jobs.prepare_proxy_for_jobs(tmp_path) == str(target)
    assert target.read_text() == "fresh"


def test_explicit_proxy_contract_does_not_refresh_default(tmp_path, monkeypatch):
    target = tmp_path / "custom.proxy"
    target.write_text("custom")
    _write_submission_contract(
        tmp_path,
        requires_grid_certificate=True,
        proxy_transfer_path=str(target),
        proxy_source="explicit",
    )
    monkeypatch.setattr(check_jobs, "get_proxy_path", lambda: pytest.fail("default lookup"))
    assert check_jobs.prepare_proxy_for_jobs(tmp_path) == str(target)


def test_missing_required_proxy_fails_before_submission(tmp_path, monkeypatch):
    target = tmp_path / "missing.proxy"
    _write_submission_contract(
        tmp_path,
        requires_grid_certificate=True,
        proxy_transfer_path=str(target),
        proxy_source="explicit",
    )
    with pytest.raises(RuntimeError, match="does not exist"):
        check_jobs.prepare_proxy_for_jobs(tmp_path)


def test_default_proxy_refresh_is_private(tmp_path, monkeypatch):
    source = tmp_path / "current.proxy"
    target = tmp_path / "transfer.proxy"
    source.write_bytes(b"fresh")
    _write_submission_contract(tmp_path, requires_grid_certificate=True,
                               proxy_transfer_path=str(target), proxy_source="default")
    monkeypatch.setattr(check_jobs, "get_proxy_path", lambda: str(source))
    check_jobs.prepare_proxy_for_jobs(tmp_path)
    assert target.read_bytes() == b"fresh"
    assert target.stat().st_mode & 0o777 == 0o600


def test_ambiguous_legacy_proxy_inference_fails(tmp_path):
    (tmp_path / "job_0.sub").write_text(
        "transfer_input_files = config_job_0.pkl,custom_a,custom_b\n"
    )
    with pytest.raises(RuntimeError, match="unambiguously infer"):
        check_jobs.prepare_proxy_for_jobs(tmp_path)
