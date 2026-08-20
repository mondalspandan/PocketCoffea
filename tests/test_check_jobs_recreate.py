"""Focused current-format proactive recreation tests."""
import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from pocket_coffea.scripts import check_jobs
from tests.test_check_jobs_progress import make_jobs


class Config:
    def __init__(self):
        self.filesets = {}

    def set_filesets_manually(self, filesets):
        self.filesets = filesets


def test_recreate_requires_current_contract(tmp_path):
    (tmp_path / "jobs_config.yaml").write_text("{}")
    result = CliRunner().invoke(
        check_jobs.check_jobs, ["-j", str(tmp_path), "--recreate", "0"])
    assert result.exit_code != 0
    assert "predates the consolidated" in result.output


def test_recreate_prepares_before_condor_rm(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    (tmp_path / "job_0.running").touch()
    config_path = tmp_path / "config_job_0.pkl"
    import cloudpickle
    with config_path.open("wb") as handle:
        cloudpickle.dump(Config(), handle)
    jobs_config = yaml.safe_load((tmp_path / "jobs_config.yaml").read_text())
    jobs_config["jobs_list"]["job_0"]["filesets"] = {
        "dataset": {"files": ["file.root"], "metadata": {"nevents": 1}}
    }
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump(jobs_config))
    order = []
    monkeypatch.setattr(check_jobs, "prepare_proxy_for_jobs",
                        lambda folder: order.append("proxy"))
    monkeypatch.setattr(check_jobs, "condor_rm_job",
                        lambda job: (order.append("rm") or (True, "")))
    monkeypatch.setattr(check_jobs, "wait_for_condor_job_removal",
                        lambda job: (order.append("wait") or True))
    monkeypatch.setattr(check_jobs, "condor_submit_job",
                        lambda folder, sub: (order.append("submit") or (True, "")))
    result = check_jobs.recreate_jobs_oneshot(
        tmp_path, "0", remove_running=True)
    assert result["submitted"] == ["job_0"]
    assert order == ["proxy", "rm", "wait", "submit"]
    assert json.loads((tmp_path / "job_state.json").read_text())["0"]["resubmissions"] == 1


def test_recreate_proxy_failure_leaves_active_job(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    monkeypatch.setattr(check_jobs, "prepare_proxy_for_jobs",
                        lambda folder: (_ for _ in ()).throw(RuntimeError("proxy")))
    removed = []
    monkeypatch.setattr(check_jobs, "condor_rm_job", lambda job: removed.append(job))
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "0", remove_running=True)
    assert "job_0" in result["failed"]
    assert removed == []
    assert (tmp_path / "job_0.running").exists()


def test_plain_recreate_does_not_initialize_rucio(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    (tmp_path / "job_0.running").unlink()
    import cloudpickle
    with (tmp_path / "config_job_0.pkl").open("wb") as handle:
        cloudpickle.dump(Config(), handle)
    monkeypatch.setattr(check_jobs, "get_xrootd_sites_map",
                        lambda: (_ for _ in ()).throw(AssertionError("site map")))
    monkeypatch.setattr(check_jobs, "get_rucio_client",
                        lambda: (_ for _ in ()).throw(AssertionError("rucio")))
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (True, ""))
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "0")
    assert result["submitted"] == ["job_0"]


def test_rucio_client_is_created_after_proxy_refresh(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    (tmp_path / "job_0.running").unlink()
    import cloudpickle
    with (tmp_path / "config_job_0.pkl").open("wb") as handle:
        cloudpickle.dump(Config(), handle)
    jobs_config = yaml.safe_load((tmp_path / "jobs_config.yaml").read_text())
    jobs_config["submission"]["requires_grid_certificate"] = True
    jobs_config["submission"]["proxy_source"] = "explicit"
    jobs_config["submission"]["proxy_transfer_path"] = str(tmp_path / "proxy")
    jobs_config["jobs_list"]["job_0"]["filesets"] = {"dataset": {"files": []}}
    (tmp_path / "jobs_config.yaml").write_text(yaml.safe_dump(jobs_config))
    order = []
    monkeypatch.setattr(check_jobs, "prepare_proxy_for_jobs", lambda folder: order.append("proxy"))
    monkeypatch.setattr(check_jobs, "get_xrootd_sites_map", lambda: order.append("map") or {})
    monkeypatch.setattr(check_jobs, "get_rucio_client", lambda: order.append("rucio") or object())
    monkeypatch.setattr(check_jobs, "rewrite_fileset_blocklist", lambda *args, **kwargs: {})
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (True, ""))
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "0", blocklist_sites=["T1_US_FNAL"])
    assert result["submitted"] == ["job_0"]
    assert order[:3] == ["proxy", "map", "rucio"]


def test_failed_recreate_restores_canonical_config(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    (tmp_path / "job_0.running").unlink()
    import cloudpickle
    config = Config()
    config.filesets = {"original": {"files": ["old.root"]}}
    with (tmp_path / "config_job_0.pkl").open("wb") as handle:
        cloudpickle.dump(config, handle)
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (False, "submit failed"))
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "0")
    assert "job_0" in result["failed"]
    with (tmp_path / "config_job_0.pkl").open("rb") as handle:
        assert cloudpickle.load(handle).filesets == {"original": {"files": ["old.root"]}}


def test_successful_recreate_installs_candidate_config(tmp_path, monkeypatch):
    make_jobs(tmp_path)
    (tmp_path / "job_0.running").unlink()
    import cloudpickle
    with (tmp_path / "config_job_0.pkl").open("wb") as handle:
        cloudpickle.dump(Config(), handle)
    monkeypatch.setattr(check_jobs, "condor_submit_job", lambda *args: (True, ""))
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "0")
    assert result["submitted"] == ["job_0"]
    with (tmp_path / "config_job_0.pkl").open("rb") as handle:
        assert cloudpickle.load(handle).filesets == {}


def test_explicit_active_recreate_refusal_is_failure(tmp_path):
    make_jobs(tmp_path)
    result = check_jobs.recreate_jobs_oneshot(tmp_path, "0")
    assert "job_0" in result["failed"]
