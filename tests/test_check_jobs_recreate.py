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
