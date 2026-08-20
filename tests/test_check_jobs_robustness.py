"""Offline tests for check-jobs queue bumping robustness (no condor/EOS needed).

Importing check_jobs pulls in pocket_coffea.utils.rucio (imports the rucio package);
stub it when unavailable so this stays offline.
"""
import sys
import types


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

from pocket_coffea.scripts.check_jobs import QUEUES, bump_jobqueue, set_queue  # noqa: E402
from pocket_coffea.utils.htcondor_queue import next_queue  # noqa: E402


def _write_sub(tmp_path, content):
    p = tmp_path / "job_1.sub"
    p.write_text(content)
    return str(p)


def test_bump_normal_flavour(tmp_path):
    sub = _write_sub(tmp_path, 'executable = job.sh\n+JobFlavour="espresso"\nqueue\n')
    assert bump_jobqueue(sub) == "microcentury"
    assert '+JobFlavour="microcentury"' in open(sub).read()


def test_bump_caps_at_longest_queue(tmp_path):
    sub = _write_sub(tmp_path, '+JobFlavour="nextweek"\n')
    assert bump_jobqueue(sub) == "nextweek"


def test_bump_unknown_flavour_does_not_raise(tmp_path):
    # Previously raised ValueError from queues.index(jf).
    sub = _write_sub(tmp_path, '+JobFlavour="some_rubin_queue"\n')
    assert bump_jobqueue(sub) == QUEUES[-1]


def test_bump_without_jobflavour_returns_none(tmp_path):
    # Previously raised NameError on `return next_jf`.
    sub = _write_sub(tmp_path, "executable = job.sh\n+MaxRuntime = 3600\nqueue\n")
    assert bump_jobqueue(sub) is None


def test_set_queue_forces_flavour(tmp_path):
    sub = _write_sub(tmp_path, 'executable = job.sh\n+JobFlavour="espresso"\nqueue\n')
    assert set_queue(sub, "longlunch") is True
    assert '+JobFlavour="longlunch"' in open(sub).read()


def test_set_queue_noop_when_already_set(tmp_path):
    sub = _write_sub(tmp_path, '+JobFlavour="workday"\n')
    assert set_queue(sub, "workday") is False
    assert '+JobFlavour="workday"' in open(sub).read()


def test_set_queue_without_jobflavour_returns_false(tmp_path):
    sub = _write_sub(tmp_path, "executable = job.sh\n+MaxRuntime = 3600\nqueue\n")
    assert set_queue(sub, "longlunch") is False


def test_negative_queue_shift_is_a_noop():
    assert next_queue("espresso", -1) == "espresso"


def test_large_queue_shift_caps_at_nextweek():
    assert next_queue("espresso", 100) == "nextweek"
