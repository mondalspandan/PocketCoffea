import pytest

from pocket_coffea.utils.configurator import Configurator


def _config(nevents, files):
    config = Configurator.__new__(Configurator)
    config.filesets = {
        "dataset": {"files": list(files), "metadata": {"nevents": str(nevents)}}
    }
    config.datasets = ["dataset"]
    return config


def test_filter_dataset_by_events_uses_ceiling_and_scales_metadata():
    config = _config(100, ["f0", "f1", "f2", "f3", "f4"])

    config.filter_dataset_by_events(21)

    assert config.filesets["dataset"]["files"] == ["f0", "f1"]
    assert config.filesets["dataset"]["metadata"]["nevents"] == "40"


def test_filter_dataset_by_events_keeps_all_files_when_target_is_larger():
    config = _config(100, ["f0", "f1"])

    config.filter_dataset_by_events(101)

    assert config.filesets["dataset"]["files"] == ["f0", "f1"]
    assert config.filesets["dataset"]["metadata"]["nevents"] == "100"


def test_filter_dataset_by_events_warns_and_keeps_invalid_dataset():
    config = _config("unknown", ["f0"])

    with pytest.warns(UserWarning, match="Could not estimate events per file"):
        config.filter_dataset_by_events(10)

    assert config.filesets["dataset"]["files"] == ["f0"]
