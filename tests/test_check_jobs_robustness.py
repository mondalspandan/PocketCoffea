"""Pure current queue semantics; no submit-file compatibility tests."""
from pocket_coffea.utils.htcondor_queue import next_queue


def test_negative_queue_shift_is_clamped():
    assert next_queue("espresso", -1) == "espresso"


def test_large_queue_shift_caps_at_nextweek():
    assert next_queue("espresso", 100) == "nextweek"


def test_unknown_queue_uses_terminal_queue():
    assert next_queue("site-specific", 1) == "nextweek"
