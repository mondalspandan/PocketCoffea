"""Current queue semantics."""
from pocket_coffea.utils.htcondor_queue import next_queue


def test_queue_shift_zero_is_a_noop_and_unknown_queues_bump_to_terminal():
    assert next_queue("espresso", 0) == "espresso"
    assert next_queue("site-specific", 0) == "site-specific"
    assert next_queue("site-specific", 1) == "nextweek"
