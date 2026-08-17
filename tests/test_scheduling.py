"""Unit tests for the scheduling decision logic (no Airflow required).

Run directly:  python tests/test_scheduling.py
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from pipeline.schedule_utils import compute_trigger_slots, parse_report_times
from common.slots import build_trigger_run_id, find_due_slots

GROUP = {"name": "test", "sheet_tab": "Sheet1", "env": {}}


def row(**overrides):
    base = {"active": "TRUE", "tenant": "ba", "state_name": "Bauchi",
            "campaign_start": "2026-08-10", "campaign_end": "2026-08-20",
            "report_times": "05:30,11:30", "partner_report_times": ""}
    base.update(overrides)
    return base


def utc(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def test_parse_report_times_skips_invalid():
    assert parse_report_times("05:30, 4:50,bad,25:00,11:70,") == ["05:30", "04:50"]
    assert parse_report_times(None) == []


def test_slots_default_to_both_mode():
    assert compute_trigger_slots(row()) == [("05:30", "both"), ("11:30", "both")]


def test_slots_split_internal_and_partner():
    r = row(report_times="05:30,11:30", partner_report_times="11:30,12:00")
    assert compute_trigger_slots(r) == [
        ("05:30", "internal"), ("11:30", "both"), ("12:00", "partner")]


def test_slot_fires_inside_lookback_window():
    due = find_due_slots(GROUP, [row()], utc(2026, 8, 14, 5, 35), 60)
    assert len(due) == 1
    assert due[0]["conf"]["slot_time"] == "05:30"
    assert due[0]["trigger_run_id"] == "dst_test_ba_both_2026-08-14_0530"


def test_slot_outside_window_does_not_fire():
    assert find_due_slots(GROUP, [row()], utc(2026, 8, 14, 7, 0), 60) == []


def test_inactive_row_never_fires():
    assert find_due_slots(GROUP, [row(active="FALSE")], utc(2026, 8, 14, 5, 35), 60) == []


def test_outside_campaign_dates_never_fires():
    assert find_due_slots(GROUP, [row(campaign_end="2026-08-13")],
                          utc(2026, 8, 14, 5, 35), 60) == []


def test_window_crossing_midnight_catches_yesterdays_slot():
    r = row(report_times="23:50")
    due = find_due_slots(GROUP, [r], utc(2026, 8, 15, 0, 10), 60)
    assert len(due) == 1
    assert due[0]["conf"]["slot_date"] == "2026-08-14"


def test_retime_guard_blocks_past_slot_already_reported():
    r = row(report_times="04:50")
    already_ran = lambda tenant, mode, slot_dt: True   # a 05:30 run exists
    assert find_due_slots(GROUP, [r], utc(2026, 8, 14, 5, 40), 60,
                          has_report_since=already_ran) == []


def test_retime_guard_allows_slot_when_no_report_since():
    r = row(report_times="04:50")
    nothing_ran = lambda tenant, mode, slot_dt: False
    assert len(find_due_slots(GROUP, [r], utc(2026, 8, 14, 5, 40), 60,
                              has_report_since=nothing_ran)) == 1


def test_run_id_is_deterministic_per_slot():
    a = build_trigger_run_id("g", "ba", "both", "2026-08-14", "05:30")
    b = build_trigger_run_id("g", "ba", "both", "2026-08-14", "05:30")
    c = build_trigger_run_id("g", "ba", "both", "2026-08-14", "04:50")
    assert a == b and a != c


def test_data_errors_fail_fast_without_retry():
    from common.campaign_runner import AirflowFailException, _run_stage
    marker = {"stages": {}}

    def bad_data():
        raise KeyError("Data.age")

    try:
        _run_stage("analyze", bad_data, marker)
        raise AssertionError("expected AirflowFailException")
    except AirflowFailException:
        pass
    assert marker["stages"]["analyze"].startswith("failed")


def test_infrastructure_errors_propagate_for_retry():
    from common.campaign_runner import AirflowFailException, _run_stage

    def network_down():
        raise ConnectionError("ES unreachable")

    try:
        _run_stage("analyze", network_down, {"stages": {}})
        raise AssertionError("expected ConnectionError")
    except AirflowFailException:
        raise AssertionError("infra error must NOT fail fast")
    except ConnectionError:
        pass


if __name__ == "__main__":
    tests = [fn for name, fn in sorted(globals().items()) if name.startswith("test_")]
    for fn in tests:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"\n{len(tests)} tests passed")
