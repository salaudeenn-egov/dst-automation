"""Sheet-backed retime guard for sheet mode — no database anywhere.

There is deliberately NO run lock in this system: duplicate fires of the same
slot are impossible via deterministic trigger run-ids, and the rare overlap of
two different slots for one tenant is accepted (worst case: one duplicate
Slack post), the same trade the platform's production report system makes.

The guard must be built inside group_environment (it reads the group's sheet).
mdms mode skips the guard entirely (zero sheet/DB access on the scheduling
path there).
"""
import logging

from pipeline.run_log import fetch_today_runs

log = logging.getLogger(__name__)


def build_retime_guard():
    """One sheet read, then a pure closure for find_due_slots.

    has_report_since(tenant, mode, slot_dt) -> True when a SUCCESS row for this
    tenant already exists today for a slot at or after this one, with a
    covering mode ("both" covers internal and partner). FAILED runs do not
    count, so a retimed slot may replace a failed report.
    """
    today_runs = fetch_today_runs()
    log.info(f"[retime-guard] {len(today_runs)} run(s) recorded today")

    def has_report_since(tenant, mode, slot_dt):
        """Keyed on TENANT, which is what find_due_slots passes. It previously
        compared against the Run Log's State column while the caller passed the
        lowercase tenant, so the guard never matched anything and a retimed
        slot re-fired. Compares the recorded SLOT time, not the write time."""
        covering = {mode, "both"}
        slot_hhmm = slot_dt.strftime("%H:%M")
        return any(r["tenant"] == tenant and r["status"] == "SUCCESS"
                   and r["mode"] in covering and r["slot_time"] >= slot_hhmm
                   for r in today_runs)

    return has_report_since
