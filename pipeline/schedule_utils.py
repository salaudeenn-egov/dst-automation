"""
schedule_utils.py — pure scheduling-slot computation, shared by scheduler.py
and the Airflow DAG factory so the "both/internal/partner" dedup logic is
never duplicated between the two orchestrators.
"""


def compute_trigger_slots(row):
    """
    Given a Google Sheet row, return the list of (time, mode) trigger slots
    for that campaign, where time is a "HH:MM" 24h UTC string and mode is
    one of "both" / "internal" / "partner".

    A time present in BOTH report_times and partner_report_times becomes a
    single combined "both" trigger (avoids firing the same campaign twice
    at the same clock time, which would race on the per-campaign lock/pool).
    If partner_report_times is empty, report_times drives a "both" trigger
    for every time (backward-compatible: internal + partner post together).
    """
    internal_times = [t.strip() for t in str(row.get("report_times", "")).split(",") if t.strip()]
    partner_times  = [t.strip() for t in str(row.get("partner_report_times", "")).split(",") if t.strip()]

    if not partner_times:
        return [(t, "both") for t in internal_times]

    both_times    = [t for t in internal_times if t in partner_times]
    internal_only = [t for t in internal_times if t not in partner_times]
    partner_only  = [t for t in partner_times  if t not in internal_times]

    slots = [(t, "both") for t in both_times]
    slots += [(t, "internal") for t in internal_only]
    slots += [(t, "partner") for t in partner_only]
    return slots
