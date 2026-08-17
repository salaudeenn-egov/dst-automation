"""Trigger-slot computation shared by scheduler.py and the Airflow scheduler DAG.

A slot is (time "HH:MM" UTC, mode). Modes: "both" posts internal + partner,
"internal" / "partner" post one channel only.
"""
import logging

log = logging.getLogger(__name__)


def parse_report_times(raw):
    """Parse a comma-separated times cell into normalized "HH:MM" strings.

    Invalid entries are logged and skipped, never fatal — one typo in the sheet
    must not take down the other slots.
    """
    times = []
    for part in str(raw or "").split(","):
        candidate = part.strip()
        if not candidate:
            continue
        try:
            hour, minute = candidate.split(":")
            hour, minute = int(hour), int(minute)
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
            times.append(f"{hour:02d}:{minute:02d}")
        except ValueError:
            log.warning(f"invalid report time {candidate!r} — skipped")
    return times


def compute_trigger_slots(row):
    """Return [(time, mode)] for one campaign row, deduplicating overlaps.

    A time present in BOTH report_times and partner_report_times becomes a
    single "both" slot (two same-time runs would collide on the tenant lock).
    With no partner_report_times, every report_times slot posts both channels
    (backward compatible).
    """
    internal = parse_report_times(row.get("report_times"))
    partner = parse_report_times(row.get("partner_report_times"))

    if not partner:
        return [(t, "both") for t in internal]

    slots = [(t, "both") for t in internal if t in partner]
    slots += [(t, "internal") for t in internal if t not in partner]
    slots += [(t, "partner") for t in partner if t not in internal]
    return sorted(slots)
