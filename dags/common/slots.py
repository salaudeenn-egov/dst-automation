"""Due-slot matching for the scheduler DAG.

Pure functions, no Airflow imports — unit-testable with plain Python.

Why wall-clock matching: Airflow 3's CronTriggerTimetable gives a ZERO-WIDTH
data interval (data_interval_start == data_interval_end), so the classic
"[previous tick, this tick)" window does not exist. Each tick therefore matches
slots against [now - lookback, now] computed from datetime.now(UTC).
"""
from datetime import datetime, timedelta, timezone

from pipeline.schedule_utils import compute_trigger_slots


def parse_sheet_date(value):
    """Parse the date formats the config sheet uses. Returns date or None."""
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H-%M-%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def is_row_active(row):
    return str(row.get("active", "")).strip().upper() in ("TRUE", "YES", "1", "Y")


def campaign_window_contains(row, day):
    start = parse_sheet_date(row.get("campaign_start"))
    end = parse_sheet_date(row.get("campaign_end"))
    return bool(start and end and start <= day <= end)


def build_trigger_run_id(group_name, tenant, mode, slot_date, slot_time):
    """Deterministic run id: the same slot can never fire twice
    (TriggerDagRunOperator skips when the run id already exists)."""
    return f"dst_{group_name}_{tenant}_{mode}_{slot_date}_{slot_time.replace(':', '')}"


def find_due_slots(group, rows, now_utc, lookback_minutes, has_report_since=None):
    """Return one trigger payload per slot falling inside (now - lookback, now].

    The lookback gives downtime catch-up: a slot missed while Airflow was down
    still fires on the first tick after recovery, as long as it is within the
    window. Two guards prevent over-firing:
      - the deterministic trigger_run_id (same slot never fires twice), and
      - has_report_since(tenant, mode, slot_dt) -> bool, the retime guard: a
        past-due slot is skipped when the same campaign already produced a
        report at or after that slot's time (e.g. report_times edited from
        05:30 back to 04:50 after the 05:30 run went out).

    The window may cross midnight, so candidate slot datetimes are built for
    every calendar date the window touches.
    """
    window_start = now_utc - timedelta(minutes=lookback_minutes)
    window_dates = {window_start.date(), now_utc.date()}
    due = []

    for row in rows:
        if not is_row_active(row):
            continue
        tenant = str(row.get("tenant", "")).strip().lower()
        state = str(row.get("state_name", "")).strip()
        if not tenant:
            continue

        for slot_time, mode in compute_trigger_slots(row):
            hour, minute = (int(p) for p in slot_time.split(":"))
            for day in sorted(window_dates):
                slot_dt = datetime(day.year, day.month, day.day, hour, minute,
                                   tzinfo=timezone.utc)
                if not (window_start < slot_dt <= now_utc):
                    continue
                if not campaign_window_contains(row, slot_dt.date()):
                    continue
                if has_report_since and has_report_since(tenant, mode, slot_dt):
                    continue
                slot_date = slot_dt.date().isoformat()
                due.append({
                    "trigger_run_id": build_trigger_run_id(
                        group["name"], tenant, mode, slot_date, slot_time),
                    "conf": {
                        "group": group,
                        "row": row,
                        "mode": mode,
                        "tenant": tenant,
                        "state_name": state,
                        "slot_date": slot_date,
                        "slot_time": slot_time,
                    },
                })
    return due
