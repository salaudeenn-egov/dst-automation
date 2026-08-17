"""
run.py — Daily campaign report orchestrator
Usage: python run.py
Reads all active rows from the Google Sheet, runs the full pipeline per row.
"""
import subprocess
import sys
import os


def _ensure_deps():
    try:
        import dotenv, gspread, openpyxl, anthropic, requests, pandas, docx, matplotlib
    except ImportError:
        print("[bootstrap] Installing missing dependencies ...")
        req = os.path.join(os.path.dirname(__file__), "requirements.txt")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req, "-q"])
        print("[bootstrap] Done.")

_ensure_deps()

import logging
from datetime import date, datetime, timedelta

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# ── logging ────────────────────────────────────────────────────────────────────
_log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(_log_dir, exist_ok=True)
_log_file = os.path.join(_log_dir, f"{date.today().isoformat()}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

from pipeline import config, analyze, cdd_sync, report, notify
from pipeline import analyze_itn, cdd_sync_itn, report_itn

# Drug-type dispatch: ITN/LLIN campaigns (household bed-net distribution) use a
# fully separate module set (see analyze_itn.py's docstring for why — SPAQ/AZM's
# per-child age/dose logic doesn't apply to household-based delivery). Every
# other drug_type (SPAQ, AZM, ...) keeps using the original analyze/cdd_sync/report.
_ITN_DRUG_TYPES = {"ITN", "LLIN"}


def _pipeline_modules(cfg):
    if cfg.get("drug_type") in _ITN_DRUG_TYPES:
        return analyze_itn, cdd_sync_itn, report_itn
    return analyze, cdd_sync, report


def _update_sheet_status(cfg, status, step_failed="", error_msg="", drive_link=""):
    """Append a row to the Run Log tab in the config Google Sheet."""
    from pipeline.run_log import append_run_log
    append_run_log(cfg.get("state_name", ""), cfg.get("campaign_name", ""),
                   cfg.get("DAY", ""), status,
                   step_failed=step_failed, error=error_msg, drive_link=drive_link)


def _slack_error(cfg_or_channel, state, step, error):
    """Post a failure alert to Slack so the team knows immediately."""
    import requests as _req
    token   = os.getenv("SLACK_TOKEN")
    channel = (cfg_or_channel.get("slack_channel") if isinstance(cfg_or_channel, dict)
               else cfg_or_channel) or os.getenv("SLACK_CHANNEL", "")
    if not token or not channel:
        return
    try:
        _req.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel,
                  "text": f"PIPELINE FAILURE [{state}] step={step}\n{type(error).__name__}: {error}"},
            timeout=10,
        )
    except Exception:
        pass   # don't let notification failure mask the original error


def run_campaign(row):
    state = str(row.get("state_name", "unknown")).strip()
    try:
        cfg = config.build(row)
    except Exception as e:
        log.error(f"[{state}] config build FAILED: {e}", exc_info=True)
        _slack_error(os.getenv("SLACK_CHANNEL", ""), state, "config", e)
        return

    if not cfg["active"]:
        log.info(f"[{state}] active=FALSE — skipped")
        return

    if not cfg["in_campaign_window"]:
        log.info(
            f"[{state}] outside campaign window "
            f"({cfg['campaign_start']} to {cfg['campaign_end']}) — skipped"
        )
        return

    log.info(f"[{state}] ── Day {cfg['DAY']} / {cfg['campaign_days']} ─────────────")

    analyze_mod, cdd_sync_mod, report_mod = _pipeline_modules(cfg)

    try:
        analyze_mod.run(cfg)
    except Exception as e:
        log.error(f"[{state}] analyze FAILED: {e}", exc_info=True)
        _slack_error(cfg, state, "analyze", e)
        _update_sheet_status(cfg, "FAILED", "analyze", str(e))
        return

    try:
        cdd_sync_mod.run(cfg)
    except Exception as e:
        log.error(f"[{state}] cdd_sync FAILED: {e}", exc_info=True)
        _slack_error(cfg, state, "cdd_sync", e)
        # non-fatal — continue to report with no sync data

    try:
        docx_path, partner_docx_path, slack_text = report_mod.run(cfg)
    except Exception as e:
        log.error(f"[{state}] report FAILED: {e}", exc_info=True)
        _slack_error(cfg, state, "report", e)
        _update_sheet_status(cfg, "FAILED", "report", str(e))
        return

    drive_link = ""
    try:
        drive_link = notify.run(cfg, docx_path, slack_text,
                                partner_docx_path=partner_docx_path) or ""
    except Exception as e:
        log.error(f"[{state}] notify FAILED: {e}", exc_info=True)
        _slack_error(cfg, state, "notify", e)
        _update_sheet_status(cfg, "FAILED", "notify", str(e))
        return

    _update_sheet_status(cfg, "SUCCESS", drive_link=drive_link)
    log.info(f"[{state}] DONE")


def _parse_cli_date(s):
    """
    Parse a CLI date. Accepts YYYY-MM-DD, YYYYMMDD, DD/MM/YYYY, DD-MM-YYYY, DDMMYYYY.
    For 8-digit input, YYYYMMDD is tried before DDMMYYYY (20260708 -> 2026-07-08;
    08072026, invalid as YYYYMMDD, falls through to 2026-07-08). Returns date or None.
    """
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%d-%m-%Y", "%d%m%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def run_cumulative(row, end_date):
    """
    Build a cumulative / overall-target report for one campaign row.

    Covers the WHOLE campaign (all distribution + mop-up days) from campaign_start
    through end_date (inclusive). Coverage is measured against the FULL campaign
    target (undivided), not a per-day target. Generates files locally only —
    no Slack post and no Drive upload (no_upload=True, notify step skipped).
    """
    state = str(row.get("state_name", "unknown")).strip()
    cfg   = config.build(row)

    if not cfg["active"]:
        log.info(f"[{state}] active=FALSE — skipped")
        return None

    start = cfg["campaign_start"]
    end   = end_date or cfg["campaign_end"]
    if end < start:
        log.error(f"[{state}] end {end} is before start {start} — skipped")
        return None

    total_days = (end - start).days + 1
    dates      = [(start + timedelta(days=i)).isoformat() for i in range(total_days)]

    from pipeline.config import _date_label
    cfg.update({
        "cumulative":         True,
        "no_upload":          False,      # DO upload the Excels to Drive (so the docs
                                          # link to them); Slack is still never touched —
                                          # run_cumulative never calls notify.run().
        "DAY":                total_days,
        "campaign_days":      total_days, # labels read "Days 1-N"; target math is
                                          # decoupled from this via the cumulative flag
        "GTE":                f"{start.isoformat()}T00:00:00.000Z",
        "LTE":                f"{end.isoformat()}T23:59:59.999Z",
        "CAMPAIGN_DATES":     dates,
        "END_LABEL":          _date_label(end),
        "in_campaign_window": True,
    })

    out_dir    = cfg["out_dir"]
    hm         = datetime.now().strftime("%H%M")
    state_slug = state.replace(" ", "_")
    cfg["perf_xlsx"]         = os.path.join(out_dir, "performance_cumulative.xlsx")
    cfg["sync_xlsx"]         = os.path.join(out_dir, "cdd_sync_cumulative.xlsx")
    cfg["docx_path"]         = os.path.join(out_dir, f"{state_slug}_Cumulative_Campaign_Report_{hm}.docx")
    cfg["partner_docx_path"] = os.path.join(out_dir, f"{state_slug}_Cumulative_PartnerReport_{hm}.docx")

    log.info(f"[{state}] ── CUMULATIVE Days 1-{total_days}  ({start} to {end}) ─────────────")

    analyze_mod, cdd_sync_mod, report_mod = _pipeline_modules(cfg)

    analyze_mod.run(cfg)

    try:
        cdd_sync_mod.run(cfg)
    except Exception as e:
        log.error(f"[{state}] cdd_sync FAILED (non-fatal — continuing to report): {e}", exc_info=True)

    # report.run already uploaded the performance + CDD-sync Excels to Drive and embedded
    # those links inside both docs (no_upload=False); it stashes the links back on cfg.
    docx_path, partner_docx_path, _slack_text = report_mod.run(cfg)

    # Upload the two Word reports to Drive too (Slack is NOT touched — cumulative never
    # calls notify.run(), so nothing is posted to any channel).
    hm2            = datetime.now().strftime("%H:%M")
    internal_title = f"{state} Cumulative Days 1-{total_days} Report — {cfg['END_LABEL']} {hm2}"
    partner_title  = f"{state} Cumulative Days 1-{total_days} Report (Partner) — {cfg['END_LABEL']} {hm2}"
    _fid           = notify.campaign_folder_id(cfg)
    internal_link  = (notify.upload_file(docx_path, internal_title, folder_id=_fid)
                      if docx_path and os.path.exists(docx_path) else "")
    partner_link   = (notify.upload_file(partner_docx_path, partner_title, folder_id=_fid)
                      if partner_docx_path and os.path.exists(partner_docx_path) else "")

    # Collect the files actually written for the summary list. Two possible
    # chart filename patterns — SPAQ/AZM's progress_chart_day{N}.png vs ITN's
    # itn_progress_chart_day{N}.png (report_itn.py names it off the last logged
    # itn_history/ day, which may differ from total_days if a day was missed).
    chart_candidates = [
        os.path.join(out_dir, f"progress_chart_day{total_days}.png"),
        os.path.join(out_dir, f"itn_progress_chart_day{total_days}.png"),
    ]
    chart_path = next((p for p in chart_candidates if os.path.exists(p)), chart_candidates[0])
    candidates = [
        ("Performance Excel", cfg["perf_xlsx"]),
        ("CDD Sync Excel",    cfg["sync_xlsx"]),
        ("Internal Report",   docx_path),
        ("Partner Report",    partner_docx_path),
        ("Progress Chart",    chart_path),
    ]
    files = [(label, p) for label, p in candidates if p and os.path.exists(p)]

    # Collect Drive links (internal + partner docs, and the Excels the docs link to)
    link_items = [
        ("Internal Report (Doc)",     internal_link),
        ("Partner Report (Doc)",      partner_link),
        ("Performance Excel (Sheet)", cfg.get("perf_drive_link", "")),
        ("CDD Sync Excel (Sheet)",    cfg.get("sync_drive_link", "")),
        ("Partner Perf Excel (Sheet)", cfg.get("partner_perf_drive_link", "")),
    ]
    links = [(label, url) for label, url in link_items if url]

    log.info(f"[{state}] cumulative DONE — {len(files)} file(s), {len(links)} Drive link(s)")
    return {"state": state, "days": total_days, "start": start, "end": end,
            "files": files, "links": links}


def _print_generated(generated):
    log.info("=" * 60)
    if not generated:
        log.warning("No cumulative reports generated.")
        return
    print("\nCumulative reports generated:\n")
    for res in generated:
        print(f"  [{res['state']}]  Days 1-{res['days']}  ({res['start']} to {res['end']})")
        print("    Local files:")
        for label, p in res["files"]:
            print(f"      {label:<18}: {p}")
        if res.get("links"):
            print("    Google Drive links:")
            for label, url in res["links"]:
                print(f"      {label:<28}: {url}")
        print()


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(
        description="DST campaign report runner (daily by default, or --cumulative)."
    )
    parser.add_argument(
        "--cumulative", nargs="?", const="", default=None, metavar="END_DATE",
        help="Generate a cumulative overall-target report for the whole campaign "
             "(all distribution + mop-up days). Optionally pass the LAST day "
             "(inclusive) as YYYYMMDD or YYYY-MM-DD; defaults to the sheet's campaign_end.",
    )
    parser.add_argument("--end", default=None,
                        help="Alias for the cumulative end date (YYYYMMDD or YYYY-MM-DD).")
    parser.add_argument("--state", default=None,
                        help="Only run this state_name (default: all active rows).")
    args = parser.parse_args(argv)

    is_cumulative = args.cumulative is not None

    log.info("=" * 60)
    log.info(f"DST {'CUMULATIVE' if is_cumulative else 'Daily'} Report Run  —  {date.today().isoformat()}")
    log.info("=" * 60)

    try:
        rows = config.get_active_rows()
    except Exception as e:
        log.error(f"Failed to read Google Sheet: {e}", exc_info=True)
        sys.exit(1)

    if not rows:
        log.warning("No rows returned from Google Sheet — nothing to do")
        return

    if args.state:
        want = args.state.strip().lower()
        rows = [r for r in rows if str(r.get("state_name", "")).strip().lower() == want]
        if not rows:
            log.error(f"No campaign row matching --state '{args.state}'")
            sys.exit(1)

    if is_cumulative:
        raw_end  = (args.cumulative or args.end or "").strip()
        end_date = _parse_cli_date(raw_end) if raw_end else None
        if raw_end and not end_date:
            log.error(f"Could not parse cumulative end date '{raw_end}' — use YYYYMMDD or YYYY-MM-DD")
            sys.exit(1)
        if end_date:
            log.info(f"Cumulative end date (last day, inclusive): {end_date}")
        else:
            log.info("Cumulative end date not given — using each row's campaign_end")

        generated = []
        for row in rows:
            state = str(row.get("state_name", "unknown")).strip()
            try:
                res = run_cumulative(row, end_date)
                if res:
                    generated.append(res)
            except Exception as e:
                log.error(f"[{state}] cumulative run failed: {e}", exc_info=True)
        _print_generated(generated)
        log.info("Cumulative run complete")
        log.info("=" * 60)
        return

    for row in rows:
        state = str(row.get("state_name", "unknown")).strip()
        try:
            run_campaign(row)
        except Exception as e:
            log.error(f"[{state}] unexpected error: {e}", exc_info=True)

    log.info("=" * 60)
    log.info("Run complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
