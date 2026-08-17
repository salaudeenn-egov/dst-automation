"""Executes the whole campaign pipeline for one sheet row, inside one task.

Everything runs in a single process/pod on purpose: intermediate files
(Excels, chart, docx, checkpoints) stay on local disk, so no shared storage
is needed between Airflow tasks. Per-stage visibility comes from the stage-
tagged pipeline logs and from the checkpoints, which are pushed to the
campaign's Drive temp/ folder even when the run fails.

Error classification (retry only what a retry can fix):
  - KeyError/ValueError/TypeError/AttributeError -> AirflowFailException:
    malformed data or config fails identically on retry, so fail immediately.
  - Anything else (network, ES, Drive, Slack) -> normal raise, Airflow retries.
  - cdd_sync failures never kill the run — the report proceeds without sync
    data, and the marker records the degradation.
"""
import logging

log = logging.getLogger(__name__)

try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    try:
        from airflow.exceptions import AirflowFailException
    except ImportError:  # unit tests without Airflow installed
        class AirflowFailException(Exception):
            pass

DATA_ERRORS = (KeyError, ValueError, TypeError, AttributeError)
ITN_DRUG_TYPES = {"ITN", "LLIN"}


def select_pipeline_modules(drug_type):
    """ITN/LLIN campaigns use the household-grain module set; everything else
    (SPAQ, AZM) uses the per-child originals."""
    if drug_type in ITN_DRUG_TYPES:
        from pipeline import analyze_itn, cdd_sync_itn, report_itn
        return analyze_itn, cdd_sync_itn, report_itn
    from pipeline import analyze, cdd_sync, report
    return analyze, cdd_sync, report


def _run_stage(stage_name, fn, marker):
    try:
        result = fn()
        marker["stages"][stage_name] = "ok"
        return result
    except DATA_ERRORS as e:
        marker["stages"][stage_name] = f"failed: {e}"
        raise AirflowFailException(
            f"{stage_name} failed on malformed data/config — retry would fail "
            f"identically: {type(e).__name__}: {e}") from e
    except Exception as e:
        marker["stages"][stage_name] = f"failed: {e}"
        raise


def execute_campaign(row, mode="both"):
    """Run analyze -> cdd_sync -> report -> notify for one campaign row.

    Returns a marker dict: {"ok": True, ...} for a real report,
    {"ok": None, "reason": ...} for a routine no-op (inactive / out of window).
    Raises on genuine failure. Checkpoints are uploaded to Drive in all cases.
    """
    from pipeline import config, notify

    cfg = config.build(row)
    state = cfg["state_name"]

    if not cfg["active"]:
        return {"ok": None, "reason": "row inactive"}
    if not cfg["in_campaign_window"]:
        return {"ok": None,
                "reason": f"outside campaign window "
                          f"({cfg['campaign_start']} to {cfg['campaign_end']})"}

    analyze_mod, cdd_sync_mod, report_mod = select_pipeline_modules(cfg["drug_type"])
    marker = {"ok": True, "tenant": cfg["tenant"], "state": state,
              "mode": mode, "day": cfg["DAY"], "stages": {}, "drive_link": ""}

    log.info(f"[runner] {state} Day {cfg['DAY']}/{cfg['campaign_days']} mode={mode}")
    try:
        _run_stage("analyze", lambda: analyze_mod.run(cfg), marker)

        try:
            cdd_sync_mod.run(cfg)
            marker["stages"]["cdd_sync"] = "ok"
        except Exception as e:
            marker["stages"]["cdd_sync"] = f"degraded: {e}"
            log.error(f"[runner] cdd_sync failed (non-fatal — report continues "
                      f"without sync data): {e}", exc_info=True)

        docx_path, partner_docx_path, slack_text = _run_stage(
            "report", lambda: report_mod.run(cfg), marker)

        drive_link = _run_stage(
            "notify",
            lambda: notify.run(cfg, docx_path, slack_text,
                               partner_docx_path=partner_docx_path, mode=mode),
            marker)
        marker["drive_link"] = drive_link or ""
    finally:
        # Even a failed run leaves its checkpoints on Drive for offline debugging
        # (the pod's local disk disappears with the pod).
        try:
            notify.upload_checkpoints(cfg)
        except Exception as e:
            log.warning(f"[runner] checkpoint upload failed (non-fatal): {e}")

    log.info(f"[runner] {state} complete: {marker['stages']}")
    return marker
