# Pipeline developer guide

Business logic for the DST campaign reporting pipeline. `run.py` (manual) and
`scheduler.py` (unattended) orchestrate these modules today; the Airflow DAGs will
call the same `run(cfg)` entry points.

## Module map

```
config.py         Google Sheet row -> resolved cfg dict (dates, ES indices, paths)
analyze.py        ES task docs -> performance_dayN.xlsx        (SPAQ / AZM)
cdd_sync.py       ES staff + sync indices -> cdd_sync_dayN.xlsx
report.py         both Excels + LLM narrative -> Word docs + Slack text
notify.py         Google Drive upload + Slack post
analyze_itn.py    ITN/LLIN variants (household grain, LGA targets)
cdd_sync_itn.py     — separate by design; see their module docstrings
report_itn.py
core/             shared plumbing, no business logic
  es.py             scroll_all, scroll_batches, composite_agg
  excel.py          openpyxl styling (style_cell, fills, FLAG_COLOR)
  word.py           python-docx primitives (hdr, dat, tables, hyperlinks)
  llm.py            generate_narrative (Groq chat API)
  checkpoint.py     save_checkpoint / load_checkpoint (stage snapshots)
```

## Stage structure

`analyze` and `cdd_sync` are split into two stages with a JSON snapshot between:

```
collect(cfg)   all ES I/O -> plain data (rows, counters)
     |
save_checkpoint(cfg, stage, data)     -> {out_dir}/checkpoints/{stage}_dayN.json
     |
render(cfg, ...)   pure: data -> Excel file, no network
```

`report.run(cfg)` composes named stages: `_load_inputs` (Excel parsing) ->
`_build_trajectory` (day-by-day totals + chart) -> `_publish_excels` (Drive) ->
`_generate_narratives` (all LLM calls) -> `_render_docs` (internal + partner docx).
Its durable inputs are the Excels themselves, so it has no separate checkpoint.

## Debugging a bad or failed run

1. Find the stage in the log — every line is tagged, e.g.
   `[analyze:collect] 152,328 records -> 274 facilities`.
2. Re-render offline from the snapshot (no ES access, no credentials needed):

```python
from pipeline import config, analyze, cdd_sync
cfg = config.build(row)                  # or reconstruct the cfg dict by hand
analyze.rerun_from_checkpoint(cfg)       # rebuilds performance_dayN.xlsx
cdd_sync.rerun_from_checkpoint(cfg)      # rebuilds cdd_sync_dayN.xlsx
```

3. Inspect the checkpoint JSON directly — it is the evidence of what ES returned
   at run time (ES data keeps changing as syncs land; a later re-query cannot
   reproduce the original numbers).

## Name resolution (child and household-head names)

Names come only from the individual index, never from task additionalDetails:

```
project-task.projectBeneficiaryClientReferenceId
  -> project-beneficiary.beneficiaryClientReferenceId   (= child individual id)
  -> household-member (by individualClientReferenceId)  -> householdClientReferenceId
  -> household-member (isHeadOfHousehold=true)          -> head individual id
  -> individual index -> name.givenName + familyName
```

Implemented in `analyze._resolve_batch_names`; the per-index lookups are
`_map_beneficiary_refs_to_individual_ids`, `_map_individual_ids_to_household_ids`,
`_map_household_ids_to_head_ids`, `_fetch_individual_names`.

## Conventions

- `run(cfg)` is the only public seam orchestrators may call; its signature and
  return values are frozen.
- All ES I/O belongs in a collect-side function; render-side functions take plain
  data and may only write their output file.
- Checkpoint payloads must stay JSON-safe (convert sets to sorted lists).
- Config comes only from `config.build(row)`; modules never read the sheet or
  `.env` directly (exception: `core/llm.py` reads its GROQ_* keys).
- Tenant/drug specifics live in config values, not `if tenant == ...` branches.
