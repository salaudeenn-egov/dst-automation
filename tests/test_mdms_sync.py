"""Unit tests for the sheet -> MDMS sync planning logic (pure, no network).

Run directly:  python tests/test_mdms_sync.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.mdms import plan_sync, row_identity, validate_row

GROUP = "nigeria_states"


def sheet_row(**overrides):
    base = {"active": "TRUE", "state_name": "Bauchi", "tenant": "ba",
            "campaign_number": "CMP-2026-06-22-000401", "project_type_id": "",
            "campaign_start": "2026-08-10", "campaign_end": "2026-08-20",
            "report_times": "05:30,11:30", "slack_channel": "C0XXX"}
    base.update(overrides)
    return base


def mdms_entry(row, is_active=True, group=GROUP):
    return {"id": "uuid-x", "tenantId": "mz", "isActive": is_active,
            "data": {"rowIdentity": row_identity(row),
                     "deploymentGroup": group,
                     "row": {str(k): str(v) for k, v in row.items()}}}


def test_identity_is_tenant_plus_campaign_number():
    assert row_identity(sheet_row()) == "ba::CMP-2026-06-22-000401"


def test_identity_falls_back_to_project_type_id_then_state():
    r = sheet_row(campaign_number="", project_type_id="644c4356-uuid")
    assert row_identity(r) == "ba::644c4356-uuid"
    r = sheet_row(campaign_number="", project_type_id="")
    assert row_identity(r) == "ba::bauchi"


def test_new_state_row_is_created():
    plan = plan_sync([sheet_row()], [], GROUP)
    assert len(plan["create"]) == 1
    assert plan["create"][0]["rowIdentity"] == "ba::CMP-2026-06-22-000401"
    assert not plan["update"] and not plan["deactivate"]


def test_edited_row_is_updated():
    old = mdms_entry(sheet_row())
    edited = sheet_row(report_times="04:50,11:30")
    plan = plan_sync([edited], [old], GROUP)
    assert len(plan["update"]) == 1 and not plan["create"]


def test_unchanged_row_does_nothing():
    row = sheet_row()
    plan = plan_sync([row], [mdms_entry(row)], GROUP)
    assert plan["unchanged"] == 1
    assert not plan["create"] and not plan["update"] and not plan["deactivate"]


def test_deleted_row_is_deactivated():
    gone = mdms_entry(sheet_row())
    still_here = sheet_row(tenant="ko", campaign_number="CMP-KO-1", state_name="Kogi")
    plan = plan_sync([still_here], [gone], GROUP)
    assert len(plan["deactivate"]) == 1
    assert (plan["deactivate"][0]["data"]["rowIdentity"]
            == "ba::CMP-2026-06-22-000401")


def test_empty_sheet_read_never_mass_deactivates():
    plan = plan_sync([], [mdms_entry(sheet_row())], GROUP)
    assert plan["skip_deactivation"] is True
    assert not plan["deactivate"]


def test_invalid_row_rejected_and_existing_entry_kept():
    existing = mdms_entry(sheet_row())
    broken = sheet_row(campaign_start="not-a-date")
    plan = plan_sync([broken], [existing], GROUP)
    assert len(plan["rejected"]) == 1
    assert "campaign_start" in plan["rejected"][0][1][0]
    assert not plan["deactivate"]          # last-known-good survives a typo
    assert not plan["update"]


def test_duplicate_identity_first_wins():
    a = sheet_row(report_times="05:30")
    b = sheet_row(report_times="11:30")
    plan = plan_sync([a, b], [], GROUP)
    assert len(plan["create"]) == 1
    assert plan["create"][0]["row"]["report_times"] == "05:30"
    assert plan["rejected"] == [("ba::CMP-2026-06-22-000401",
                                 ["duplicate identity in sheet"])]


def test_changed_campaign_number_is_new_campaign():
    old = mdms_entry(sheet_row())
    renumbered = sheet_row(campaign_number="CMP-2026-09-01-000512")
    plan = plan_sync([renumbered], [old], GROUP)
    assert len(plan["create"]) == 1        # new identity -> create
    assert len(plan["deactivate"]) == 1    # old identity vanished -> deactivate


def test_returning_identity_reactivates():
    dormant = mdms_entry(sheet_row(), is_active=False)
    plan = plan_sync([sheet_row()], [dormant], GROUP)
    assert len(plan["update"]) == 1        # update path sets isActive True


def test_other_groups_entries_are_invisible():
    other = mdms_entry(sheet_row(), group="togo")
    plan = plan_sync([], [other], GROUP)
    plan2 = plan_sync([sheet_row(tenant="tg", campaign_number="X",
                                 state_name="Togo")], [other], GROUP)
    assert not plan["deactivate"]          # never touches another tab's entries
    assert len(plan2["create"]) == 1


def test_multiple_tenants_on_one_tab_coexist():
    rows = [sheet_row(),
            sheet_row(tenant="ko", campaign_number="CMP-KO-1", state_name="Kogi"),
            sheet_row(tenant="ch", campaign_number="CMP-CH-1", state_name="Chad")]
    plan = plan_sync(rows, [], GROUP)
    assert len(plan["create"]) == 3
    assert len({d["rowIdentity"] for d in plan["create"]}) == 3


def test_two_campaigns_same_tenant_coexist():
    spaq = sheet_row()
    vas = sheet_row(campaign_number="CMP-2026-09-01-000777")
    plan = plan_sync([spaq, vas], [], GROUP)
    assert len(plan["create"]) == 2        # same tenant ba, different campaigns


def test_partner_only_campaign_is_valid():
    r = sheet_row(report_times="", partner_report_times="12:00")
    assert validate_row(r) == []
    plan = plan_sync([r], [], GROUP)
    assert len(plan["create"]) == 1


def test_no_times_at_all_is_rejected():
    r = sheet_row(report_times="", partner_report_times="")
    assert any("report_times" in p for p in validate_row(r))


def test_validate_row_catches_the_classics():
    assert validate_row(sheet_row()) == []
    assert "tenant is empty" in validate_row(sheet_row(tenant=""))[0]
    assert any("report_times" in p for p in
               validate_row(sheet_row(report_times="25:99,bad")))


if __name__ == "__main__":
    tests = [fn for name, fn in sorted(globals().items()) if name.startswith("test_")]
    for fn in tests:
        fn()
        print(f"PASS  {fn.__name__}")
    print(f"\n{len(tests)} tests passed")
