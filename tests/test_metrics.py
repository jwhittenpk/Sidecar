"""Tests for metrics module and related API routes."""

import json
from unittest.mock import MagicMock, patch

import pytest

import app as app_module
import metrics as metrics_module


def test_parse_github_pr_urls():
    text = "See https://github.com/acme/rp/pull/12 and also http://github.com/X/Y/pull/3/files"
    got = metrics_module.parse_github_pr_urls(text)
    assert ("acme", "rp", 12) in got
    assert ("X", "Y", 3) in got


def test_discover_pr_refs_lists_pulls_not_search():
    """Fallback discovery uses GET .../pulls (repo read), not /search/issues (often 403)."""
    urls_requested = []

    def fake_get(url, headers=None, params=None, timeout=None):
        urls_requested.append(url)
        m = MagicMock()
        m.status_code = 200
        m.headers = {}
        m.raise_for_status = MagicMock()
        if "/pulls" in url and "search" not in url:
            m.json.return_value = [
                {
                    "number": 77,
                    "title": "Feature PKINT-198",
                    "user": {"login": "devperson"},
                    "head": {"ref": "chore/PKINT-198-x"},
                }
            ]
        else:
            m.json.return_value = []
        return m

    sess = MagicMock()
    sess.get.side_effect = fake_get
    refs = metrics_module.discover_pr_refs_for_issue(
        "PKINT-198",
        "",
        "hcapatientkeeper",
        ["myrepo"],
        "tok",
        "devperson",
        sess,
    )
    assert ("hcapatientkeeper", "myrepo", 77) in refs
    assert not any("search" in u for u in urls_requested)
    assert any("/repos/hcapatientkeeper/myrepo/pulls" in u for u in urls_requested)


def test_discover_pr_refs_no_substring_false_positive():
    """PK-15 must not claim PRs whose title contains PK-1516, PK-150, etc."""
    sess = MagicMock()
    pull_resp = MagicMock()
    pull_resp.status_code = 200
    pull_resp.headers = {}
    pull_resp.raise_for_status = MagicMock()
    pull_resp.json.return_value = [
        # Should NOT match PK-15 — the number continues
        {"number": 1516, "title": "PK-1516: Some feature", "user": {"login": "me"}, "head": {"ref": "chore/PK-1516-x"}},
        {"number": 150, "title": "PK-150: Another feature", "user": {"login": "me"}, "head": {"ref": "PK-150-y"}},
        # SHOULD match PK-15 — exact identifier in title/branch
        {"number": 15, "title": "PK-15: Actual ticket", "user": {"login": "me"}, "head": {"ref": "chore/PK-15-fix"}},
    ]
    sess.get.return_value = pull_resp
    refs = metrics_module.discover_pr_refs_for_issue("PK-15", "", "org", ["repo"], "tok", "me", sess)
    nums = [r[2] for r in refs]
    assert 15 in nums
    assert 1516 not in nums
    assert 150 not in nums


def test_pull_cache_reuses_single_repo_fetch():
    sess = MagicMock()
    pull_resp = MagicMock()
    pull_resp.status_code = 200
    pull_resp.headers = {}
    pull_resp.raise_for_status = MagicMock()
    pull_resp.json.return_value = [
        {
            "number": 1,
            "title": "PK-1",
            "user": {"login": "a"},
            "head": {"ref": "main"},
        }
    ]
    sess.get.return_value = pull_resp
    cache: dict = {}
    metrics_module.discover_pr_refs_for_issue("PK-1", "", "o", ["r"], "t", "a", sess, pull_cache=cache)
    metrics_module.discover_pr_refs_for_issue("PK-1", "", "o", ["r"], "t", "a", sess, pull_cache=cache)
    assert sess.get.call_count == 1


def test_parse_identifier_team_number():
    assert metrics_module.parse_identifier_team_number("LIN-42") == ("LIN", 42.0)
    assert metrics_module.parse_identifier_team_number("bad") is None


def test_overlay_entry_eligible_for_backfill_personal():
    assert metrics_module.overlay_entry_eligible_for_backfill_personal({"personal_status": "Completed"}) is True
    assert metrics_module.overlay_entry_eligible_for_backfill_personal({"personal_status": ""}) is False
    assert metrics_module.overlay_entry_eligible_for_backfill_personal({}) is False


def test_display_days_round_up():
    assert metrics_module.display_days_round_up(6.462398) == 6.5
    assert metrics_module.display_days_round_up(6.0) == 6.0
    assert metrics_module.display_days_round_up(6.01) == 6.1
    assert metrics_module.display_days_round_up(0) == 0.0
    assert metrics_module.display_days_round_up(-0.5) == 0.0


def test_record_and_compute_linear_dwell(tmp_path):
    p = tmp_path / "m.json"
    issues = [
        {
            "id": "i1",
            "identifier": "T-1",
            "linear_status": "In Progress",
            "linear_state_id": "s1",
            "linear_state_type": "started",
        }
    ]
    metrics_module.record_linear_snapshots(issues, path=p)
    store = metrics_module.read_metrics_store(p)
    assert len(store["linear_transitions"]) == 1

    issues2 = [{**issues[0], "linear_status": "In Review", "linear_state_id": "s2", "linear_state_type": "started"}]
    metrics_module.record_linear_snapshots(issues2, path=p)
    store = metrics_module.read_metrics_store(p)
    assert len(store["linear_transitions"]) == 2

    dwell = metrics_module.compute_linear_dwell_by_state(store, ["In Progress"])
    assert dwell["issues_tracked"] == 1
    assert "In Review" in dwell["per_state_days"]
    assert dwell["per_state_issues"].get("In Review") == ["T-1"]
    # In Progress segment only if the two snapshot timestamps differ (often same second in tests)
    if "In Progress" in dwell["per_state_days"]:
        assert dwell["per_state_issues"].get("In Progress") == ["T-1"]


def test_compute_linear_dwell_full_timeline_counts_todo_without_cycle_start(tmp_path):
    """Issues that never hit 'In Progress' still accrue time in Todo, Done, etc."""
    store = {
        "linear_transitions": [
            {
                "t": "2025-01-01T00:00:00Z",
                "issue_id": "iss1",
                "identifier": "X-1",
                "state_id": "t1",
                "state_name": "Todo",
                "state_type": "unstarted",
            },
            {
                "t": "2025-01-10T00:00:00Z",
                "issue_id": "iss1",
                "identifier": "X-1",
                "state_id": "d1",
                "state_name": "Done",
                "state_type": "completed",
            },
        ],
        "linear_last_sample": {},
    }
    dwell = metrics_module.compute_linear_dwell_by_state(
        store, ["In Progress"], now_utc=metrics_module.datetime(2025, 6, 1, tzinfo=metrics_module.timezone.utc)
    )
    assert dwell["issues_tracked"] == 1
    assert dwell["per_state_days"].get("Todo") == 9.0
    assert "Done" not in dwell["per_state_days"]
    assert dwell["per_state_issues"]["Todo"] == ["X-1"]


def _clear_github_token_env(monkeypatch):
    for k in ("SIDECAR_TOKEN", "GITHUB_TOKEN"):
        monkeypatch.delenv(k, raising=False)


def test_get_github_token_from_env(monkeypatch):
    _clear_github_token_env(monkeypatch)
    monkeypatch.setenv("SIDECAR_TOKEN", "ghp_testtoken1234567890123456789012345678")
    t = metrics_module.get_github_token()
    assert t.startswith("ghp_")


def test_get_github_token_prefers_sidecar_over_github(monkeypatch):
    _clear_github_token_env(monkeypatch)
    monkeypatch.setenv("SIDECAR_TOKEN", "ghp_sidecar")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_legacy")
    assert metrics_module.get_github_token() == "ghp_sidecar"


def test_get_github_token_github_fallback(monkeypatch):
    _clear_github_token_env(monkeypatch)
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_fallback012345678901234567890123456789")
    assert metrics_module.get_github_token().startswith("ghp_")


def test_get_github_token_missing(monkeypatch):
    _clear_github_token_env(monkeypatch)
    with pytest.raises(ValueError, match="GitHub is not configured"):
        metrics_module.get_github_token()


def test_github_enrich_gate_respects_cooldown(tmp_path, monkeypatch):
    monkeypatch.setenv("SIDECAR_GITHUB_REFRESH_COOLDOWN_SEC", "600")
    p = tmp_path / "m.json"
    now = metrics_module.datetime.now(metrics_module.timezone.utc)
    p.write_text(
        json.dumps(
            {
                "last_github_enrich_at": now.isoformat().replace("+00:00", "Z"),
                "github_prs": {},
                "linear_transitions": [],
                "linear_last_sample": {},
            }
        ),
        encoding="utf-8",
    )
    g = metrics_module.github_enrich_gate(p, force=False)
    assert g["allowed"] is False
    assert g["seconds_until_next"] is not None
    g2 = metrics_module.github_enrich_gate(p, force=True)
    assert g2["allowed"] is True


def test_merge_site_defaults():
    m = metrics_module.merge_site_defaults(None)
    assert m["github"]["org"] == metrics_module.DEFAULT_GITHUB_ORG
    assert "int-transporter" in m["github"]["repos"]


@pytest.fixture
def client():
    app_module.app.config["TESTING"] = True
    return app_module.app.test_client()


def test_api_metrics_get_empty_store(client):
    """Flask test client: GET /api/metrics returns JSON shape."""
    resp = client.get("/api/metrics")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "linear_dwell_days_by_state" in data
    assert "linear_dwell_issues_by_state" in data
    assert isinstance(data["linear_dwell_issues_by_state"], dict)
    assert "github_prs" in data


def test_api_settings_site_get_post(temp_overlay_path, client):
    resp = client.get("/api/settings/site")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "github" in data and "metrics" in data

    resp2 = client.post(
        "/api/settings/site",
        data=json.dumps({"github": {"org": "myorg", "repos": ["a"], "login": "me"}}),
        content_type="application/json",
    )
    assert resp2.status_code == 200
    body = resp2.get_json()
    assert body["site"]["github"]["org"] == "myorg"
    assert body["site"]["github"]["repos"] == ["a"]


def test_backfill_github_requires_token(monkeypatch, temp_overlay_path, client):
    for k in ("SIDECAR_TOKEN", "GITHUB_TOKEN"):
        monkeypatch.delenv(k, raising=False)
    resp = client.post("/api/metrics/backfill-github")
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_backfill_linear_dwell_requires_linear_token(monkeypatch, temp_overlay_path, client):
    monkeypatch.delenv("LINEAR_GRAPHQL_API", raising=False)
    monkeypatch.delenv("LINEAR_GRAPHQL_API_FILE", raising=False)
    resp = client.post("/api/metrics/backfill-linear-dwell")
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_post_api_backfill_linear_dwell_ok(temp_overlay_path, client):
    with patch.object(app_module, "get_linear_token", return_value="tok"):
        with patch.object(app_module, "_fetch_all_assigned_issues", return_value=[]):
            with patch.object(
                app_module.metrics_module,
                "run_linear_dwell_backfill",
                return_value={
                    "processed": ["LIN-1"],
                    "skipped": [],
                    "transition_rows_total": 4,
                    "from_completed_overlay": 0,
                    "from_assigned": 1,
                    "from_metrics_store": 0,
                },
            ) as m:
                resp = client.post("/api/metrics/backfill-linear-dwell")
    assert m.call_args.kwargs.get("assigned_nodes") == []
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["processed"] == ["LIN-1"]
    assert data["transition_rows_total"] == 4
    assert data["from_assigned"] == 1


def test_run_github_backfill_mocked_linear_github(tmp_path):
    store_path = tmp_path / "ms.json"
    completed = {
        "LIN-1": {"personal_status": "Completed", "notes": ""},
        "LIN-2": {"personal_status": "", "notes": ""},
    }

    def linear_fn(query, variables):
        team = variables.get("teamKey")
        num = variables.get("num")
        if team == "LIN" and num == 1:
            return {
                "issues": {
                    "nodes": [
                        {
                            "id": "x",
                            "identifier": "LIN-1",
                            "description": "",
                            "createdAt": "2024-01-01T00:00:00.000Z",
                            "state": {"id": "d", "name": "Done", "type": "completed"},
                        }
                    ]
                }
            }
        return {"issues": {"nodes": []}}

    site = metrics_module.merge_site_defaults(None)

    with patch.object(metrics_module, "discover_pr_refs_for_issue", return_value=[]):
        out = metrics_module.run_github_backfill(
            completed,
            site,
            "gh-token",
            linear_fn,
            "Done",
            path=store_path,
            session=MagicMock(),
        )
    assert "LIN-1" in out["processed"]
    assert any(s.get("identifier") == "LIN-2" and "not_completed" in s.get("reason", "") for s in out["skipped"])


def test_build_metrics_api_payload_github_split():
    store = {
        "linear_transitions": [],
        "linear_last_sample": {},
        "github_prs": {
            "o/r#1": {
                "owner": "o",
                "repo": "r",
                "number": 1,
                "outcome": "merged",
                "open_seconds_non_draft": 86400.0,
                "linear_identifier": "X-1",
                "html_url": "http://example.com",
            },
            "o/r#2": {
                "owner": "o",
                "repo": "r",
                "number": 2,
                "outcome": "closed_unmerged",
                "open_seconds_non_draft": 43200.0,
                "linear_identifier": "X-2",
            },
        },
    }
    site = metrics_module.merge_site_defaults(None)
    payload = metrics_module.build_metrics_api_payload(store, site)
    assert payload["github_merged_count"] == 1
    assert payload["github_closed_unmerged_count"] == 1
    assert 1.0 in payload["github_merged_open_days"]
    assert payload["linear_dwell_issues_by_state"] == {}

    store_frac = {
        "linear_transitions": [],
        "linear_last_sample": {},
        "github_prs": {
            "o/r#9": {
                "owner": "o",
                "repo": "r",
                "number": 9,
                "outcome": "merged",
                "open_seconds_non_draft": 90000.0,
                "linear_identifier": "X-9",
            },
        },
    }
    payload2 = metrics_module.build_metrics_api_payload(store_frac, site)
    merged_row = next(r for r in payload2["github_prs"] if r.get("number") == 9)
    assert merged_row["open_days_non_draft"] == 1.1


def test_collect_issue_ids_for_dwell_backfill_pass_unions_disk_and_memory():
    store = {
        "linear_transitions": [{"issue_id": "disk-only"}],
        "linear_last_sample": {"last-key": {"state_id": "x"}},
    }
    transitions = [{"issue_id": "mem-only"}]
    last = {"last-key": {}}
    ids = metrics_module.collect_issue_ids_for_dwell_backfill_pass(store, transitions, last)
    assert ids == {"disk-only", "mem-only", "last-key"}


def test_apply_linear_history_fallback_when_history_empty():
    transitions: list = []
    last: dict = {}
    gid = "550e8400-e29b-41d4-a716-446655440002"

    def linear_fn(query, variables):
        return {
            "issues": {
                "nodes": [
                    {
                        "id": variables.get("issueId"),
                        "history": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": []},
                    }
                ]
            }
        }

    ok, reason = metrics_module.apply_linear_history_for_issue(
        gid,
        "PK-15",
        "2025-02-10T12:00:00.000Z",
        {"id": "ip", "name": "In Progress", "type": "started"},
        linear_fn,
        transitions,
        last,
        "2026-04-01T00:00:00Z",
    )
    assert ok is True
    assert reason is None
    assert len(transitions) == 1
    assert transitions[0]["state_name"] == "In Progress"
    assert transitions[0]["t"] == "2025-02-10T12:00:00.000Z"
    assert transitions[0].get("source") == "linear_backfill_fallback_no_history"


def test_run_linear_dwell_backfill_store_id_pass_when_not_assigned(tmp_path):
    ghost = "550e8400-e29b-41d4-a716-446655440000"
    store_path = tmp_path / "ms.json"
    metrics_module.write_metrics_store(
        {
            "version": 1,
            "linear_transitions": [
                {
                    "t": "2026-01-01T00:00:00Z",
                    "issue_id": ghost,
                    "identifier": "PK-15",
                    "state_id": "x",
                    "state_name": "In Progress",
                    "state_type": "started",
                }
            ],
            "linear_last_sample": {},
            "github_prs": {},
        },
        store_path,
    )

    def linear_fn(query, variables):
        iid = variables.get("issueId")
        if iid != ghost:
            return {"issues": {"nodes": []}}
        if "IssueByUuidFilter" in query:
            return {
                "issues": {
                    "nodes": [
                        {
                            "id": ghost,
                            "identifier": "PK-15",
                            "createdAt": "2025-02-10T12:00:00.000Z",
                            "state": {"id": "ip", "name": "In Progress", "type": "started"},
                        }
                    ]
                }
            }
        if "IssueHistoryPage" in query:
            return {
                "issues": {
                    "nodes": [
                        {
                            "id": ghost,
                            "history": {
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                                "nodes": [],
                            },
                        }
                    ]
                }
            }
        raise AssertionError("unexpected query fragment")

    out = metrics_module.run_linear_dwell_backfill(
        {},
        linear_fn,
        "Done",
        path=store_path,
        assigned_nodes=[],
    )
    assert out["from_metrics_store"] == 1
    assert out["from_completed_overlay"] == 0
    assert out["from_assigned"] == 0
    assert "PK-15" in out["processed"]


def test_history_nodes_to_transition_rows_prepends_from_state_at_created():
    rows = metrics_module.history_nodes_to_transition_rows(
        "i1",
        "T-1",
        "2024-01-01T00:00:00.000Z",
        [
            {
                "createdAt": "2024-01-05T00:00:00.000Z",
                "fromState": {"id": "a", "name": "Todo", "type": "unstarted"},
                "toState": {"id": "b", "name": "In Progress", "type": "started"},
            },
        ],
    )
    assert len(rows) == 2
    assert rows[0]["t"] == "2024-01-01T00:00:00.000Z"
    assert rows[0]["state_name"] == "Todo"
    assert rows[1]["state_name"] == "In Progress"
    assert rows[0].get("source") == "linear_history_backfill"


def test_run_linear_dwell_backfill_replaces_issue_transitions(tmp_path):
    store_path = tmp_path / "ms.json"
    metrics_module.write_metrics_store(
        {
            "version": 1,
            "linear_transitions": [
                {
                    "t": "2020-01-01T00:00:00Z",
                    "issue_id": "other",
                    "identifier": "OTHER",
                    "state_id": "s0",
                    "state_name": "Backlog",
                    "state_type": "unstarted",
                }
            ],
            "linear_last_sample": {},
            "github_prs": {},
        },
        store_path,
    )
    completed = {"LIN-1": {"personal_status": "Completed", "notes": ""}}

    def linear_fn(query, variables):
        if variables.get("teamKey") is not None:
            return {
                "issues": {
                    "nodes": [
                        {
                            "id": "a1111111-1111-4111-8111-111111111111",
                            "identifier": "LIN-1",
                            "description": "",
                            "createdAt": "2024-01-01T10:00:00.000Z",
                            "state": {"id": "done", "name": "Done", "type": "completed"},
                        }
                    ]
                }
            }
        if variables.get("issueId"):
            if variables["issueId"] != "a1111111-1111-4111-8111-111111111111":
                return {"issues": {"nodes": []}}
            return {
                "issues": {
                    "nodes": [
                        {
                            "id": "a1111111-1111-4111-8111-111111111111",
                            "history": {
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                                "nodes": [
                                    {
                                        "createdAt": "2024-01-02T10:00:00.000Z",
                                        "fromState": {
                                            "id": "s1",
                                            "name": "In Progress",
                                            "type": "started",
                                        },
                                        "toState": {
                                            "id": "done",
                                            "name": "Done",
                                            "type": "completed",
                                        },
                                    },
                                ],
                            },
                        }
                    ]
                }
            }
        raise AssertionError("unexpected linear_fn query")

    out = metrics_module.run_linear_dwell_backfill(
        completed,
        linear_fn,
        "Done",
        path=store_path,
    )
    assert "LIN-1" in out["processed"]
    store = metrics_module.read_metrics_store(store_path)
    u1 = [t for t in store["linear_transitions"] if t.get("issue_id") == "a1111111-1111-4111-8111-111111111111"]
    assert len(u1) == 2
    assert any(t.get("state_name") == "In Progress" for t in u1)
    assert any(t.get("state_name") == "Done" for t in u1)
    other = [t for t in store["linear_transitions"] if t.get("issue_id") == "other"]
    assert len(other) == 1
    assert out["from_completed_overlay"] == 1
    assert out["from_assigned"] == 0
    assert out["from_metrics_store"] == 0


def test_run_linear_dwell_backfill_assigned_when_overlay_empty(tmp_path):
    store_path = tmp_path / "ms.json"
    metrics_module.write_metrics_store(
        {
            "version": 1,
            "linear_transitions": [],
            "linear_last_sample": {},
            "github_prs": {},
        },
        store_path,
    )
    completed: dict = {}
    assigned = [
        {
            "id": "b2222222-2222-4222-8222-222222222222",
            "identifier": "PK-15",
            "createdAt": "2025-02-10T12:00:00.000Z",
            "state": {"id": "ip", "name": "In Progress", "type": "started"},
        }
    ]

    def linear_fn(query, variables):
        if variables.get("issueId"):
            return {
                "issues": {
                    "nodes": [
                        {
                            "id": "b2222222-2222-4222-8222-222222222222",
                            "history": {
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                                "nodes": [
                                    {
                                        "createdAt": "2025-02-10T12:00:00.000Z",
                                        "fromState": {"id": "todo", "name": "Todo", "type": "unstarted"},
                                        "toState": {
                                            "id": "ip",
                                            "name": "In Progress",
                                            "type": "started",
                                        },
                                    },
                                ],
                            },
                        }
                    ]
                }
            }
        raise AssertionError("unexpected query")

    out = metrics_module.run_linear_dwell_backfill(
        completed,
        linear_fn,
        "Done",
        path=store_path,
        assigned_nodes=assigned,
    )
    assert out["from_assigned"] == 1
    assert out["from_completed_overlay"] == 0
    assert out.get("from_metrics_store", 0) == 0
    assert "PK-15" in out["processed"]
    store = metrics_module.read_metrics_store(store_path)
    u1 = [t for t in store["linear_transitions"] if t.get("issue_id") == "b2222222-2222-4222-8222-222222222222"]
    assert len(u1) == 2
    assert u1[0]["t"] == "2025-02-10T12:00:00.000Z"
    assert u1[0]["state_name"] == "Todo"
