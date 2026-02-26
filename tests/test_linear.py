"""Unit tests for Linear data parsing and merging logic."""

import pytest
from pathlib import Path
from unittest.mock import patch

import app as app_module


# Sample raw node as returned by Linear GraphQL issues.nodes
LINEAR_NODE = {
    "id": "abc-123-uuid",
    "identifier": "LIN-42",
    "title": "Fix the bug",
    "url": "https://linear.app/team/issue/LIN-42",
    "priority": 2,
    "updatedAt": "2025-02-20T10:30:00.000Z",
    "state": {"name": "In Progress", "type": "started"},
    "team": {"name": "Engineering"},
}


def test_normalize_issue_shape():
    """Raw Linear API node is parsed into expected internal shape."""
    normalized = app_module._normalize_issue(LINEAR_NODE)
    assert normalized["id"] == "abc-123-uuid"
    assert normalized["identifier"] == "LIN-42"
    assert normalized["title"] == "Fix the bug"
    assert normalized["linear_status"] == "In Progress"
    assert normalized["linear_priority"] == 2
    assert normalized["url"] == "https://linear.app/team/issue/LIN-42"
    assert normalized["team_name"] == "Engineering"
    assert normalized["updated_at"] == "2025-02-20T10:30:00.000Z"
    assert normalized["is_completed"] is False
    assert normalized.get("cycle") is None


def test_normalize_completed_state():
    """State type completed/canceled sets is_completed True."""
    completed_node = {**LINEAR_NODE, "state": {"name": "Done", "type": "completed"}}
    normalized = app_module._normalize_issue(completed_node)
    assert normalized["is_completed"] is True
    assert normalized["linear_status"] == "Done"


def test_normalize_handles_missing_fields():
    """Missing optional fields get safe defaults."""
    minimal = {"id": "x", "identifier": "LIN-1", "title": "T"}
    normalized = app_module._normalize_issue(minimal)
    assert normalized["linear_status"] == ""
    assert normalized["linear_priority"] == 0
    assert normalized["url"] == ""
    assert normalized["team_name"] == ""
    assert normalized["updated_at"] == ""
    assert normalized["is_completed"] is False
    assert normalized.get("cycle") is None


def test_merge_issues_with_overlay():
    """Linear issues merged with overlay have overlay fields applied."""
    linear_issues = [
        {"id": "u1", "identifier": "LIN-1", "title": "One", "linear_priority": 1},
        {"id": "u2", "identifier": "LIN-2", "title": "Two", "linear_priority": 2},
    ]
    overlay = {
        "LIN-1": {"personal_priority": 3, "personal_status": "Blocked", "notes": "Note 1", "last_updated": "2025-01-01"},
        "LIN-2": {"notes": "Note 2 only"},
    }
    merged = app_module.merge_issues(linear_issues, overlay)
    assert merged[0]["personal_priority"] == 3
    assert merged[0]["personal_status"] == "Blocked"
    assert merged[0]["notes"] == "Note 1"
    assert merged[0]["last_updated"] == "2025-01-01"
    assert merged[1]["personal_priority"] is None
    assert merged[1]["personal_status"] == ""
    assert merged[1]["notes"] == "Note 2 only"


def test_priority_label_mapping():
    """Priority 0-4 maps to correct label strings."""
    assert app_module.LINEAR_PRIORITY_LABELS[0] == "No priority"
    assert app_module.LINEAR_PRIORITY_LABELS[1] == "Urgent"
    assert app_module.LINEAR_PRIORITY_LABELS[2] == "High"
    assert app_module.LINEAR_PRIORITY_LABELS[3] == "Medium"
    assert app_module.LINEAR_PRIORITY_LABELS[4] == "Low"


def test_fetch_issues_page_uses_isMe_filter():
    """_fetch_issues_page uses assignee.isMe filter (no viewer_id / assigneeId). Prevents NameError and 400s."""
    with patch.object(app_module, "_linear_request") as mock_request:
        mock_request.return_value = {
            "issues": {
                "nodes": [],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
        app_module._fetch_issues_page("fake-token")
        mock_request.assert_called_once()
        args = mock_request.call_args[0]
        query = args[1]
        variables = args[2] if len(args) > 2 else {}
        # Query must use isMe (no separate viewer query / assigneeId)
        assert "isMe" in query
        assert "eq: true" in query
        # Must not require viewer_id or assigneeId variable
        assert "assigneeId" not in (variables or {})
        assert "$assigneeId" not in query


def test_normalize_issue_with_cycle():
    """When Linear node has cycle, normalized issue includes cycle dict."""
    node = {
        **LINEAR_NODE,
        "cycle": {
            "id": "cycle-1",
            "name": "Sprint 1",
            "number": 1,
            "startsAt": "2025-02-01T00:00:00.000Z",
            "endsAt": "2025-02-14T23:59:59.000Z",
        },
    }
    normalized = app_module._normalize_issue(node)
    assert normalized["cycle"] is not None
    assert normalized["cycle"]["id"] == "cycle-1"
    assert normalized["cycle"]["name"] == "Sprint 1"
    assert normalized["cycle"]["number"] == 1
    assert normalized["cycle"]["starts_at"] == "2025-02-01T00:00:00.000Z"
    assert normalized["cycle"]["ends_at"] == "2025-02-14T23:59:59.000Z"


def test_normalize_issue_with_labels():
    """Labels are parsed from Linear API response as list of {name, color}."""
    node = {
        **LINEAR_NODE,
        "labels": {
            "nodes": [
                {"id": "l1", "name": "bug", "color": "#d73a4a"},
                {"id": "l2", "name": "frontend", "color": "#0075ca"},
            ],
        },
    }
    normalized = app_module._normalize_issue(node)
    assert "labels" in normalized
    assert len(normalized["labels"]) == 2
    assert normalized["labels"][0]["name"] == "bug"
    assert normalized["labels"][0]["color"] == "#d73a4a"
    assert normalized["labels"][1]["name"] == "frontend"
    assert normalized["labels"][1]["color"] == "#0075ca"


def test_normalize_issue_labels_empty():
    """Missing or empty labels yields empty list."""
    normalized = app_module._normalize_issue(LINEAR_NODE)
    assert normalized.get("labels") == []


# --- sort_issues_by_cycle tests (use merged-issue shaped dicts with cycle, linear_priority, etc.)


def _issue(identifier, linear_priority=2, personal_priority=None, linear_status="In Progress", updated_at="2025-02-20T10:00:00Z", cycle=None):
    return {
        "id": identifier,
        "identifier": identifier,
        "title": identifier,
        "linear_priority": linear_priority,
        "personal_priority": personal_priority,
        "linear_status": linear_status,
        "updated_at": updated_at,
        "cycle": cycle,
    }


def test_cycle_sort_urgent_always_first():
    """Urgent tickets (linear_priority=1) appear first regardless of personal priority."""
    from datetime import datetime, timezone
    now = datetime(2025, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    issues = [
        _issue("LIN-2", linear_priority=2, personal_priority=1),
        _issue("LIN-1", linear_priority=1, personal_priority=2),
        _issue("LIN-3", linear_priority=3),
    ]
    result = app_module.sort_issues_by_cycle(issues, now_utc=now)
    assert result[0]["identifier"] == "LIN-1"
    assert result[0]["linear_priority"] == 1


def test_cycle_sort_personal_priority_before_current_cycle():
    """Non-Urgent issues with personal priority appear before current cycle tickets."""
    from datetime import datetime, timezone
    now = datetime(2025, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    current_cycle = {"id": "c1", "name": "Current", "number": 1, "starts_at": "2025-02-01T00:00:00Z", "ends_at": "2025-02-28T23:59:59Z"}
    issues = [
        _issue("LIN-cycle", linear_priority=2, cycle=current_cycle),
        _issue("LIN-pp", linear_priority=2, personal_priority=3, cycle=None),
    ]
    result = app_module.sort_issues_by_cycle(issues, now_utc=now)
    assert result[0]["identifier"] == "LIN-pp"
    assert result[1]["identifier"] == "LIN-cycle"


def test_cycle_sort_current_before_future():
    """Current cycle issues appear before future cycle issues."""
    from datetime import datetime, timezone
    now = datetime(2025, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    current = {"id": "c1", "name": "Current", "number": 1, "starts_at": "2025-02-01T00:00:00Z", "ends_at": "2025-02-28T23:59:59Z"}
    future = {"id": "f1", "name": "Future", "number": 2, "starts_at": "2025-03-01T00:00:00Z", "ends_at": "2025-03-14T23:59:59Z"}
    issues = [
        _issue("LIN-future", linear_priority=2, cycle=future),
        _issue("LIN-current", linear_priority=2, cycle=current),
    ]
    result = app_module.sort_issues_by_cycle(issues, now_utc=now)
    assert result[0]["identifier"] == "LIN-current"
    assert result[1]["identifier"] == "LIN-future"


def test_cycle_sort_within_cycle_status_order():
    """Within a cycle, status order is In Review → In Progress → Todo."""
    from datetime import datetime, timezone
    now = datetime(2025, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    cycle = {"id": "c1", "name": "C", "number": 1, "starts_at": "2025-02-01T00:00:00Z", "ends_at": "2025-02-28T23:59:59Z"}
    issues = [
        _issue("LIN-todo", linear_priority=2, linear_status="Todo", cycle=cycle),
        _issue("LIN-review", linear_priority=2, linear_status="In Review", cycle=cycle),
        _issue("LIN-progress", linear_priority=2, linear_status="In Progress", cycle=cycle),
    ]
    result = app_module.sort_issues_by_cycle(issues, now_utc=now)
    order = [r["linear_status"] for r in result]
    assert order == ["In Review", "In Progress", "Todo"]


def test_cycle_sort_within_status_linear_priority():
    """Within same status, Linear priority order is High → Medium → Low → No priority."""
    from datetime import datetime, timezone
    now = datetime(2025, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    cycle = {"id": "c1", "name": "C", "number": 1, "starts_at": "2025-02-01T00:00:00Z", "ends_at": "2025-02-28T23:59:59Z"}
    issues = [
        _issue("LIN-low", linear_priority=4, linear_status="In Progress", cycle=cycle),
        _issue("LIN-high", linear_priority=2, linear_status="In Progress", cycle=cycle),
        _issue("LIN-none", linear_priority=0, linear_status="In Progress", cycle=cycle),
        _issue("LIN-medium", linear_priority=3, linear_status="In Progress", cycle=cycle),
    ]
    result = app_module.sort_issues_by_cycle(issues, now_utc=now)
    order = [r["identifier"] for r in result]
    assert order == ["LIN-high", "LIN-medium", "LIN-low", "LIN-none"]


def test_cycle_sort_no_cycle_at_bottom():
    """Issues with no cycle are last, sorted by updated_at descending."""
    from datetime import datetime, timezone
    now = datetime(2025, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    cycle = {"id": "c1", "name": "C", "number": 1, "starts_at": "2025-02-01T00:00:00Z", "ends_at": "2025-02-28T23:59:59Z"}
    issues = [
        _issue("LIN-nocycle-old", linear_priority=2, updated_at="2025-02-01T10:00:00Z", cycle=None),
        _issue("LIN-current", linear_priority=2, cycle=cycle),
        _issue("LIN-nocycle-new", linear_priority=2, updated_at="2025-02-15T10:00:00Z", cycle=None),
    ]
    result = app_module.sort_issues_by_cycle(issues, now_utc=now)
    assert result[0]["identifier"] == "LIN-current"
    assert result[1]["identifier"] == "LIN-nocycle-new"
    assert result[2]["identifier"] == "LIN-nocycle-old"


def test_cycle_sort_future_cycles_chronological():
    """Future cycles appear in chronological order (nearest start first)."""
    from datetime import datetime, timezone
    now = datetime(2025, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    march = {"id": "m", "name": "March", "number": 3, "starts_at": "2025-03-01T00:00:00Z", "ends_at": "2025-03-14T23:59:59Z"}
    april = {"id": "a", "name": "April", "number": 4, "starts_at": "2025-04-01T00:00:00Z", "ends_at": "2025-04-14T23:59:59Z"}
    issues = [
        _issue("LIN-april", linear_priority=2, cycle=april),
        _issue("LIN-march", linear_priority=2, cycle=march),
    ]
    result = app_module.sort_issues_by_cycle(issues, now_utc=now)
    assert result[0]["identifier"] == "LIN-march"
    assert result[1]["identifier"] == "LIN-april"


def test_fetch_all_assigned_issues_runs_without_viewer_id():
    """_fetch_all_assigned_issues completes without NameError; uses only token and pagination (no viewer_id)."""
    with patch.object(app_module, "_linear_request") as mock_request:
        mock_request.return_value = {
            "issues": {
                "nodes": [
                    {
                        "id": "id-1",
                        "identifier": "LIN-1",
                        "title": "One",
                        "url": "https://linear.app/LIN-1",
                        "priority": 1,
                        "updatedAt": "2025-02-20T10:00:00.000Z",
                        "state": {"name": "In Progress", "type": "started"},
                        "team": {"name": "Eng"},
                    }
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        }
        result = app_module._fetch_all_assigned_issues("fake-token")
        assert len(result) == 1
        assert result[0]["identifier"] == "LIN-1"
        mock_request.assert_called()
        # Every call must use query with isMe (no assigneeId)
        for call in mock_request.call_args_list:
            query = call[0][1]
            assert "isMe" in query, "Linear issues query must use assignee.isMe filter"
            assert "$assigneeId" not in query, "Must not use assigneeId (use isMe instead)"


def test_refresh_removes_priority_for_completed_and_rebalances(tmp_path):
    """On refresh, completed issues have their personal priority removed and list is rebalanced."""
    original_path = app_module.OVERLAY_PATH
    try:
        app_module.OVERLAY_PATH = tmp_path / "overlay.json"
        app_module.write_overlay({
            "LIN-1": {"personal_priority": 1, "notes": "a"},
            "LIN-2": {"personal_priority": 2, "notes": "b"},
            "LIN-3": {"personal_priority": 3, "notes": "c"},
        })
        linear_issues = [
            {"id": "u1", "identifier": "LIN-1", "title": "Active", "is_completed": False},
            {"id": "u2", "identifier": "LIN-2", "title": "Done", "is_completed": True},
            {"id": "u3", "identifier": "LIN-3", "title": "Active2", "is_completed": False},
        ]
        with patch.object(app_module, "fetch_linear_issues", return_value=linear_issues):
            app_module.refresh_cache()
        overlay = app_module.read_overlay()
        assert overlay.get("LIN-2", {}).get("personal_priority") is None
        assert overlay["LIN-1"]["personal_priority"] == 1
        assert overlay["LIN-3"]["personal_priority"] == 2
    finally:
        app_module.OVERLAY_PATH = original_path


def test_personal_priority_sort_unranked_use_cycle_order():
    """When sorted by personal_priority, issues with no priority appear after ranked, in cycle order."""
    # Use a cycle spanning a wide range so it is "current" regardless of test run date
    cycle = {"id": "c1", "name": "C", "number": 1, "starts_at": "2020-01-01T00:00:00Z", "ends_at": "2030-12-31T23:59:59Z"}
    issues = [
        _issue("LIN-noprio-todo", linear_status="Todo", personal_priority=None, cycle=cycle),
        _issue("LIN-ranked", personal_priority=1),
        _issue("LIN-noprio-review", linear_status="In Review", personal_priority=None, cycle=cycle),
    ]
    result = app_module._apply_sort(issues, "personal_priority")
    assert result[0]["identifier"] == "LIN-ranked"
    assert result[0]["personal_priority"] == 1
    unranked = result[1:]
    assert len(unranked) == 2
    # Unranked sorted by cycle: In Review before Todo
    assert unranked[0]["linear_status"] == "In Review"
    assert unranked[1]["linear_status"] == "Todo"


def _issue_with_overlay(identifier, linear_priority=2, personal_priority=None, linear_status="In Progress",
                        updated_at="2025-02-20T10:00:00Z", last_updated=None, personal_status=""):
    """Merged-issue shape for sort tests (includes last_updated, personal_status)."""
    i = _issue(identifier, linear_priority=linear_priority, personal_priority=personal_priority,
               linear_status=linear_status, updated_at=updated_at)
    i["last_updated"] = last_updated
    i["personal_status"] = personal_status
    return i


def test_sort_linear_status_ascending():
    """Sort by linear_status ascending (A-Z alphabetical)."""
    issues = [
        _issue_with_overlay("LIN-c", linear_status="Done"),
        _issue_with_overlay("LIN-a", linear_status="Backlog"),
        _issue_with_overlay("LIN-b", linear_status="In Progress"),
    ]
    result = app_module._apply_sort(issues, "linear_status")
    # Alphabetical: Backlog < Done < In Progress
    assert [r["identifier"] for r in result] == ["LIN-a", "LIN-c", "LIN-b"]
    assert [r["linear_status"] for r in result] == ["Backlog", "Done", "In Progress"]


def test_sort_linear_priority_urgent_to_no_priority():
    """Linear priority sort: Urgent(1) → High(2) → Medium(3) → Low(4) → No Priority(0), not alphabetical."""
    issues = [
        _issue_with_overlay("LIN-low", linear_priority=4),
        _issue_with_overlay("LIN-urgent", linear_priority=1),
        _issue_with_overlay("LIN-none", linear_priority=0),
        _issue_with_overlay("LIN-high", linear_priority=2),
        _issue_with_overlay("LIN-medium", linear_priority=3),
    ]
    result = app_module._apply_sort(issues, "linear_priority")
    order = [r["identifier"] for r in result]
    assert order == ["LIN-urgent", "LIN-high", "LIN-medium", "LIN-low", "LIN-none"]


def test_sort_personal_priority_unset_last_ascending():
    """Personal priority sort ascending: unset issues appear last."""
    issues = [
        _issue_with_overlay("LIN-unset", personal_priority=None),
        _issue_with_overlay("LIN-2", personal_priority=2),
        _issue_with_overlay("LIN-1", personal_priority=1),
    ]
    result = app_module._apply_sort(issues, "personal_priority")
    assert result[0]["personal_priority"] == 1
    assert result[1]["personal_priority"] == 2
    assert result[2]["personal_priority"] is None


def test_sort_personal_status_ascending():
    """Sort by personal_status ascending (display order from PERSONAL_STATUS_OPTIONS)."""
    issues = [
        _issue_with_overlay("LIN-b", personal_status="Blocked"),
        _issue_with_overlay("LIN-a", personal_status=""),
        _issue_with_overlay("LIN-c", personal_status="In Progress"),
    ]
    result = app_module._apply_sort(issues, "personal_status")
    # Display order: "" (No Status) < In Progress < Blocked
    assert [r["personal_status"] for r in result] == ["", "In Progress", "Blocked"]


def test_sort_updated_at_descending():
    """Sort by updated_at: default is newest first (reverse=True in backend)."""
    issues = [
        _issue_with_overlay("LIN-old", updated_at="2025-02-01T10:00:00Z"),
        _issue_with_overlay("LIN-new", updated_at="2025-02-25T10:00:00Z"),
    ]
    result = app_module._apply_sort(issues, "updated_at")
    assert result[0]["identifier"] == "LIN-new"
    assert result[1]["identifier"] == "LIN-old"


def test_sort_last_updated_ascending_no_edit_last():
    """My Last Edit sort ascending: oldest first, no edit last."""
    issues = [
        _issue_with_overlay("LIN-no-edit", last_updated=None),
        _issue_with_overlay("LIN-old", last_updated="2025-02-01T10:00:00Z"),
        _issue_with_overlay("LIN-new", last_updated="2025-02-25T10:00:00Z"),
    ]
    result = app_module._apply_sort(issues, "last_updated", "asc")
    assert result[0]["identifier"] == "LIN-old"
    assert result[1]["identifier"] == "LIN-new"
    assert result[2]["identifier"] == "LIN-no-edit"


def test_sort_last_updated_descending_no_edit_first():
    """My Last Edit sort descending: newest first, no edit first."""
    issues = [
        _issue_with_overlay("LIN-old", last_updated="2025-02-01T10:00:00Z"),
        _issue_with_overlay("LIN-no-edit", last_updated=None),
        _issue_with_overlay("LIN-new", last_updated="2025-02-25T10:00:00Z"),
    ]
    result = app_module._apply_sort(issues, "last_updated", "desc")
    assert result[0]["identifier"] == "LIN-new"
    assert result[1]["identifier"] == "LIN-old"
    assert result[2]["identifier"] == "LIN-no-edit"


def test_sort_cycle_by_name_no_cycle_at_bottom():
    """Sort by cycle column: alphabetical by cycle name; issues with no cycle sink to bottom."""
    issues = [
        _issue("LIN-nocycle", cycle=None),
        _issue("LIN-b", cycle={"id": "c2", "name": "Beta", "number": 2, "starts_at": None, "ends_at": None}),
        _issue("LIN-a", cycle={"id": "c1", "name": "Alpha", "number": 1, "starts_at": None, "ends_at": None}),
    ]
    result = app_module._apply_sort(issues, "cycle", "asc")
    assert result[0]["identifier"] == "LIN-a"
    assert (result[0].get("cycle") or {}).get("name") == "Alpha"
    assert result[1]["identifier"] == "LIN-b"
    assert result[2]["identifier"] == "LIN-nocycle"
    assert result[2].get("cycle") is None


def test_sort_labels_by_first_name_no_labels_at_bottom():
    """Sort by labels column: alphabetical by first label name; no-label issues at bottom."""
    i1 = _issue("LIN-nolabels")
    i1["labels"] = []
    i2 = _issue("LIN-b")
    i2["labels"] = [{"name": "zebra", "color": "#fff"}]
    i3 = _issue("LIN-a")
    i3["labels"] = [{"name": "alpha", "color": "#fff"}]
    issues = [i1, i2, i3]
    result = app_module._apply_sort(issues, "labels", "asc")
    assert result[0]["identifier"] == "LIN-a"
    assert result[1]["identifier"] == "LIN-b"
    assert result[2]["identifier"] == "LIN-nolabels"


def test_sort_team_alphabetical():
    """Sort by team column: alphabetical by team name."""
    issues = [
        _issue("LIN-c", linear_priority=2),
        _issue("LIN-a", linear_priority=2),
        _issue("LIN-b", linear_priority=2),
    ]
    # _issue doesn't set team_name; we need to add it
    for i, tid in zip(issues, ["Team C", "Team A", "Team B"]):
        i["team_name"] = tid
    result = app_module._apply_sort(issues, "team", "asc")
    assert [r["identifier"] for r in result] == ["LIN-a", "LIN-b", "LIN-c"]
    assert [r["team_name"] for r in result] == ["Team A", "Team B", "Team C"]
