"""Unit tests for Linear data parsing and merging logic."""

import pytest
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
