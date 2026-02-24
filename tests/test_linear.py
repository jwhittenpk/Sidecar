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
