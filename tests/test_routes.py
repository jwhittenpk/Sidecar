"""Unit tests for Flask routes."""

import json
import pytest
from unittest.mock import patch, MagicMock

import app as app_module


@pytest.fixture(autouse=True)
def reset_cache():
    """Reset in-memory cache before each test so mocks apply."""
    app_module._issues_cache = None
    app_module._last_fetched = None
    yield
    app_module._issues_cache = None
    app_module._last_fetched = None


@pytest.fixture
def mock_linear_fetch():
    """Mock fetch_linear_issues to return a fixed list (no real API calls)."""
    with patch.object(app_module, "fetch_linear_issues") as m:
        m.return_value = [
            {
                "id": "uuid-1",
                "identifier": "LIN-1",
                "title": "Test issue",
                "linear_status": "In Progress",
                "linear_priority": 2,
                "url": "https://linear.app/issue/LIN-1",
                "team_name": "Eng",
                "updated_at": "2025-02-20T10:00:00Z",
                "is_completed": False,
            }
        ]
        yield m


@pytest.fixture
def temp_overlay_path(tmp_path):
    """Use temp overlay file for route tests that write overlay."""
    path = tmp_path / "overlay.json"
    original = app_module.OVERLAY_PATH
    app_module.OVERLAY_PATH = path
    yield path
    app_module.OVERLAY_PATH = original


def test_get_index_returns_200_and_html():
    """GET / returns 200 and HTML content."""
    client = app_module.app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.content_type
    assert b"My Linear Dashboard" in resp.data or b"Linear" in resp.data


def test_get_api_issues_returns_json_with_expected_shape(mock_linear_fetch):
    """GET /api/issues returns JSON with issues list and last_fetched; each issue has expected keys."""
    client = app_module.app.test_client()
    resp = client.get("/api/issues")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "issues" in data
    assert "last_fetched" in data
    issues = data["issues"]
    assert isinstance(issues, list)
    assert len(issues) == 1
    issue = issues[0]
    for key in ("identifier", "title", "linear_status", "linear_priority", "personal_priority", "notes"):
        assert key in issue


def test_get_api_issues_when_fetch_fails_returns_400():
    """GET /api/issues when fetch_linear_issues raises (e.g. no token) returns 400 with error message."""
    app_module._issues_cache = None
    with patch.object(app_module, "fetch_linear_issues", side_effect=ValueError("LINEAR_GRAPHQL_API is not set")):
        client = app_module.app.test_client()
        resp = client.get("/api/issues")
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data


def test_post_api_refresh_returns_updated_issues(mock_linear_fetch):
    """POST /api/refresh triggers fetch and returns issues with last_fetched."""
    client = app_module.app.test_client()
    resp = client.post("/api/refresh")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "issues" in data
    assert "last_fetched" in data
    assert len(data["issues"]) == 1
    mock_linear_fetch.assert_called()


def test_post_api_overlay_saves_and_returns_success(temp_overlay_path, mock_linear_fetch):
    """POST /api/overlay/<issue_id> with valid body saves to overlay and returns 200."""
    client = app_module.app.test_client()
    resp = client.post(
        "/api/overlay/LIN-99",
        data=json.dumps({"personal_priority": 1, "personal_status": "Blocked", "notes": "My note"}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("ok") is True
    assert "entry" in data
    assert data["entry"]["notes"] == "My note"
    assert data["entry"]["personal_status"] == "Blocked"
    assert data["entry"]["personal_priority"] == 1
    assert temp_overlay_path.exists()
    with open(temp_overlay_path, encoding="utf-8") as f:
        overlay = json.load(f)
    assert "LIN-99" in overlay
    assert overlay["LIN-99"]["notes"] == "My note"


def test_post_api_overlay_whitespace_issue_id_returns_400():
    """POST /api/overlay/ with only-whitespace issue_id returns 400 (overlay key becomes empty)."""
    client = app_module.app.test_client()
    resp = client.post(
        "/api/overlay/   ",
        data=json.dumps({"notes": "x"}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    assert resp.get_json().get("error") == "Invalid issue_id"


def test_get_api_issues_filter_active_returns_only_non_completed(mock_linear_fetch):
    """GET /api/issues?filter=active returns only issues with is_completed false."""
    mock_linear_fetch.return_value = [
        {"id": "u1", "identifier": "LIN-1", "title": "Active", "linear_priority": 2, "is_completed": False},
        {"id": "u2", "identifier": "LIN-2", "title": "Done", "linear_priority": 2, "is_completed": True},
    ]
    client = app_module.app.test_client()
    resp = client.get("/api/issues?filter=active")
    assert resp.status_code == 200
    issues = resp.get_json()["issues"]
    assert len(issues) == 1
    assert issues[0]["identifier"] == "LIN-1"
    assert issues[0]["is_completed"] is False


def test_get_api_issues_filter_completed_returns_only_completed(mock_linear_fetch):
    """GET /api/issues?filter=completed returns only completed/cancelled issues."""
    mock_linear_fetch.return_value = [
        {"id": "u1", "identifier": "LIN-1", "title": "Active", "linear_priority": 2, "is_completed": False},
        {"id": "u2", "identifier": "LIN-2", "title": "Done", "linear_priority": 2, "is_completed": True},
    ]
    client = app_module.app.test_client()
    resp = client.get("/api/issues?filter=completed")
    assert resp.status_code == 200
    issues = resp.get_json()["issues"]
    assert len(issues) == 1
    assert issues[0]["identifier"] == "LIN-2"
    assert issues[0]["is_completed"] is True


def test_post_overlay_conflicting_priority_triggers_rebalancing(temp_overlay_path, mock_linear_fetch):
    """POST /api/overlay/<id> with a priority that another issue has triggers insert-mode rebalancing."""
    client = app_module.app.test_client()
    client.post(
        "/api/overlay/LIN-1",
        data=json.dumps({"personal_priority": 1, "notes": "first"}),
        content_type="application/json",
    )
    resp = client.post(
        "/api/overlay/LIN-2",
        data=json.dumps({"personal_priority": 1, "notes": "new first"}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("ok") is True
    assert data["entry"]["personal_priority"] == 1
    assert "overlay" in data
    assert data["overlay"]["LIN-1"]["personal_priority"] == 2
    assert data["overlay"]["LIN-2"]["personal_priority"] == 1


def test_post_overlay_priority_response_has_full_rebalanced_overlay(temp_overlay_path, mock_linear_fetch):
    """Response after a priority update reflects the full rebalanced priority list."""
    client = app_module.app.test_client()
    client.post(
        "/api/overlay/LIN-1",
        data=json.dumps({"personal_priority": 1}),
        content_type="application/json",
    )
    client.post(
        "/api/overlay/LIN-2",
        data=json.dumps({"personal_priority": 2}),
        content_type="application/json",
    )
    resp = client.post(
        "/api/overlay/LIN-3",
        data=json.dumps({"personal_priority": 2}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "overlay" in data
    overlay = data["overlay"]
    assert overlay["LIN-3"]["personal_priority"] == 2
    assert overlay["LIN-2"]["personal_priority"] == 3
    assert overlay["LIN-1"]["personal_priority"] == 1


def test_post_overlay_rebalance_writes_once(temp_overlay_path, mock_linear_fetch):
    """When POST causes rebalancing, write_overlay is called once."""
    from unittest.mock import patch
    client = app_module.app.test_client()
    client.post(
        "/api/overlay/LIN-1",
        data=json.dumps({"personal_priority": 1}),
        content_type="application/json",
    )
    with patch.object(app_module, "write_overlay") as mock_write:
        resp = client.post(
            "/api/overlay/LIN-2",
            data=json.dumps({"personal_priority": 1}),
            content_type="application/json",
        )
    assert resp.status_code == 200
    mock_write.assert_called_once()
