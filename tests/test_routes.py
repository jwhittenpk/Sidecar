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


# --- apply_issue_filters and API filter param tests ---

def _merged_issues_fixture():
    """List of merged issues for filter tests (varied updated_at, status, priority, personal fields)."""
    return [
        {
            "id": "u1", "identifier": "LIN-1", "title": "One",
            "linear_status": "In Progress", "linear_priority": 2,
            "updated_at": "2025-02-10T10:00:00Z", "personal_priority": 1,
            "personal_status": "Blocked", "last_updated": "2025-02-09T12:00:00Z",
            "is_completed": False,
        },
        {
            "id": "u2", "identifier": "LIN-2", "title": "Two",
            "linear_status": "Done", "linear_priority": 1,
            "updated_at": "2025-02-20T10:00:00Z", "personal_priority": None,
            "personal_status": "", "last_updated": None,
            "is_completed": False,
        },
        {
            "id": "u3", "identifier": "LIN-3", "title": "Three",
            "linear_status": "Todo", "linear_priority": 4,
            "updated_at": "2025-02-25T10:00:00Z", "personal_priority": 2,
            "personal_status": "In Progress", "last_updated": "2025-02-24T10:00:00Z",
            "is_completed": False,
        },
        {
            "id": "u4", "identifier": "LIN-4", "title": "Four",
            "linear_status": "In Progress", "linear_priority": 0,
            "updated_at": "2025-02-15T10:00:00Z", "personal_priority": None,
            "personal_status": "Ready to Close", "last_updated": None,
            "is_completed": False,
        },
    ]


def test_apply_issue_filters_empty_config_returns_all():
    """apply_issue_filters with empty config returns all issues."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {})
    assert len(result) == len(issues)
    result = app_module.apply_issue_filters(issues, None)
    assert len(result) == len(issues)


def test_apply_issue_filters_date_from():
    """Filter by date_from: issues updated on or after that date."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"date_from": "2025-02-20"})
    assert len(result) == 2
    ids = {i["identifier"] for i in result}
    assert "LIN-2" in ids
    assert "LIN-3" in ids
    assert "LIN-1" not in ids


def test_apply_issue_filters_date_to():
    """Filter by date_to: issues updated on or before that date."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"date_to": "2025-02-15"})
    assert len(result) == 2
    ids = {i["identifier"] for i in result}
    assert "LIN-1" in ids
    assert "LIN-4" in ids


def test_apply_issue_filters_date_range_both():
    """Filter by date_from and date_to: inclusive range."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"date_from": "2025-02-12", "date_to": "2025-02-22"})
    assert len(result) == 2
    ids = {i["identifier"] for i in result}
    assert "LIN-2" in ids
    assert "LIN-4" in ids
    assert "LIN-1" not in ids
    assert "LIN-3" not in ids


def test_apply_issue_filters_date_neither():
    """No date filter when neither date_from nor date_to set."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"linear_statuses": ["In Progress"]})
    assert len(result) == 2
    result2 = app_module.apply_issue_filters(issues, {})
    assert len(result2) == 4


def test_apply_issue_filters_linear_status_single():
    """Filter by single Linear status."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"linear_statuses": ["Done"]})
    assert len(result) == 1
    assert result[0]["identifier"] == "LIN-2"


def test_apply_issue_filters_linear_status_multiple():
    """Filter by multiple Linear statuses."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"linear_statuses": ["In Progress", "Todo"]})
    assert len(result) == 3
    ids = {i["identifier"] for i in result}
    assert "LIN-1" in ids
    assert "LIN-3" in ids
    assert "LIN-4" in ids
    assert "LIN-2" not in ids


def test_apply_issue_filters_linear_status_none_selected():
    """No status filter when list empty."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"linear_statuses": []})
    assert len(result) == 4


def test_apply_issue_filters_linear_priority_single():
    """Filter by single Linear priority."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"linear_priorities": [1]})
    assert len(result) == 1
    assert result[0]["identifier"] == "LIN-2"


def test_apply_issue_filters_linear_priority_multiple():
    """Filter by multiple Linear priorities."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"linear_priorities": [0, 2]})
    assert len(result) == 2
    ids = {i["identifier"] for i in result}
    assert "LIN-1" in ids
    assert "LIN-4" in ids


def test_apply_issue_filters_personal_priority_set():
    """Filter personal_priority_filter=set: only issues with personal_priority set."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"personal_priority_filter": "set"})
    assert len(result) == 2
    ids = {i["identifier"] for i in result}
    assert "LIN-1" in ids
    assert "LIN-3" in ids


def test_apply_issue_filters_personal_priority_unset():
    """Filter personal_priority_filter=unset: only issues with no personal_priority."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"personal_priority_filter": "unset"})
    assert len(result) == 2
    ids = {i["identifier"] for i in result}
    assert "LIN-2" in ids
    assert "LIN-4" in ids


def test_apply_issue_filters_personal_priority_all():
    """personal_priority_filter=all applies no filter."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"personal_priority_filter": "all"})
    assert len(result) == 4


def test_apply_issue_filters_personal_status():
    """Filter by personal status (one or more)."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {"personal_statuses": ["Blocked"]})
    assert len(result) == 1
    assert result[0]["identifier"] == "LIN-1"
    result2 = app_module.apply_issue_filters(issues, {"personal_statuses": ["", "In Progress"]})
    assert len(result2) == 2
    ids = {i["identifier"] for i in result2}
    assert "LIN-2" in ids  # No status (empty string)
    assert "LIN-3" in ids


def test_apply_issue_filters_personal_status_new_statuses():
    """Filtering by each of the three new personal statuses returns the correct issues."""
    issues = [
        {"id": "u1", "identifier": "LIN-1", "title": "A", "linear_status": "Todo", "linear_priority": 2,
         "personal_status": "Testing", "personal_priority": None, "last_updated": None, "is_completed": False},
        {"id": "u2", "identifier": "LIN-2", "title": "B", "linear_status": "Todo", "linear_priority": 2,
         "personal_status": "Pair Testing", "personal_priority": None, "last_updated": None, "is_completed": False},
        {"id": "u3", "identifier": "LIN-3", "title": "C", "linear_status": "Todo", "linear_priority": 2,
         "personal_status": "Waiting on Testing", "personal_priority": None, "last_updated": None, "is_completed": False},
    ]
    result_testing = app_module.apply_issue_filters(issues, {"personal_statuses": ["Testing"]})
    assert len(result_testing) == 1
    assert result_testing[0]["identifier"] == "LIN-1"
    result_pair = app_module.apply_issue_filters(issues, {"personal_statuses": ["Pair Testing"]})
    assert len(result_pair) == 1
    assert result_pair[0]["identifier"] == "LIN-2"
    result_waiting = app_module.apply_issue_filters(issues, {"personal_statuses": ["Waiting on Testing"]})
    assert len(result_waiting) == 1
    assert result_waiting[0]["identifier"] == "LIN-3"


def test_apply_issue_filters_and_logic():
    """Multiple filters combined with AND: only issues matching all criteria."""
    issues = _merged_issues_fixture()
    # LIN-1: updated 2025-02-10, In Progress, personal_priority set -> use date_from 2025-02-09 to include it
    result = app_module.apply_issue_filters(issues, {
        "date_from": "2025-02-09",
        "linear_statuses": ["In Progress"],
        "personal_priority_filter": "set",
    })
    assert len(result) == 1
    assert result[0]["identifier"] == "LIN-1"


def test_apply_issue_filters_no_matches_returns_empty():
    """Filter combination that matches no issues returns empty list, not error."""
    issues = _merged_issues_fixture()
    result = app_module.apply_issue_filters(issues, {
        "linear_statuses": ["Done"],
        "personal_priority_filter": "set",
    })
    assert len(result) == 0
    assert isinstance(result, list)


def test_get_api_issues_filter_date_from(mock_linear_fetch):
    """GET /api/issues?date_from=... returns only issues updated on or after date."""
    mock_linear_fetch.return_value = [
        {"id": "u1", "identifier": "LIN-1", "title": "A", "linear_priority": 2, "updated_at": "2025-02-10T10:00:00Z", "linear_status": "X", "is_completed": False},
        {"id": "u2", "identifier": "LIN-2", "title": "B", "linear_priority": 2, "updated_at": "2025-02-20T10:00:00Z", "linear_status": "Y", "is_completed": False},
    ]
    client = app_module.app.test_client()
    resp = client.get("/api/issues?filter=active&date_from=2025-02-15")
    assert resp.status_code == 200
    issues = resp.get_json()["issues"]
    assert len(issues) == 1
    assert issues[0]["identifier"] == "LIN-2"


def test_get_api_issues_filter_linear_status(mock_linear_fetch):
    """GET /api/issues?linear_status=In Progress returns only that status."""
    mock_linear_fetch.return_value = [
        {"id": "u1", "identifier": "LIN-1", "title": "A", "linear_priority": 2, "linear_status": "In Progress", "is_completed": False},
        {"id": "u2", "identifier": "LIN-2", "title": "B", "linear_priority": 2, "linear_status": "Done", "is_completed": False},
    ]
    client = app_module.app.test_client()
    resp = client.get("/api/issues?filter=active&linear_status=In%20Progress")
    assert resp.status_code == 200
    issues = resp.get_json()["issues"]
    assert len(issues) == 1
    assert issues[0]["identifier"] == "LIN-1"


def test_get_api_issues_filter_linear_priority(mock_linear_fetch):
    """GET /api/issues?linear_priority=1 returns only that priority."""
    mock_linear_fetch.return_value = [
        {"id": "u1", "identifier": "LIN-1", "title": "A", "linear_priority": 1, "linear_status": "X", "is_completed": False},
        {"id": "u2", "identifier": "LIN-2", "title": "B", "linear_priority": 2, "linear_status": "X", "is_completed": False},
    ]
    client = app_module.app.test_client()
    resp = client.get("/api/issues?filter=active&linear_priority=1")
    assert resp.status_code == 200
    issues = resp.get_json()["issues"]
    assert len(issues) == 1
    assert issues[0]["identifier"] == "LIN-1"


def test_get_api_issues_filter_personal_priority_set(mock_linear_fetch, temp_overlay_path):
    """GET /api/issues?personal_priority_filter=set returns only issues with personal priority."""
    mock_linear_fetch.return_value = [
        {"id": "u1", "identifier": "LIN-1", "title": "A", "linear_priority": 2, "linear_status": "X", "is_completed": False},
        {"id": "u2", "identifier": "LIN-2", "title": "B", "linear_priority": 2, "linear_status": "X", "is_completed": False},
    ]
    temp_overlay_path.write_text(json.dumps({"LIN-1": {"personal_priority": 1}}))
    client = app_module.app.test_client()
    resp = client.get("/api/issues?filter=active&personal_priority_filter=set")
    assert resp.status_code == 200
    issues = resp.get_json()["issues"]
    assert len(issues) == 1
    assert issues[0]["identifier"] == "LIN-1"


def test_get_api_issues_filter_no_matches_returns_200_empty_list(mock_linear_fetch):
    """When filters match no issues, API returns 200 with issues: []."""
    mock_linear_fetch.return_value = [
        {"id": "u1", "identifier": "LIN-1", "title": "A", "linear_priority": 2, "linear_status": "Todo", "is_completed": False},
    ]
    client = app_module.app.test_client()
    resp = client.get("/api/issues?filter=active&linear_status=Done")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["issues"] == []
    assert "last_fetched" in data


def test_get_api_personal_status_options_includes_new_statuses_in_order():
    """GET /api/personal-status-options returns all statuses including new ones in correct display order."""
    client = app_module.app.test_client()
    resp = client.get("/api/personal-status-options")
    assert resp.status_code == 200
    opts = resp.get_json()
    assert isinstance(opts, list)
    assert "Testing" in opts
    assert "Pair Testing" in opts
    assert "Waiting on Testing" in opts
    idx_in_progress = opts.index("In Progress")
    idx_testing = opts.index("Testing")
    idx_pair = opts.index("Pair Testing")
    idx_waiting_testing = opts.index("Waiting on Testing")
    idx_waiting_someone = opts.index("Waiting On Someone")
    assert idx_testing > idx_in_progress
    assert idx_pair > idx_testing
    assert idx_waiting_testing > idx_pair
    assert idx_waiting_someone > idx_waiting_testing


def test_post_api_overlay_invalid_personal_status_returns_400(temp_overlay_path, mock_linear_fetch):
    """POST /api/overlay/<id> with invalid personal_status returns 400 and error message."""
    client = app_module.app.test_client()
    resp = client.post(
        "/api/overlay/LIN-1",
        data=json.dumps({"personal_status": "Invalid Status"}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data
    assert "personal_status" in data["error"].lower() or "invalid" in data["error"].lower()


# --- GET/POST /api/config/columns ---


def test_get_api_config_columns_returns_registry_order_visibility(temp_overlay_path):
    """GET /api/config/columns returns column registry, order, and visibility (fresh install)."""
    client = app_module.app.test_client()
    resp = client.get("/api/config/columns")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "columns" in data
    assert "order" in data
    assert "visibility" in data
    columns = data["columns"]
    assert isinstance(columns, list)
    assert len(columns) >= 9
    ids = [c["id"] for c in columns]
    assert "identifier" in ids
    assert "title" in ids
    assert "linear_status" in ids
    assert "cycle" in ids
    assert "team" in ids
    assert "labels" in ids
    order = data["order"]
    assert isinstance(order, list)
    assert set(order) == set(ids)
    vis = data["visibility"]
    assert vis.get("identifier") is True
    assert vis.get("title") is True
    assert vis.get("cycle") is False
    assert vis.get("team") is False
    assert vis.get("labels") is False


def test_post_api_config_columns_persists_order_and_visibility(temp_overlay_path):
    """POST /api/config/columns with valid order and visibility persists and returns them."""
    client = app_module.app.test_client()
    order = [c["id"] for c in app_module.COLUMN_REGISTRY]
    order = [order[1], order[0]] + order[2:]
    new_vis = {
        "identifier": True,
        "title": True,
        "linear_status": True,
        "linear_priority": True,
        "personal_priority": True,
        "personal_status": True,
        "notes_preview": True,
        "linear_updated": True,
        "my_last_edit": True,
        "cycle": True,
        "team": False,
        "labels": False,
    }
    resp = client.post(
        "/api/config/columns",
        data=json.dumps({"order": order, "visibility": new_vis}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["order"] == order
    assert data["visibility"].get("cycle") is True
    assert temp_overlay_path.exists()
    with open(temp_overlay_path, encoding="utf-8") as f:
        overlay = json.load(f)
    assert app_module.COLUMN_PREFERENCES_KEY in overlay
    assert overlay[app_module.COLUMN_PREFERENCES_KEY]["order"] == order
    assert overlay[app_module.COLUMN_PREFERENCES_KEY]["visibility"]["cycle"] is True


def test_post_api_config_columns_duplicate_order_returns_400(temp_overlay_path):
    """POST /api/config/columns with duplicate column ID in order returns 400."""
    client = app_module.app.test_client()
    order = [c["id"] for c in app_module.COLUMN_REGISTRY]
    order = list(order)
    order[0] = order[1]
    vis = {c["id"]: c["default_visible"] for c in app_module.COLUMN_REGISTRY}
    resp = client.post(
        "/api/config/columns",
        data=json.dumps({"order": order, "visibility": vis}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_post_api_config_columns_missing_order_returns_400(temp_overlay_path):
    """POST /api/config/columns with missing column ID in order returns 400."""
    client = app_module.app.test_client()
    order = [c["id"] for c in app_module.COLUMN_REGISTRY][1:]
    vis = {c["id"]: c["default_visible"] for c in app_module.COLUMN_REGISTRY}
    resp = client.post(
        "/api/config/columns",
        data=json.dumps({"order": order, "visibility": vis}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_post_api_config_columns_rejects_hiding_identifier(temp_overlay_path):
    """POST /api/config/columns with identifier false returns 400."""
    client = app_module.app.test_client()
    order = [c["id"] for c in app_module.COLUMN_REGISTRY]
    vis = {c["id"]: c["default_visible"] for c in app_module.COLUMN_REGISTRY}
    vis["identifier"] = False
    resp = client.post(
        "/api/config/columns",
        data=json.dumps({"order": order, "visibility": vis}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data
    assert "identifier" in data["error"].lower()


def test_post_api_config_columns_rejects_hiding_title(temp_overlay_path):
    """POST /api/config/columns with title false returns 400."""
    client = app_module.app.test_client()
    order = [c["id"] for c in app_module.COLUMN_REGISTRY]
    vis = {c["id"]: c["default_visible"] for c in app_module.COLUMN_REGISTRY}
    vis["title"] = False
    resp = client.post(
        "/api/config/columns",
        data=json.dumps({"order": order, "visibility": vis}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data
    assert "title" in data["error"].lower()


def test_post_api_config_columns_rejects_only_identifier_title_visible(temp_overlay_path):
    """POST /api/config/columns that would leave only identifier and title visible returns 400."""
    client = app_module.app.test_client()
    order = [c["id"] for c in app_module.COLUMN_REGISTRY]
    only_two = {
        c["id"]: (c["id"] in ("identifier", "title"))
        for c in app_module.COLUMN_REGISTRY
    }
    resp = client.post(
        "/api/config/columns",
        data=json.dumps({"order": order, "visibility": only_two}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_filter_order_matches_column_order_after_reorder(temp_overlay_path):
    """After POST with custom order, GET returns that order (filter popover can follow column order)."""
    client = app_module.app.test_client()
    order = [c["id"] for c in app_module.COLUMN_REGISTRY]
    order = [order[2], order[0], order[1]] + order[3:]
    vis = {c["id"]: c["default_visible"] for c in app_module.COLUMN_REGISTRY}
    resp = client.post(
        "/api/config/columns",
        data=json.dumps({"order": order, "visibility": vis}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    resp2 = client.get("/api/config/columns")
    assert resp2.status_code == 200
    data = resp2.get_json()
    assert data["order"] == order
