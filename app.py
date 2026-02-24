"""
Sidecar — A personal overlay dashboard for Linear tickets.
Flask backend: read-only Linear API, local overlay.json for notes/priority/status.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

# Load .env from app directory so cwd doesn't matter (e.g. python app.py vs python -m app)
_env_path = Path(__file__).parent / ".env"
load_dotenv(_env_path)
load_dotenv()  # also allow cwd .env to override

app = Flask(__name__)

LINEAR_GRAPHQL_URL = "https://api.linear.app/graphql"
OVERLAY_PATH = Path(__file__).parent / "overlay.json"
PAGE_SIZE = 50

# In-memory cache: list of merged issue dicts, and when we last fetched from Linear.
_issues_cache = None
_last_fetched = None

# Linear priority: 0 = No priority, 1 = Urgent, 2 = High, 3 = Medium, 4 = Low
LINEAR_PRIORITY_LABELS = {
    0: "No priority",
    1: "Urgent",
    2: "High",
    3: "Medium",
    4: "Low",
}

PERSONAL_STATUS_OPTIONS = [
    "",
    "Not started",
    "In Progress",
    "Blocked",
    "Waiting On Someone",
    "Ready to Close",
]


def get_linear_token():
    """Return Linear API token; raise if not set.
    Prefer LINEAR_GRAPHQL_API_FILE (path to a file containing the token) so the same
    file used in your shell (e.g. in zshrc) is used here. Otherwise use LINEAR_GRAPHQL_API.
    """
    token = None
    file_path = os.getenv("LINEAR_GRAPHQL_API_FILE")
    if file_path:
        path = Path(file_path).expanduser()
        if path.exists():
            try:
                token = path.read_text(encoding="utf-8").strip()
            except OSError:
                pass
    if not token or not token.strip():
        token = os.getenv("LINEAR_GRAPHQL_API")
    if not token or not token.strip():
        raise ValueError(
            "LINEAR_GRAPHQL_API or LINEAR_GRAPHQL_API_FILE is not set. Set one in .env. "
            "Use LINEAR_GRAPHQL_API_FILE with the path to your token file to use the same source as your shell."
        )
    out = token.strip()
    # If value had quotes around it, strip them
    quoted = len(out) >= 2 and out[0] in ('\"', "'") and out[-1] == out[0]
    if quoted:
        out = out[1:-1].strip()
    return out


def _linear_request(token, query, variables=None):
    """POST to Linear GraphQL; return JSON data or raise."""
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = requests.post(LINEAR_GRAPHQL_URL, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Linear API errors: {data['errors']}")
    return data.get("data", {})


def _fetch_issues_page(token, after_cursor=None):
    """Fetch one page of issues assigned to me (all states). Filter active vs completed in Python."""
    query = """
    query AssignedIssues($first: Int!, $after: String) {
      issues(
        first: $first
        after: $after
        filter: { assignee: { isMe: { eq: true } } }
      ) {
        nodes {
          id
          identifier
          title
          url
          priority
          updatedAt
          state { name, type }
          team { name }
        }
        pageInfo { hasNextPage, endCursor }
      }
    }
    """
    v = {"first": PAGE_SIZE}
    if after_cursor:
        v["after"] = after_cursor
    data = _linear_request(token, query, v)
    return data["issues"]


def _fetch_all_assigned_issues(token):
    """Fetch all issues assigned to me (all states). Filter to active + completed in last 6 months in Python."""
    six_months_ago = datetime.now(timezone.utc) - timedelta(days=180)
    all_nodes = []
    after = None
    while True:
        page = _fetch_issues_page(token, after_cursor=after)  # no viewer_id: filter uses isMe in query
        nodes = page["nodes"]
        for n in nodes:
            state_type = (n.get("state") or {}).get("type")
            is_completed = state_type in ("completed", "canceled")
            updated_at = n.get("updatedAt")
            try:
                updated_dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00")) if updated_at else None
            except (ValueError, AttributeError):
                updated_dt = None
            # Include: active (not completed), or completed/canceled updated in last 6 months
            if not is_completed or (updated_dt and updated_dt.tzinfo and updated_dt >= six_months_ago):
                all_nodes.append(n)
        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]
    return all_nodes


def _normalize_issue(node):
    """Turn Linear API node into our internal shape."""
    state = node.get("state") or {}
    team = node.get("team") or {}
    return {
        "id": node["id"],
        "identifier": node.get("identifier", ""),
        "title": node.get("title", ""),
        "linear_status": state.get("name", ""),
        "linear_priority": node.get("priority", 0),
        "url": node.get("url", ""),
        "team_name": team.get("name", ""),
        "updated_at": node.get("updatedAt", ""),
        "is_completed": state.get("type") in ("completed", "canceled"),
    }


def fetch_linear_issues():
    """Fetch from Linear API and return list of normalized issue dicts (no overlay)."""
    token = get_linear_token()
    raw = _fetch_all_assigned_issues(token)
    return [_normalize_issue(n) for n in raw]


def read_overlay():
    """Return overlay dict (issue_id -> overlay entry). Empty dict if file missing."""
    if not OVERLAY_PATH.exists():
        return {}
    try:
        with open(OVERLAY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def write_overlay_entry(issue_id, data):
    """Update overlay with one entry; merge with existing. data: personal_priority, personal_status, notes."""
    overlay = read_overlay()
    key = _overlay_key(issue_id)
    entry = overlay.get(key, {})
    if "personal_priority" in data:
        entry["personal_priority"] = data["personal_priority"]
    if "personal_status" in data:
        entry["personal_status"] = data.get("personal_status", "")
    if "notes" in data:
        entry["notes"] = data.get("notes", "")
    entry["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    overlay[key] = entry
    with open(OVERLAY_PATH, "w", encoding="utf-8") as f:
        json.dump(overlay, f, indent=2)
    return entry


def _overlay_key(issue_id):
    """Normalize issue_id for overlay key (e.g. LIN-123 or UUID). Use as-is if it looks like identifier."""
    s = (issue_id or "").strip()
    if not s:
        return None
    # If it's already an identifier (e.g. LIN-123), use it; otherwise Linear UUID is valid key too.
    return s


def merge_issues(linear_issues, overlay):
    """Merge Linear issues with overlay; return list of merged dicts with defaults for missing overlay."""
    result = []
    for issue in linear_issues:
        merged = dict(issue)
        key = issue.get("identifier") or issue.get("id")
        entry = overlay.get(key, {}) if key else {}
        merged["personal_priority"] = entry.get("personal_priority")
        merged["personal_status"] = entry.get("personal_status", "")
        merged["notes"] = entry.get("notes", "")
        merged["last_updated"] = entry.get("last_updated")
        result.append(merged)
    return result


def get_cached_issues():
    """Return merged issues from cache; if cache empty, fetch from Linear then merge and cache."""
    global _issues_cache, _last_fetched
    if _issues_cache is None:
        linear = fetch_linear_issues()
        overlay = read_overlay()
        _issues_cache = merge_issues(linear, overlay)
        _last_fetched = datetime.now(timezone.utc).isoformat()
    return _issues_cache


def get_last_fetched():
    """Return ISO timestamp of last Linear fetch, or None."""
    return _last_fetched


def refresh_cache():
    """Force refetch from Linear, update cache, return merged issues."""
    global _issues_cache, _last_fetched
    linear = fetch_linear_issues()
    overlay = read_overlay()
    _issues_cache = merge_issues(linear, overlay)
    _last_fetched = datetime.now(timezone.utc).isoformat()
    return _issues_cache


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/issues", methods=["GET"])
def api_issues():
    try:
        issues = get_cached_issues()
        last = get_last_fetched()
        return jsonify({"issues": issues, "last_fetched": last})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            return jsonify({
                "error": "Linear rejected the API key (401). Use your Linear **personal API key** from Settings → API → Personal API Keys (not an OAuth token). Ensure the key is correct in .env as LINEAR_GRAPHQL_API and not revoked."
            }), 401
        return jsonify({"error": str(e)}), (e.response.status_code if e.response else 502)
    except (requests.RequestException, RuntimeError) as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    try:
        issues = refresh_cache()
        last = get_last_fetched()
        return jsonify({"issues": issues, "last_fetched": last})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            return jsonify({
                "error": "Linear rejected the API key (401). Use your Linear **personal API key** from Settings → API → Personal API Keys (not an OAuth token). Ensure the key is correct in .env as LINEAR_GRAPHQL_API and not revoked."
            }), 401
        return jsonify({"error": str(e)}), (e.response.status_code if e.response else 502)
    except (requests.RequestException, RuntimeError) as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/overlay/<issue_id>", methods=["POST"])
def api_overlay_save(issue_id):
    if not issue_id or not _overlay_key(issue_id):
        return jsonify({"error": "Invalid issue_id"}), 400
    data = request.get_json(force=True, silent=True) or {}
    allowed = {"personal_priority", "personal_status", "notes"}
    payload = {k: data[k] for k in allowed if k in data}
    try:
        entry = write_overlay_entry(issue_id, payload)
        return jsonify({"ok": True, "entry": entry})
    except (OSError, TypeError) as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/priority-labels", methods=["GET"])
def api_priority_labels():
    """Expose Linear priority label map for frontend."""
    return jsonify(LINEAR_PRIORITY_LABELS)


@app.route("/api/personal-status-options", methods=["GET"])
def api_personal_status_options():
    return jsonify(PERSONAL_STATUS_OPTIONS)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
