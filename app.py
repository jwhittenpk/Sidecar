"""
Sidecar — A personal overlay dashboard for Linear tickets.
Flask backend: read-only Linear API, local overlay.json for notes/priority/status.
"""

import copy
import json
import logging
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

# Cycle sort: status order within current/future cycle (lower index first)
CYCLE_STATUS_ORDER = ["In Review", "In Progress", "Todo"]
# Linear priority sort within status: High(2) → Medium(3) → Low(4) → No priority(0)
LINEAR_PRIORITY_SORT_ORDER = {2: 0, 3: 1, 4: 2, 0: 3}


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
          cycle { id name number startsAt endsAt }
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


def _parse_cycle(cycle_node):
    """Extract cycle dict from Linear API node; return None if missing/invalid."""
    if not cycle_node or not isinstance(cycle_node, dict):
        return None
    starts_at = cycle_node.get("startsAt")
    ends_at = cycle_node.get("endsAt")
    return {
        "id": cycle_node.get("id"),
        "name": cycle_node.get("name", ""),
        "number": cycle_node.get("number"),
        "starts_at": starts_at,
        "ends_at": ends_at,
    }


def _normalize_issue(node):
    """Turn Linear API node into our internal shape."""
    state = node.get("state") or {}
    team = node.get("team") or {}
    cycle = _parse_cycle(node.get("cycle"))
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
        "cycle": cycle,
    }


def fetch_linear_issues():
    """Fetch from Linear API and return list of normalized issue dicts (no overlay)."""
    token = get_linear_token()
    raw = _fetch_all_assigned_issues(token)
    return [_normalize_issue(n) for n in raw]


def read_overlay():
    """Return overlay dict (issue_id -> overlay entry). Empty dict if file missing.
    Resolves priority conflicts on load; if fixed, logs a warning and writes back."""
    if not OVERLAY_PATH.exists():
        return {}
    try:
        with open(OVERLAY_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    resolved = resolve_priority_conflicts(raw)
    if resolved != raw:
        logging.warning(
            "Overlay had duplicate personal_priority values; auto-resolved and rewrote overlay.json"
        )
        write_overlay(resolved)
        return resolved
    return raw


def write_overlay(overlay):
    """Write the entire overlay dict to overlay.json in one write."""
    with open(OVERLAY_PATH, "w", encoding="utf-8") as f:
        json.dump(overlay, f, indent=2)


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


def rebalance_overlay_after_assign(overlay, issue_id, new_priority):
    """Pure: assign personal_priority N to issue_id. If N is taken, shift existing >= N down by 1.
    Returns a new overlay dict; does not mutate input. No I/O."""
    key = _overlay_key(issue_id)
    if not key or new_priority is None:
        return overlay
    try:
        n = int(new_priority)
    except (TypeError, ValueError):
        return overlay
    if n < 1:
        return overlay
    out = copy.deepcopy(overlay)
    if key not in out:
        out[key] = {}
    entry = out[key]
    old_priority = entry.get("personal_priority")
    someone_else_has_n = any(
        k != key and (out[k].get("personal_priority")) == n
        for k in out
    )
    if someone_else_has_n:
        for k in out:
            p = out[k].get("personal_priority")
            if p is not None and p >= n:
                out[k] = {**out[k], "personal_priority": p + 1}
        out[key] = {**entry, "personal_priority": n}
    else:
        out[key] = {**entry, "personal_priority": n}
        if old_priority is not None and old_priority != n:
            for k in out:
                if k == key:
                    continue
                p = out[k].get("personal_priority")
                if p is not None and p > old_priority:
                    out[k] = {**out[k], "personal_priority": p - 1}
    return out


def rebalance_overlay_after_remove(overlay, issue_id):
    """Pure: remove personal_priority for issue_id; decrement everyone above so list stays contiguous.
    Returns a new overlay dict; does not mutate input. No I/O."""
    key = _overlay_key(issue_id)
    if not key or key not in overlay:
        return copy.deepcopy(overlay)
    entry = overlay[key]
    removed = entry.get("personal_priority")
    if removed is None:
        return copy.deepcopy(overlay)
    out = copy.deepcopy(overlay)
    out[key] = {k: v for k, v in entry.items() if k != "personal_priority"}
    for k in out:
        p = out[k].get("personal_priority")
        if p is not None and p > removed:
            out[k] = {**out[k], "personal_priority": p - 1}
    return out


def rebalance_overlay_after_remove_multiple(overlay, issue_ids):
    """Pure: remove personal_priority for each issue_id in issue_ids and rebalance so list stays contiguous.
    Removes in ascending order of current priority so decrements are correct. Returns a new overlay dict."""
    keys = [_overlay_key(i) for i in issue_ids]
    keys = [k for k in keys if k and k in overlay]
    if not keys:
        return copy.deepcopy(overlay)
    # Sort by current priority (ascending) so we remove from bottom up and don't shift wrong
    with_priority = [(k, overlay[k].get("personal_priority")) for k in keys if overlay[k].get("personal_priority") is not None]
    with_priority.sort(key=lambda x: (x[1] or 0))
    out = copy.deepcopy(overlay)
    for k, _ in with_priority:
        removed = out.get(k, {}).get("personal_priority")
        if removed is None:
            continue
        out[k] = {kk: vv for kk, vv in out[k].items() if kk != "personal_priority"}
        for kk in out:
            p = out[kk].get("personal_priority")
            if p is not None and p > removed:
                out[kk] = {**out[kk], "personal_priority": p - 1}
    return out


def resolve_priority_conflicts(overlay):
    """Pure: if duplicate personal_priority values exist, reassign contiguous 1,2,3 by last_updated desc.
    Returns a new overlay dict; does not mutate input. No I/O."""
    entries_with_priority = [
        (k, v) for k, v in overlay.items()
        if v.get("personal_priority") is not None
    ]
    if not entries_with_priority:
        return copy.deepcopy(overlay)
    priorities = {v.get("personal_priority") for _, v in entries_with_priority}
    if len(priorities) == len(entries_with_priority):
        return copy.deepcopy(overlay)
    # Duplicates: sort by last_updated desc, reassign 1, 2, 3, ...
    sorted_entries = sorted(
        entries_with_priority,
        key=lambda x: (x[1].get("last_updated") or ""),
        reverse=True,
    )
    out = copy.deepcopy(overlay)
    for i, (k, v) in enumerate(sorted_entries, 1):
        out[k] = {**out[k], "personal_priority": i}
    return out


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


def _parse_iso_date(iso_str):
    """Parse ISO date string to datetime in UTC; return None if invalid."""
    if not iso_str:
        return None
    try:
        s = (iso_str or "").replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except (ValueError, AttributeError):
        return None


def _cycle_is_current(cycle, now_utc):
    """True if now_utc (date or datetime) falls within cycle start and end (inclusive)."""
    if not cycle or not now_utc:
        return False
    start = _parse_iso_date(cycle.get("starts_at"))
    end = _parse_iso_date(cycle.get("ends_at"))
    if not start or not end:
        return False
    now_date = now_utc.date() if hasattr(now_utc, "date") else now_utc
    start_date = start.date()
    end_date = end.date()
    return start_date <= now_date <= end_date


def _cycle_is_future(cycle, now_utc):
    """True if cycle starts after now (UTC)."""
    if not cycle:
        return False
    start = _parse_iso_date(cycle.get("starts_at"))
    if not start:
        return False
    now_date = now_utc.date() if hasattr(now_utc, "date") else now_utc
    return start.date() > now_date


def _status_sort_key(status):
    """Lower index = earlier in CYCLE_STATUS_ORDER; unknown statuses after."""
    if not status:
        return len(CYCLE_STATUS_ORDER)
    try:
        return CYCLE_STATUS_ORDER.index(status)
    except ValueError:
        return len(CYCLE_STATUS_ORDER)


def _linear_priority_sort_key(priority):
    """Lower value = higher priority in cycle (High→Medium→Low→None)."""
    p = priority if priority is not None else 0
    return LINEAR_PRIORITY_SORT_ORDER.get(p, 3)


def _sort_within_cycle_group(issues):
    """Sort issues by status order then linear priority (for current/future cycle groups)."""
    return sorted(
        issues,
        key=lambda i: (
            _status_sort_key(i.get("linear_status")),
            _linear_priority_sort_key(i.get("linear_priority")),
        ),
    )


def sort_issues_by_cycle(issues, now_utc=None):
    """Sort merged issues by Cycle order: Urgent → Personal priority → Current cycle → Future cycles → No cycle.
    Pure function: no I/O. now_utc defaults to datetime.now(timezone.utc) for testing can inject a fixed time.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    now_date = now_utc.date() if hasattr(now_utc, "date") else now_utc

    urgent = []
    personal_priority = []
    current_cycle = []
    future_cycle = []
    no_cycle = []

    for issue in issues:
        if (issue.get("linear_priority")) == 1:
            urgent.append(issue)
            continue
        if issue.get("personal_priority") is not None:
            personal_priority.append(issue)
            continue
        cycle = issue.get("cycle")
        if not cycle:
            no_cycle.append(issue)
            continue
        if _cycle_is_current(cycle, now_utc):
            current_cycle.append(issue)
        elif _cycle_is_future(cycle, now_utc):
            future_cycle.append(issue)
        else:
            no_cycle.append(issue)

    personal_priority.sort(key=lambda i: i.get("personal_priority") or 99)
    current_cycle = _sort_within_cycle_group(current_cycle)
    # Future cycles: sort by cycle start (nearest first), then within cycle by status then priority
    def future_sort_key(i):
        c = i.get("cycle") or {}
        start = _parse_iso_date(c.get("starts_at")) or datetime.max.replace(tzinfo=timezone.utc)
        return (start, _status_sort_key(i.get("linear_status")), _linear_priority_sort_key(i.get("linear_priority")))
    future_cycle.sort(key=future_sort_key)
    no_cycle.sort(key=lambda i: (i.get("updated_at") or ""), reverse=True)

    return urgent + personal_priority + current_cycle + future_cycle + no_cycle


def _apply_filter(issues, filter_val):
    """Filter issues by active/completed. filter_val: 'active' | 'completed' | None (all)."""
    if not filter_val:
        return list(issues)
    if filter_val == "active":
        return [i for i in issues if not i.get("is_completed")]
    if filter_val == "completed":
        return [i for i in issues if i.get("is_completed")]
    return list(issues)


def _apply_sort(issues, sort_val):
    """Sort issues by sort_val: 'cycle' | 'personal_priority' | 'linear_priority' | 'linear_status' | 'updated_at'."""
    if not sort_val:
        return list(issues)
    if sort_val == "cycle":
        return sort_issues_by_cycle(issues)
    if sort_val == "personal_priority":
        ranked = [i for i in issues if i.get("personal_priority") is not None]
        unranked = [i for i in issues if i.get("personal_priority") is None]
        ranked.sort(key=lambda i: i.get("personal_priority"))
        return ranked + sort_issues_by_cycle(unranked)
    if sort_val == "linear_priority":
        return sorted(issues, key=lambda i: (i.get("linear_priority") or 0))
    if sort_val == "linear_status":
        return sorted(issues, key=lambda i: (i.get("linear_status") or ""))
    if sort_val == "updated_at":
        return sorted(issues, key=lambda i: (i.get("updated_at") or ""), reverse=True)
    return list(issues)


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
    """Force refetch from Linear, update cache, return merged issues.
    Completed/cancelled issues have their personal priority removed and list rebalanced (one write)."""
    global _issues_cache, _last_fetched
    linear = fetch_linear_issues()
    overlay = read_overlay()
    completed_with_priority = [
        issue.get("identifier") or issue.get("id")
        for issue in linear
        if issue.get("is_completed")
        and (overlay.get(issue.get("identifier") or issue.get("id")) or {}).get("personal_priority") is not None
    ]
    if completed_with_priority:
        overlay = rebalance_overlay_after_remove_multiple(overlay, completed_with_priority)
        write_overlay(overlay)
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
        filter_val = request.args.get("filter")
        sort_val = request.args.get("sort")
        if sort_val is None and filter_val == "active":
            sort_val = "personal_priority"
        issues = _apply_filter(issues, filter_val)
        issues = _apply_sort(issues, sort_val)
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
    key = _overlay_key(issue_id)
    try:
        overlay = read_overlay()
        if "personal_priority" in payload:
            pri = payload.get("personal_priority")
            if pri is None or (isinstance(pri, str) and pri.strip() == ""):
                new_overlay = rebalance_overlay_after_remove(overlay, issue_id)
            else:
                try:
                    n = int(pri)
                    new_overlay = rebalance_overlay_after_assign(overlay, issue_id, n)
                except (TypeError, ValueError):
                    new_overlay = overlay
            for k in ("personal_status", "notes"):
                if k in payload:
                    if key not in new_overlay:
                        new_overlay[key] = {}
                    new_overlay[key][k] = payload.get(k, "" if k == "personal_status" else "")
            new_overlay[key] = new_overlay.get(key, {})
            new_overlay[key]["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            write_overlay(new_overlay)
            entry = new_overlay[key]
            return jsonify({"ok": True, "entry": entry, "overlay": new_overlay})
        else:
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
