"""
Sidecar — A personal overlay dashboard for Linear tickets.
Flask backend: read-only Linear API, local overlay files for notes/priority/status.
"""

import copy
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

import metrics as metrics_module

# Load .env from app directory so cwd doesn't matter (e.g. python app.py vs python -m app)
_env_path = Path(__file__).parent / ".env"
load_dotenv(_env_path)
load_dotenv()  # also allow cwd .env to override

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

app = Flask(__name__)

LINEAR_GRAPHQL_URL = "https://api.linear.app/graphql"
_app_dir = Path(__file__).parent
_config_dir = _app_dir / "config"
_data_dir = _app_dir / "data"

SETTINGS_PATH = _config_dir / "settings.json"
INPROGRESS_PATH = _data_dir / "inprogress.json"
COMPLETED_PATH = _data_dir / "completed.json"
OVERLAY_LEGACY_PATH = _data_dir / "overlay.json"
OVERLAY_OLD_PATH = _data_dir / "overlay.old"
OVERLAY_PATH = _data_dir / "overlay.json"
METRICS_STORE_PATH = _data_dir / "metrics_store.json"
PAGE_SIZE = 50

metrics_module.METRICS_STORE_PATH = METRICS_STORE_PATH

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
    "Meeting Scheduled",
    "Testing",
    "Pair Testing",
    "Waiting on Testing",
    "Waiting On Someone",
    "Waiting On Me",
    "Waiting On Review",
    "Blocked",
    "Ready to Close",
    "Completed",
    "Canceled",
    "Notable",
]

# Cycle sort: status order within current/future cycle (lower index first)
CYCLE_STATUS_ORDER = ["In Review", "In Progress", "Todo"]
# Linear priority sort within status: High(2) → Medium(3) → Low(4) → No priority(0)
LINEAR_PRIORITY_SORT_ORDER = {2: 0, 3: 1, 4: 2, 0: 3}

# Reserved top-level keys in overlay.json (not issue entries)
COLUMN_VISIBILITY_KEY = "column_visibility"  # legacy; migrated to column_preferences
COLUMN_PREFERENCES_KEY = "column_preferences"

# Single source of truth for all columns. Sort/filter types and linear_field per spec.
COLUMN_REGISTRY = [
    {"id": "identifier", "label": "Issue", "default_visible": True, "sortable": True, "sort_type": "alpha", "filterable": False, "filter_type": None, "linear_field": "identifier"},
    {"id": "title", "label": "Title", "default_visible": True, "sortable": True, "sort_type": "alpha", "filterable": False, "filter_type": None, "linear_field": "title"},
    {"id": "linear_status", "label": "Linear Status", "default_visible": True, "sortable": True, "sort_type": "alpha", "filterable": True, "filter_type": "multiselect", "linear_field": "state.name"},
    {"id": "linear_priority", "label": "Linear Priority", "default_visible": True, "sortable": True, "sort_type": "priority", "filterable": True, "filter_type": "multiselect", "linear_field": "priority"},
    {"id": "personal_priority", "label": "Personal Priority", "default_visible": True, "sortable": True, "sort_type": "numeric", "filterable": True, "filter_type": "toggle", "linear_field": None},
    {"id": "personal_status", "label": "Personal Status", "default_visible": True, "sortable": True, "sort_type": "alpha", "filterable": True, "filter_type": "multiselect", "linear_field": None},
    {"id": "notes_preview", "label": "Notes", "default_visible": True, "sortable": True, "sort_type": "alpha", "filterable": False, "filter_type": None, "linear_field": None},
    {"id": "linear_updated", "label": "Linear Updated", "default_visible": True, "sortable": True, "sort_type": "date", "filterable": True, "filter_type": "daterange", "linear_field": "updatedAt"},
    {"id": "my_last_edit", "label": "My Last Edit", "default_visible": True, "sortable": True, "sort_type": "date", "filterable": False, "filter_type": None, "linear_field": None},
    {"id": "cycle", "label": "Cycle", "default_visible": False, "sortable": True, "sort_type": "alpha", "filterable": True, "filter_type": "multiselect", "linear_field": "cycle.name"},
    {"id": "team", "label": "Team", "default_visible": False, "sortable": True, "sort_type": "alpha", "filterable": True, "filter_type": "multiselect", "linear_field": "team.name"},
    {"id": "labels", "label": "Labels", "default_visible": False, "sortable": True, "sort_type": "alpha", "filterable": True, "filter_type": "multiselect", "linear_field": "labels"},
]
DEFAULT_COLUMN_ORDER = [c["id"] for c in COLUMN_REGISTRY]
_REGISTRY_IDS = frozenset(c["id"] for c in COLUMN_REGISTRY)
RESERVED_KEYS = (COLUMN_VISIBILITY_KEY, COLUMN_PREFERENCES_KEY)


def _migrate_files_to_subdirs():
    """Move data files from the old root layout into config/ and data/ subdirectories.
    Idempotent — skips any file whose destination already exists.
    Must run before _migrate_overlay_to_split so that function finds files in the right place."""
    _config_dir.mkdir(exist_ok=True)
    _data_dir.mkdir(exist_ok=True)

    moves = [
        (_app_dir / "settings.json",      SETTINGS_PATH),
        (_app_dir / "inprogress.json",     INPROGRESS_PATH),
        (_app_dir / "completed.json",      COMPLETED_PATH),
        (_app_dir / "metrics_store.json",  METRICS_STORE_PATH),
        (_app_dir / "overlay.json",        OVERLAY_LEGACY_PATH),
        (_app_dir / "overlay.old",         OVERLAY_OLD_PATH),
    ]
    for src, dst in moves:
        if src.exists() and not dst.exists():
            src.rename(dst)
            logging.info("Migrated %s → %s", src.name, dst)


def _migrate_overlay_to_split():
    """One-time migration: overlay.json -> settings.json + inprogress.json + completed.json; rename overlay.json to overlay.old.
    Idempotent: only runs when SETTINGS_PATH does not exist."""
    _config_dir.mkdir(exist_ok=True)
    _data_dir.mkdir(exist_ok=True)
    if SETTINGS_PATH.exists():
        return
    if OVERLAY_LEGACY_PATH.exists():
        try:
            with open(OVERLAY_LEGACY_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError):
            raw = {}
        # Build settings: column_preferences only (migrate legacy column_visibility)
        default_vis = {c["id"]: c["default_visible"] for c in COLUMN_REGISTRY}
        prefs = raw.get(COLUMN_PREFERENCES_KEY)
        if isinstance(prefs, dict) and isinstance(prefs.get("order"), list) and isinstance(prefs.get("visibility"), dict):
            if _valid_column_order(prefs["order"]):
                merged_vis = {**default_vis, **{k: bool(v) for k, v in prefs["visibility"].items() if k in default_vis}}
                settings = {COLUMN_PREFERENCES_KEY: {"order": list(prefs["order"]), "visibility": merged_vis}}
            else:
                settings = {COLUMN_PREFERENCES_KEY: {"order": list(DEFAULT_COLUMN_ORDER), "visibility": default_vis}}
        else:
            leg = raw.get(COLUMN_VISIBILITY_KEY)
            if isinstance(leg, dict):
                merged_vis = {**default_vis, **{k: bool(v) for k, v in leg.items() if k in default_vis}}
                settings = {COLUMN_PREFERENCES_KEY: {"order": list(DEFAULT_COLUMN_ORDER), "visibility": merged_vis}}
            else:
                settings = {COLUMN_PREFERENCES_KEY: {"order": list(DEFAULT_COLUMN_ORDER), "visibility": default_vis}}
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
        # Issue entries -> inprogress; completed empty
        inprogress = {k: v for k, v in raw.items() if k not in RESERVED_KEYS and isinstance(v, dict)}
        with open(INPROGRESS_PATH, "w", encoding="utf-8") as f:
            json.dump(inprogress, f, indent=2)
        with open(COMPLETED_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        OVERLAY_LEGACY_PATH.rename(OVERLAY_OLD_PATH)
    else:
        default_vis = {c["id"]: c["default_visible"] for c in COLUMN_REGISTRY}
        settings = {COLUMN_PREFERENCES_KEY: {"order": list(DEFAULT_COLUMN_ORDER), "visibility": default_vis}}
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
        with open(INPROGRESS_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        with open(COMPLETED_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)


def ensure_migrated():
    """Ensure correct directory layout exists; run one-time migrations if needed."""
    _migrate_files_to_subdirs()
    _migrate_overlay_to_split()


def read_settings():
    """Return settings dict from settings.json. Empty dict if missing. No migration (call ensure_migrated first)."""
    if not SETTINGS_PATH.exists():
        return {}
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def write_settings(settings_dict):
    """Write settings dict to settings.json."""
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(settings_dict, f, indent=2)


def read_inprogress_overlay():
    """Return inprogress overlay dict (issue_id -> entry). Empty if file missing. No migration."""
    if not INPROGRESS_PATH.exists():
        return {}
    try:
        with open(INPROGRESS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def read_completed_overlay():
    """Return completed overlay dict (issue_id -> entry). Empty if file missing. No migration."""
    if not COMPLETED_PATH.exists():
        return {}
    try:
        with open(COMPLETED_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def write_inprogress_overlay(overlay):
    """Write inprogress overlay dict to inprogress.json."""
    with open(INPROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(overlay, f, indent=2)


def write_completed_overlay(overlay):
    """Write completed overlay dict to completed.json."""
    with open(COMPLETED_PATH, "w", encoding="utf-8") as f:
        json.dump(overlay, f, indent=2)


def read_issue_overlay():
    """Return merged issue overlay (inprogress + completed). Resolves priority conflicts on inprogress; may write back inprogress."""
    ensure_migrated()
    inprog = read_inprogress_overlay()
    completed = read_completed_overlay()
    resolved = resolve_priority_conflicts(inprog)
    if resolved != inprog:
        logging.warning(
            "Overlay had duplicate personal_priority values; auto-resolved and rewrote inprogress.json"
        )
        write_inprogress_overlay(resolved)
    return {**resolved, **completed}


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
          createdAt
          title
          url
          priority
          updatedAt
          description
          state { id name type }
          team { name }
          cycle { id name number startsAt endsAt }
          labels { nodes { id name color } }
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


def _fetch_all_assigned_issues(token, *, for_backfill: bool = False):
    """Fetch all issues assigned to me (all states), paginated.

    Default (for_backfill=False): same as dashboard — active issues plus completed/canceled
    updated in the last 6 months.

    for_backfill=True: include every issue Linear returns for assignee isMe (no date cutoff),
    so older completed work still assigned to you is included for dwell history import.
    """
    six_months_ago = datetime.now(timezone.utc) - timedelta(days=180)
    all_nodes = []
    after = None
    while True:
        page = _fetch_issues_page(token, after_cursor=after)  # no viewer_id: filter uses isMe in query
        nodes = page["nodes"]
        for n in nodes:
            if for_backfill:
                all_nodes.append(n)
                continue
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
    labels_nodes = (node.get("labels") or {}).get("nodes") or []
    labels = [{"name": n.get("name", ""), "color": n.get("color")} for n in labels_nodes if isinstance(n, dict)]
    return {
        "id": node["id"],
        "identifier": node.get("identifier", ""),
        "title": node.get("title", ""),
        "description": node.get("description", ""),
        "linear_status": state.get("name", ""),
        "linear_state_id": state.get("id", ""),
        "linear_state_type": state.get("type", ""),
        "linear_priority": node.get("priority", 0),
        "url": node.get("url", ""),
        "team_name": team.get("name", ""),
        "updated_at": node.get("updatedAt", ""),
        "is_completed": state.get("type") in ("completed", "canceled"),
        "cycle": cycle,
        "labels": labels,
    }


_metrics_refresh_lock = threading.Lock()


def _bg_refresh_linear_snapshots():
    """Background sub-thread: fetch fresh Linear data and record snapshots."""
    try:
        token = get_linear_token()
        raw = _fetch_all_assigned_issues(token)
        fresh = [_normalize_issue(n) for n in raw]
        metrics_module.record_linear_snapshots(fresh, path=METRICS_STORE_PATH)
        logging.info("Background Linear snapshot: %d issues recorded", len(fresh))
    except Exception as e:
        logging.warning("Background Linear snapshot failed: %s", e)


def _bg_refresh_github_prs(gh_issues, site, gh_token):
    """Background sub-thread: enrich GitHub PRs."""
    try:
        metrics_module.enrich_github_prs_for_issues(
            gh_issues, site, gh_token,
            path=METRICS_STORE_PATH,
            refresh_pr_meta=False,
        )
    except (OSError, requests.RequestException) as e:
        logging.warning("Background GitHub enrich failed: %s", e)


def _run_background_metrics_refresh(gh_issues, site, gh_token):
    """Fetch fresh Linear snapshots and enrich GitHub PRs concurrently. Skips if already running."""
    if not _metrics_refresh_lock.acquire(blocking=False):
        logging.info("Background metrics refresh already running, skipping")
        return
    try:
        t_linear = threading.Thread(target=_bg_refresh_linear_snapshots, daemon=True)
        t_github = threading.Thread(
            target=_bg_refresh_github_prs,
            args=(gh_issues, site, gh_token),
            daemon=True,
        )
        t_linear.start()
        t_github.start()
        t_linear.join()
        t_github.join()
    finally:
        _metrics_refresh_lock.release()


def _record_metrics_after_linear_fetch(linear_normalized, *, force_github: bool = False):
    """Record Linear snapshots; kick off periodic background refresh of both Linear and GitHub."""
    detail: dict = {"metrics_snapshot": "ok"}
    try:
        metrics_module.record_linear_snapshots(linear_normalized, path=METRICS_STORE_PATH)
    except (OSError, TypeError) as e:
        logging.warning("metrics snapshot failed: %s", e)
        detail["metrics_snapshot"] = "error"
    gh_issues = [
        {"identifier": li.get("identifier") or "", "description": li.get("description") or ""}
        for li in linear_normalized
    ]
    try:
        gh_token = metrics_module.get_github_token()
    except ValueError:
        detail["github_enrich"] = "skipped_no_token"
        return detail

    site = get_site_settings()
    cd_minutes = (site.get(metrics_module.SITE_METRICS_KEY) or {}).get("github_enrich_cooldown_minutes")
    cd_seconds = int(cd_minutes) * 60 if isinstance(cd_minutes, (int, float)) else None
    gate = metrics_module.github_enrich_gate(METRICS_STORE_PATH, force=force_github, cooldown_seconds=cd_seconds)
    detail["github_cooldown_seconds"] = gate["cooldown_seconds"]
    if not gate["allowed"]:
        detail["github_enrich"] = "skipped_cooldown"
        detail["github_seconds_until_next"] = gate["seconds_until_next"]
        return detail

    if force_github:
        # Explicit user request from metrics page — run GitHub synchronously so response reflects result
        try:
            n = metrics_module.enrich_github_prs_for_issues(
                gh_issues, site, gh_token,
                path=METRICS_STORE_PATH,
                refresh_pr_meta=True,
            )
            detail["github_enrich"] = "completed"
            detail["github_pr_updates"] = n
        except (OSError, requests.RequestException) as e:
            logging.warning("GitHub metrics enrich failed: %s", e)
            detail["github_enrich"] = "error"
            detail["github_error"] = str(e)
    else:
        # Periodic background refresh: Linear + GitHub run concurrently, dashboard loads instantly
        t = threading.Thread(
            target=_run_background_metrics_refresh,
            args=(gh_issues, site, gh_token),
            daemon=True,
        )
        t.start()
        detail["github_enrich"] = "background"
    return detail


def fetch_linear_issues():
    """Fetch from Linear API and return list of normalized issue dicts (no overlay)."""
    token = get_linear_token()
    raw = _fetch_all_assigned_issues(token)
    return [_normalize_issue(n) for n in raw]


def read_overlay():
    """Return merged issue overlay dict (issue_id -> overlay entry). Uses inprogress.json + completed.json.
    Resolves priority conflicts on inprogress; may write back inprogress.json."""
    ensure_migrated()
    return read_issue_overlay()


def write_overlay(overlay):
    """Write full overlay dict to split files. All issue entries go to inprogress; completed cleared.
    Kept for backward compat (e.g. tests). Prefer write_inprogress_overlay/write_completed_overlay for targeted writes."""
    ensure_migrated()
    issue_entries = {k: v for k, v in overlay.items() if k not in RESERVED_KEYS and isinstance(v, dict)}
    write_inprogress_overlay(issue_entries)
    write_completed_overlay({})


def get_column_visibility():
    """Return column visibility dict from settings; if missing, return defaults from COLUMN_REGISTRY."""
    return get_column_preferences()["visibility"]


def write_column_visibility(visibility_dict):
    """Write column visibility; persists to settings.json. Legacy; prefer write_column_preferences."""
    prefs = get_column_preferences()
    write_column_preferences(prefs["order"], visibility_dict)


def _valid_column_order(order):
    """Return True if order is a list containing every registry ID exactly once."""
    if not isinstance(order, list) or len(order) != len(_REGISTRY_IDS):
        return False
    return set(order) == _REGISTRY_IDS


def get_column_preferences():
    """Return { order: [...], visibility: {...} } from settings.json. Migrates legacy column_visibility only during migration."""
    ensure_migrated()
    settings = read_settings()
    prefs = settings.get(COLUMN_PREFERENCES_KEY)
    if isinstance(prefs, dict):
        order = prefs.get("order")
        vis = prefs.get("visibility")
        if isinstance(order, list) and isinstance(vis, dict):
            if _valid_column_order(order):
                default_vis = {c["id"]: c["default_visible"] for c in COLUMN_REGISTRY}
                merged_vis = {**default_vis, **{k: bool(v) for k, v in vis.items() if k in default_vis}}
                return {"order": list(order), "visibility": merged_vis}
    return {
        "order": list(DEFAULT_COLUMN_ORDER),
        "visibility": {c["id"]: c["default_visible"] for c in COLUMN_REGISTRY},
    }


def write_column_preferences(order_list, visibility_dict):
    """Write column_preferences to settings.json only."""
    ensure_migrated()
    settings = read_settings()
    settings[COLUMN_PREFERENCES_KEY] = {"order": list(order_list), "visibility": dict(visibility_dict)}
    write_settings(settings)


def get_site_settings():
    """Return merged site config (GitHub org/repos/login, metrics cycle labels) from settings.json."""
    ensure_migrated()
    settings = read_settings()
    raw = settings.get(metrics_module.SETTINGS_SITE_KEY)
    return metrics_module.merge_site_defaults(raw if isinstance(raw, dict) else None)


def write_site_settings(updates):
    """Merge updates dict (github / metrics keys) into settings[site] and persist."""
    ensure_migrated()
    if not isinstance(updates, dict):
        raise ValueError("updates must be a dict")
    settings = read_settings()
    raw_site = settings.get(metrics_module.SETTINGS_SITE_KEY)
    if not isinstance(raw_site, dict):
        raw_site = {}
    merged = metrics_module.merge_site_defaults(raw_site)
    gh_u = updates.get("github")
    if isinstance(gh_u, dict):
        gh = merged[metrics_module.SITE_GITHUB_KEY]
        if isinstance(gh_u.get("org"), str):
            gh["org"] = gh_u["org"].strip()
        if isinstance(gh_u.get("login"), str):
            gh["login"] = gh_u["login"].strip()
        if isinstance(gh_u.get("repos"), list):
            gh["repos"] = [str(x).strip() for x in gh_u["repos"] if str(x).strip()]
    m_u = updates.get("metrics")
    if isinstance(m_u, dict):
        m = merged[metrics_module.SITE_METRICS_KEY]
        if isinstance(m_u.get("cycle_start_states"), list):
            m["cycle_start_states"] = [str(x).strip() for x in m_u["cycle_start_states"] if str(x).strip()]
        if isinstance(m_u.get("terminal_state_name"), str):
            m["terminal_state_name"] = m_u["terminal_state_name"].strip()
        raw_cd = m_u.get("github_enrich_cooldown_minutes")
        if raw_cd is not None:
            try:
                v = int(raw_cd)
                m["github_enrich_cooldown_minutes"] = max(0, v)
            except (ValueError, TypeError):
                pass
    settings[metrics_module.SETTINGS_SITE_KEY] = merged
    write_settings(settings)
    return merged


def write_overlay_entry(issue_id, data, is_completed=None):
    """Update overlay with one entry; write to inprogress or completed based on is_completed.
    data: personal_priority, personal_status, notes. personal_priority is stripped when is_completed is True."""
    ensure_migrated()
    key = _overlay_key(issue_id)
    if not key:
        return None
    inprog = read_inprogress_overlay()
    completed = read_completed_overlay()
    entry = (inprog.get(key) or completed.get(key) or {}).copy()
    if "personal_priority" in data:
        entry["personal_priority"] = data["personal_priority"] if not is_completed else None
    if "personal_status" in data:
        entry["personal_status"] = data.get("personal_status", "")
    if "notes" in data:
        entry["notes"] = data.get("notes", "")
    entry["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    if is_completed:
        entry_clean = {k: v for k, v in entry.items() if k != "personal_priority"}
        if key in inprog:
            inprog = {k: v for k, v in inprog.items() if k != key}
            write_inprogress_overlay(inprog)
        completed[key] = entry_clean
        write_completed_overlay(completed)
    else:
        if key in completed:
            completed = {k: v for k, v in completed.items() if k != key}
            write_completed_overlay(completed)
        inprog[key] = entry
        write_inprogress_overlay(inprog)
    return entry if not is_completed else {k: v for k, v in entry.items() if k != "personal_priority"}


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
    _reserved = (COLUMN_VISIBILITY_KEY, COLUMN_PREFERENCES_KEY)
    someone_else_has_n = any(
        k != key and k not in _reserved and (out[k].get("personal_priority")) == n
        for k in out
    )
    if someone_else_has_n:
        for k in out:
            if k in _reserved:
                continue
            p = out[k].get("personal_priority")
            if p is not None and p >= n:
                out[k] = {**out[k], "personal_priority": p + 1}
        out[key] = {**entry, "personal_priority": n}
    else:
        out[key] = {**entry, "personal_priority": n}
        if old_priority is not None and old_priority != n:
            for k in out:
                if k == key or k in _reserved:
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
    _reserved = (COLUMN_VISIBILITY_KEY, COLUMN_PREFERENCES_KEY)
    out = copy.deepcopy(overlay)
    out[key] = {k: v for k, v in entry.items() if k != "personal_priority"}
    for k in out:
        if k in _reserved:
            continue
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
            if kk in (COLUMN_VISIBILITY_KEY, COLUMN_PREFERENCES_KEY):
                continue
            p = out[kk].get("personal_priority")
            if p is not None and p > removed:
                out[kk] = {**out[kk], "personal_priority": p - 1}
    return out


def resolve_priority_conflicts(overlay):
    """Pure: if duplicate personal_priority values exist, reassign contiguous 1,2,3 by last_updated desc.
    Returns a new overlay dict; does not mutate input. No I/O. Skips reserved keys."""
    _reserved = (COLUMN_VISIBILITY_KEY, COLUMN_PREFERENCES_KEY)
    entries_with_priority = [
        (k, v) for k, v in overlay.items()
        if k not in _reserved and isinstance(v, dict) and v.get("personal_priority") is not None
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


def apply_issue_filters(issues, filter_config):
    """Filter issues by date range, Linear status/priority, personal priority set/unset, personal status.
    filter_config: dict with date_from, date_to (ISO date or None), linear_statuses (list of str),
    linear_priorities (list of int 0-4), personal_priority_filter ('all'|'set'|'unset'),
    personal_statuses (list of str, '' = No Status). All filters are ANDed. Pure function, no I/O."""
    if not filter_config:
        return list(issues)
    result = list(issues)
    cfg = filter_config or {}
    date_from = cfg.get("date_from")
    date_to = cfg.get("date_to")
    if date_from is not None or date_to is not None:
        def in_date_range(issue):
            updated = _parse_iso_date(issue.get("updated_at"))
            if not updated:
                return False
            d = updated.date()
            if date_from is not None:
                try:
                    from_dt = datetime.fromisoformat(date_from.replace("Z", "+00:00")).date()
                    if d < from_dt:
                        return False
                except (ValueError, AttributeError):
                    pass
            if date_to is not None:
                try:
                    to_dt = datetime.fromisoformat(date_to.replace("Z", "+00:00")).date()
                    if d > to_dt:
                        return False
                except (ValueError, AttributeError):
                    pass
            return True
        result = [i for i in result if in_date_range(i)]
    linear_statuses = cfg.get("linear_statuses") or []
    if linear_statuses:
        status_set = set(linear_statuses)
        result = [i for i in result if (i.get("linear_status") or "") in status_set]
    linear_priorities = cfg.get("linear_priorities") or []
    if linear_priorities:
        pri_set = set(int(p) for p in linear_priorities if p is not None)
        result = [i for i in result if (i.get("linear_priority") if i.get("linear_priority") is not None else 0) in pri_set]
    personal_priority_filter = (cfg.get("personal_priority_filter") or "all").strip().lower()
    if personal_priority_filter == "set":
        result = [i for i in result if i.get("personal_priority") is not None]
    elif personal_priority_filter == "unset":
        result = [i for i in result if i.get("personal_priority") is None]
    personal_statuses = cfg.get("personal_statuses") or []
    if personal_statuses:
        status_set = set(s if s is not None else "" for s in personal_statuses)
        result = [i for i in result if (i.get("personal_status") or "") in status_set]
    cycles = cfg.get("cycles") or []
    if cycles:
        cycle_set = set(cycles)
        result = [i for i in result if (i.get("cycle") or {}).get("name", "") in cycle_set]
    teams = cfg.get("teams") or []
    if teams:
        team_set = set(teams)
        result = [i for i in result if (i.get("team_name") or "") in team_set]
    label_names = cfg.get("labels") or []
    if label_names:
        label_set = set(label_names)
        result = [i for i in result if any((lb.get("name") or "") in label_set for lb in (i.get("labels") or []))]
    return result


def _parse_filter_config_from_request():
    """Build filter_config dict from Flask request args. For GET /api/issues."""
    cfg = {}
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")
    if date_from and (date_from := date_from.strip()):
        cfg["date_from"] = date_from
    if date_to and (date_to := date_to.strip()):
        cfg["date_to"] = date_to
    linear_status = request.args.getlist("linear_status") or request.args.get("linear_status")
    if linear_status is not None:
        if isinstance(linear_status, str):
            linear_status = [s.strip() for s in linear_status.split(",") if s.strip() or s == ""]
        else:
            linear_status = [s.strip() if isinstance(s, str) else s for s in linear_status]
        if linear_status:
            cfg["linear_statuses"] = linear_status
    linear_priority = request.args.getlist("linear_priority") or request.args.get("linear_priority")
    if linear_priority is not None:
        if isinstance(linear_priority, str):
            linear_priority = [p.strip() for p in linear_priority.split(",")]
        else:
            linear_priority = [p.strip() if isinstance(p, str) else p for p in linear_priority]
        pri_ints = []
        for p in linear_priority:
            try:
                pri_ints.append(int(p))
            except (TypeError, ValueError):
                pass
        if pri_ints:
            cfg["linear_priorities"] = pri_ints
    personal_priority_filter = request.args.get("personal_priority_filter")
    if personal_priority_filter and (personal_priority_filter := personal_priority_filter.strip().lower()) in ("set", "unset", "all"):
        cfg["personal_priority_filter"] = personal_priority_filter
    personal_status = request.args.getlist("personal_status") or request.args.get("personal_status")
    if personal_status is not None:
        if isinstance(personal_status, str):
            # Allow empty string for "No status" via a special token or empty
            personal_status = [s.strip() for s in personal_status.split(",")]
            if "" in personal_status or any(s == "" for s in personal_status):
                pass
            else:
                personal_status = [s for s in personal_status if s is not None]
        else:
            personal_status = [s if s == "" else (s.strip() if isinstance(s, str) else s) for s in personal_status]
        if personal_status is not None and len(personal_status) > 0:
            cfg["personal_statuses"] = personal_status
    cycle = request.args.getlist("cycle") or request.args.get("cycle")
    if cycle is not None:
        if isinstance(cycle, str):
            cycle = [s.strip() for s in cycle.split(",") if s.strip()]
        else:
            cycle = [s.strip() if isinstance(s, str) else str(s) for s in cycle]
        if cycle:
            cfg["cycles"] = cycle
    team = request.args.getlist("team") or request.args.get("team")
    if team is not None:
        if isinstance(team, str):
            team = [s.strip() for s in team.split(",") if s.strip()]
        else:
            team = [s.strip() if isinstance(s, str) else str(s) for s in team]
        if team:
            cfg["teams"] = team
    labels = request.args.getlist("labels") or request.args.get("labels")
    if labels is not None:
        if isinstance(labels, str):
            labels = [s.strip() for s in labels.split(",") if s.strip()]
        else:
            labels = [s.strip() if isinstance(s, str) else str(s) for s in labels]
        if labels:
            cfg["labels"] = labels
    return cfg


def _apply_sort(issues, sort_val, sort_dir=None):
    """Sort issues by sort_val (column id). sort_dir: 'asc' | 'desc'."""
    if not sort_val:
        return list(issues)
    dir_asc = (sort_dir or "asc").strip().lower() != "desc"
    # Column: cycle — alpha by cycle name; no cycle at bottom
    if sort_val == "cycle":
        def cycle_name_key(i):
            c = i.get("cycle")
            name = (c.get("name") or "") if c else ""
            return (0 if name else 1, name)
        return sorted(issues, key=cycle_name_key, reverse=not dir_asc)
    if sort_val == "personal_priority":
        ranked = [i for i in issues if i.get("personal_priority") is not None]
        unranked = [i for i in issues if i.get("personal_priority") is None]
        ranked.sort(key=lambda i: i.get("personal_priority"))
        return ranked + sort_issues_by_cycle(unranked)
    if sort_val == "linear_priority":
        # Ascending: Urgent(1) → High(2) → Medium(3) → Low(4) → No Priority(0)
        def linear_pri_key(i):
            p = i.get("linear_priority") if i.get("linear_priority") is not None else 0
            return (5 if p == 0 else p)
        return sorted(issues, key=linear_pri_key)
    if sort_val == "linear_status":
        return sorted(issues, key=lambda i: (i.get("linear_status") or ""))
    if sort_val == "updated_at":
        return sorted(issues, key=lambda i: (i.get("updated_at") or ""), reverse=True)
    if sort_val == "personal_status":
        status_order = {s: i for i, s in enumerate(PERSONAL_STATUS_OPTIONS)}
        def personal_status_key(i):
            s = i.get("personal_status") or ""
            return status_order.get(s, len(PERSONAL_STATUS_OPTIONS))
        return sorted(issues, key=personal_status_key)
    if sort_val == "last_updated" or sort_val == "my_last_edit":
        # Ascending: oldest first, no-edit last. Descending: newest first, no-edit first.
        def last_updated_key_asc(i):
            lu = i.get("last_updated") or ""
            return (0 if lu else 1, lu)
        def last_updated_key_desc(i):
            lu = i.get("last_updated") or ""
            return (1 if lu else 0, lu)
        key_fn = last_updated_key_desc if not dir_asc else last_updated_key_asc
        return sorted(issues, key=key_fn, reverse=not dir_asc)
    if sort_val == "identifier":
        return sorted(issues, key=lambda i: (i.get("identifier") or ""), reverse=not dir_asc)
    if sort_val == "title":
        return sorted(issues, key=lambda i: (i.get("title") or ""), reverse=not dir_asc)
    if sort_val == "team":
        return sorted(issues, key=lambda i: (i.get("team_name") or ""), reverse=not dir_asc)
    if sort_val == "labels":
        def first_label_key(i):
            labels = i.get("labels") or []
            name = (labels[0].get("name") or "") if labels else ""
            return (0 if name else 1, name)
        return sorted(issues, key=first_label_key, reverse=not dir_asc)
    if sort_val == "notes_preview":
        return sorted(issues, key=lambda i: (i.get("notes") or ""), reverse=not dir_asc)
    if sort_val == "linear_updated":
        return sorted(issues, key=lambda i: (i.get("updated_at") or ""), reverse=not dir_asc)
    return list(issues)


def get_cached_issues():
    """Return merged issues from cache; if cache empty, fetch from Linear then merge and cache."""
    global _issues_cache, _last_fetched
    if _issues_cache is None:
        token = get_linear_token()
        raw = _fetch_all_assigned_issues(token)
        linear = [_normalize_issue(n) for n in raw]
        _record_metrics_after_linear_fetch(linear, force_github=False)
        overlay = read_overlay()
        _issues_cache = merge_issues(linear, overlay)
        _last_fetched = datetime.now(timezone.utc).isoformat()
    return _issues_cache


def get_last_fetched():
    """Return ISO timestamp of last Linear fetch, or None."""
    return _last_fetched


def refresh_cache(force_github: bool = False):
    """Force refetch from Linear, update cache, return (merged issues, refresh_detail dict).
    Moves overlay entries between inprogress/completed by Linear is_completed; rebalances inprogress only."""
    global _issues_cache, _last_fetched
    ensure_migrated()
    token = get_linear_token()
    raw = _fetch_all_assigned_issues(token)
    linear = [_normalize_issue(n) for n in raw]
    refresh_detail = _record_metrics_after_linear_fetch(linear, force_github=force_github)
    inprog = read_inprogress_overlay()
    completed_d = read_completed_overlay()
    completed_ids = {issue.get("identifier") or issue.get("id") for issue in linear if issue.get("is_completed")}
    active_ids = {issue.get("identifier") or issue.get("id") for issue in linear if not issue.get("is_completed")}
    linear_by_key = {(i.get("identifier") or i.get("id")): i for i in linear}
    completed_with_priority = [iid for iid in completed_ids if (inprog.get(iid) or {}).get("personal_priority") is not None]
    if completed_with_priority:
        inprog = rebalance_overlay_after_remove_multiple(inprog, completed_with_priority)
    for key in list(inprog.keys()):
        if key in completed_ids:
            entry = inprog.pop(key)
            issue = linear_by_key.get(key)
            if issue and (issue.get("linear_status") or "").strip().lower() == "canceled":
                entry["personal_status"] = "Canceled"
            else:
                entry["personal_status"] = "Completed"
            completed_d[key] = {k: v for k, v in entry.items() if k != "personal_priority"}
    for key in list(completed_d.keys()):
        if key in active_ids:
            inprog[key] = completed_d.pop(key)
    write_inprogress_overlay(inprog)
    write_completed_overlay(completed_d)
    overlay = {**inprog, **completed_d}
    _issues_cache = merge_issues(linear, overlay)
    _last_fetched = datetime.now(timezone.utc).isoformat()
    refresh_detail["linear_issue_count"] = len(linear)
    return _issues_cache, refresh_detail


def _get_version():
    """Return version string from VERSION file, or empty string if missing."""
    version_path = Path(__file__).parent / "VERSION"
    if version_path.exists():
        try:
            return version_path.read_text(encoding="utf-8").strip()
        except OSError:
            pass
    return ""


@app.route("/")
def landing():
    return render_template("index.html", version=_get_version(), nav_active="landing")


@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html", version=_get_version(), nav_active="dashboard")


@app.route("/metrics")
def metrics_page():
    return render_template("metrics.html", version=_get_version(), nav_active="metrics")


@app.route("/settings")
def settings_page():
    return render_template("settings.html", version=_get_version(), nav_active="settings")


@app.route("/api/issues", methods=["GET"])
def api_issues():
    try:
        issues = get_cached_issues()
        filter_val = request.args.get("filter")
        sort_val = request.args.get("sort")
        if sort_val is None and filter_val == "active":
            sort_val = "personal_priority"
        issues = _apply_filter(issues, filter_val)
        filter_config = _parse_filter_config_from_request()
        issues = apply_issue_filters(issues, filter_config)
        sort_dir = request.args.get("sort_dir")
        issues = _apply_sort(issues, sort_val, sort_dir)
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
        data = request.get_json(force=True, silent=True) or {}
        force_github = bool(data.get("force_github"))
        issues, refresh_detail = refresh_cache(force_github=force_github)
        last = get_last_fetched()
        return jsonify({"issues": issues, "last_fetched": last, "refresh_detail": refresh_detail})
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


def _is_issue_completed(issue_id, data, cache):
    """Return True if issue is completed. Prefer data.get('is_completed'), else lookup in cache."""
    if "is_completed" in data:
        return bool(data["is_completed"])
    if cache:
        for issue in cache:
            if (issue.get("identifier") or issue.get("id")) == issue_id:
                return bool(issue.get("is_completed"))
    return False


@app.route("/api/overlay/<issue_id>", methods=["POST"])
def api_overlay_save(issue_id):
    global _issues_cache
    if not issue_id or not _overlay_key(issue_id):
        return jsonify({"error": "Invalid issue_id"}), 400
    data = request.get_json(force=True, silent=True) or {}
    allowed = {"personal_priority", "personal_status", "notes", "is_completed"}
    payload = {k: data[k] for k in allowed if k in data and k != "is_completed"}
    if "personal_status" in payload:
        val = payload.get("personal_status", "")
        if val not in PERSONAL_STATUS_OPTIONS:
            return jsonify({"error": "Invalid personal_status"}), 400
    key = _overlay_key(issue_id)
    try:
        cache = get_cached_issues() if _issues_cache is not None else None
        is_completed = _is_issue_completed(key, data, cache)
        if "personal_priority" in payload and not is_completed:
            inprog = read_inprogress_overlay()
            pri = payload.get("personal_priority")
            if pri is None or (isinstance(pri, str) and pri.strip() == ""):
                new_inprog = rebalance_overlay_after_remove(inprog, issue_id)
            else:
                try:
                    n = int(pri)
                    if inprog.get(key, {}).get("personal_priority") is not None:
                        inprog = rebalance_overlay_after_remove(inprog, issue_id)
                    new_inprog = rebalance_overlay_after_assign(inprog, issue_id, n)
                except (TypeError, ValueError):
                    new_inprog = inprog
            for k in ("personal_status", "notes"):
                if k in payload:
                    if key not in new_inprog:
                        new_inprog[key] = {}
                    new_inprog[key][k] = payload.get(k, "" if k == "personal_status" else "")
            new_inprog[key] = new_inprog.get(key, {})
            new_inprog[key]["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            write_inprogress_overlay(new_inprog)
            _issues_cache = None
            entry = new_inprog[key]
            full_overlay = read_overlay()
            return jsonify({"ok": True, "entry": entry, "overlay": full_overlay})
        entry = write_overlay_entry(issue_id, payload, is_completed=is_completed)
        _issues_cache = None
        return jsonify({"ok": True, "entry": entry})
    except (OSError, TypeError) as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/config/columns", methods=["GET"])
def api_config_columns_get():
    """Return column registry and current column order and visibility (single source of truth for frontend)."""
    prefs = get_column_preferences()
    return jsonify({
        "columns": COLUMN_REGISTRY,
        "order": prefs["order"],
        "visibility": prefs["visibility"],
    })


@app.route("/api/config/columns", methods=["POST"])
def api_config_columns_post():
    """Update column order and visibility; persist to overlay.json. Reject invalid order or hiding identifier/title."""
    data = request.get_json(force=True, silent=True) or {}
    order = data.get("order")
    visibility = data.get("visibility")
    if not isinstance(order, list):
        return jsonify({"error": "order must be an array of column IDs"}), 400
    if not isinstance(visibility, dict):
        return jsonify({"error": "visibility must be a dict"}), 400
    if not _valid_column_order(order):
        if len(set(order)) != len(order):
            return jsonify({"error": "order must contain each column ID exactly once (no duplicates)"}), 400
        missing = _REGISTRY_IDS - set(order)
        extra = set(order) - _REGISTRY_IDS
        if missing or extra:
            return jsonify({"error": "order must contain every column ID exactly once"}), 400
        return jsonify({"error": "order must contain every column ID exactly once"}), 400
    current = get_column_preferences()
    merged_vis = {**current["visibility"], **{k: bool(v) for k, v in visibility.items() if k in _REGISTRY_IDS}}
    if not merged_vis.get("identifier", True):
        return jsonify({"error": "identifier column cannot be hidden"}), 400
    if not merged_vis.get("title", True):
        return jsonify({"error": "title column cannot be hidden"}), 400
    visible_count = sum(1 for c in COLUMN_REGISTRY if merged_vis.get(c["id"], True))
    if visible_count <= 2:
        return jsonify({"error": "At least one column besides Issue and Title must remain visible"}), 400
    write_column_preferences(order, merged_vis)
    prefs = get_column_preferences()
    return jsonify({
        "columns": COLUMN_REGISTRY,
        "order": prefs["order"],
        "visibility": prefs["visibility"],
    })


@app.route("/api/priority-labels", methods=["GET"])
def api_priority_labels():
    """Expose Linear priority label map for frontend."""
    return jsonify(LINEAR_PRIORITY_LABELS)


@app.route("/api/personal-status-options", methods=["GET"])
def api_personal_status_options():
    return jsonify(PERSONAL_STATUS_OPTIONS)


@app.route("/api/metrics", methods=["GET"])
def api_metrics_get():
    try:
        store = metrics_module.read_metrics_store(METRICS_STORE_PATH)
        site = get_site_settings()
        return jsonify(metrics_module.build_metrics_api_payload(store, site))
    except (OSError, TypeError, ValueError) as e:
        logging.exception("api_metrics_get")
        return jsonify({"error": str(e)}), 500


@app.route("/api/metrics/backfill-github", methods=["POST"])
def api_metrics_backfill_github():
    try:
        gh_token = metrics_module.get_github_token()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    ensure_migrated()
    completed = read_completed_overlay()
    site = get_site_settings()
    terminal = (site.get(metrics_module.SITE_METRICS_KEY) or {}).get("terminal_state_name", "Done")

    def linear_request_fn(query, variables):
        tok = get_linear_token()
        return _linear_request(tok, query, variables)

    try:
        result = metrics_module.run_github_backfill(
            completed,
            site,
            gh_token,
            linear_request_fn,
            terminal,
            path=METRICS_STORE_PATH,
        )
        return jsonify({"ok": True, **result})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except requests.HTTPError as e:
        return jsonify({"error": str(e)}), (e.response.status_code if e.response else 502)
    except (requests.RequestException, RuntimeError, OSError) as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/metrics/backfill-linear-dwell", methods=["POST"])
def api_metrics_backfill_linear_dwell():
    """Rebuild linear_transitions from Linear issue.history (completed cohort + all assigned issues)."""
    try:
        tok = get_linear_token()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    ensure_migrated()
    completed = read_completed_overlay()
    site = get_site_settings()
    terminal = (site.get(metrics_module.SITE_METRICS_KEY) or {}).get("terminal_state_name", "Done")

    def linear_request_fn(query, variables):
        return _linear_request(tok, query, variables)

    try:
        assigned_nodes = _fetch_all_assigned_issues(tok, for_backfill=True)
        result = metrics_module.run_linear_dwell_backfill(
            completed,
            linear_request_fn,
            terminal,
            path=METRICS_STORE_PATH,
            assigned_nodes=assigned_nodes,
        )
        return jsonify({"ok": True, **result})
    except (requests.RequestException, RuntimeError, OSError) as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/settings/site", methods=["GET"])
def api_settings_site_get():
    ensure_migrated()
    return jsonify(get_site_settings())


@app.route("/api/settings/site", methods=["POST"])
def api_settings_site_post():
    data = request.get_json(force=True, silent=True) or {}
    try:
        merged = write_site_settings(data)
        return jsonify({"ok": True, "site": merged})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
