"""
Local metrics: Linear workflow snapshots, GitHub PR durations, aggregates.
No data sent to external services except GitHub/Linear API reads initiated by the app.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

# Default paths (app.py may assign before use in tests)
METRICS_STORE_PATH: Path = Path(__file__).parent / "data" / "metrics_store.json"

DEFAULT_GITHUB_ORG = "hcapatientkeeper"
DEFAULT_GITHUB_REPOS = [
    "int-transporter",
    "int-hca-expanse",
    "int-hca-integrations",
    "int-release-server",
]

SETTINGS_SITE_KEY = "site"
SITE_GITHUB_KEY = "github"
SITE_METRICS_KEY = "metrics"

GITHUB_PR_URL_RE = re.compile(
    r"https?://github\.com/([^/]+)/([^/]+)/pull/(\d+)(?:\s|#|$|/|\?)?",
    re.IGNORECASE,
)

# Personal overlay value for backfill eligibility
BACKFILL_PERSONAL_STATUS = "Completed"


def _github_enrich_cooldown_seconds() -> int:
    """Min seconds between full GitHub enrich runs on refresh (0 = always run)."""
    try:
        v = int(os.getenv("SIDECAR_GITHUB_REFRESH_COOLDOWN_SEC", "120"))
    except ValueError:
        v = 120
    return max(0, v)


def _github_pr_meta_fresh_seconds() -> float:
    """Skip re-fetching PR details if cache entry newer than this (when not forcing)."""
    try:
        v = int(os.getenv("SIDECAR_GITHUB_PR_META_FRESH_SEC", "14400"))
    except ValueError:
        v = 14400
    return float(max(0, v))


def _github_pull_list_max_pages() -> int:
    """Pages of GET /pulls per repo (100 PRs each). Capped for safety."""
    try:
        v = int(os.getenv("SIDECAR_GITHUB_PULL_PAGES", "8"))
    except ValueError:
        v = 8
    return max(1, min(v, 30))


def github_enrich_gate(path: Optional[Path] = None, *, force: bool = False) -> dict[str, Any]:
    """
    Whether refresh may run GitHub list+PR metadata sync.
    Returns allowed, cooldown_seconds, seconds_until_next (if blocked).
    """
    cd = _github_enrich_cooldown_seconds()
    if force or cd == 0:
        return {"allowed": True, "cooldown_seconds": cd, "seconds_until_next": None}
    store = read_metrics_store(path)
    last = store.get("last_github_enrich_at")
    if not isinstance(last, str):
        return {"allowed": True, "cooldown_seconds": cd, "seconds_until_next": None}
    dt = _parse_t_iso(last)
    if not dt:
        return {"allowed": True, "cooldown_seconds": cd, "seconds_until_next": None}
    elapsed = (datetime.now(timezone.utc) - dt).total_seconds()
    if elapsed >= cd:
        return {"allowed": True, "cooldown_seconds": cd, "seconds_until_next": None}
    return {
        "allowed": False,
        "cooldown_seconds": cd,
        "seconds_until_next": max(0, int(cd - elapsed)),
    }


def _github_pr_entry_is_fresh(entry: Any, max_age_sec: float, now_utc: datetime) -> bool:
    if not isinstance(entry, dict):
        return False
    u = entry.get("updated_at")
    if not u or not isinstance(u, str):
        return False
    dt = _parse_t_iso(u)
    if not dt:
        return False
    return (now_utc - dt).total_seconds() < max_age_sec


def get_github_token() -> str:
    """
    Resolve GitHub token for metrics (read-only API). Never log the token.

    Precedence: SIDECAR_TOKEN, then GITHUB_TOKEN (for users who prefer a generic env name).
    Set in .env or your shell (e.g. export SIDECAR_TOKEN=... in zshrc).
    """
    for env_key in ("SIDECAR_TOKEN", "GITHUB_TOKEN"):
        raw = os.getenv(env_key)
        if raw and raw.strip():
            return raw.strip().strip('"').strip("'")
    raise ValueError(
        "GitHub is not configured. Set SIDECAR_TOKEN (or GITHUB_TOKEN) in the environment or .env."
    )


def default_site_github() -> dict[str, Any]:
    return {
        "org": DEFAULT_GITHUB_ORG,
        "repos": list(DEFAULT_GITHUB_REPOS),
        "login": "",
    }


def default_site_metrics() -> dict[str, Any]:
    return {
        "cycle_start_states": ["In Progress"],
        "terminal_state_name": "Done",
    }


def merge_site_defaults(site: Optional[dict]) -> dict[str, Any]:
    gh = default_site_github()
    mt = default_site_metrics()
    if isinstance(site, dict):
        g = site.get(SITE_GITHUB_KEY)
        if isinstance(g, dict):
            if isinstance(g.get("org"), str) and g["org"].strip():
                gh["org"] = g["org"].strip()
            if isinstance(g.get("repos"), list):
                gh["repos"] = [str(x).strip() for x in g["repos"] if str(x).strip()]
            if isinstance(g.get("login"), str):
                gh["login"] = g["login"].strip()
        m = site.get(SITE_METRICS_KEY)
        if isinstance(m, dict):
            if isinstance(m.get("cycle_start_states"), list):
                mt["cycle_start_states"] = [str(x).strip() for x in m["cycle_start_states"] if str(x).strip()]
            if isinstance(m.get("terminal_state_name"), str) and m["terminal_state_name"].strip():
                mt["terminal_state_name"] = m["terminal_state_name"].strip()
    return {SITE_GITHUB_KEY: gh, SITE_METRICS_KEY: mt}


def read_metrics_store(path: Optional[Path] = None) -> dict[str, Any]:
    p = path or METRICS_STORE_PATH
    if not p.exists():
        return _empty_metrics_store()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _empty_metrics_store()
        data.setdefault("version", 1)
        data.setdefault("linear_last_sample", {})
        data.setdefault("linear_transitions", [])
        data.setdefault("github_prs", {})
        return data
    except (json.JSONDecodeError, OSError):
        return _empty_metrics_store()


def _empty_metrics_store() -> dict[str, Any]:
    return {
        "version": 1,
        "linear_last_sample": {},
        "linear_transitions": [],
        "github_prs": {},
    }


def write_metrics_store(data: dict[str, Any], path: Optional[Path] = None) -> None:
    p = path or METRICS_STORE_PATH
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def parse_github_pr_urls(text: str) -> list[tuple[str, str, int]]:
    if not text:
        return []
    seen = set()
    out = []
    for m in GITHUB_PR_URL_RE.finditer(text):
        owner, repo, num_s = m.group(1), m.group(2), m.group(3)
        key = (owner.lower(), repo.lower(), int(num_s))
        if key not in seen:
            seen.add(key)
            out.append((owner, repo, int(num_s)))
    return out


def parse_identifier_team_number(identifier: str) -> Optional[tuple[str, float]]:
    if not identifier or not isinstance(identifier, str):
        return None
    parts = identifier.split("-", 1)
    if len(parts) != 2:
        return None
    team, num_s = parts[0].strip(), parts[1].strip()
    if not team or not num_s.isdigit():
        return None
    return team, float(int(num_s))


def record_linear_snapshots(
    issues: list[dict[str, Any]],
    now_utc: Optional[datetime] = None,
    path: Optional[Path] = None,
) -> None:
    """
    issues: normalized dicts with id, identifier, linear_state_id, linear_status, linear_state_type
    Append to linear_transitions when state id changes; update linear_last_sample.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    t_iso = now_utc.isoformat().replace("+00:00", "Z")
    store = read_metrics_store(path)
    last = store.get("linear_last_sample") or {}
    if not isinstance(last, dict):
        last = {}
    transitions = store.get("linear_transitions") or []
    if not isinstance(transitions, list):
        transitions = []

    for issue in issues:
        iid = issue.get("id")
        ident = issue.get("identifier") or ""
        sid = issue.get("linear_state_id") or ""
        sname = issue.get("linear_status") or ""
        stype = issue.get("linear_state_type") or ""
        if not iid:
            continue
        prev = last.get(iid)
        prev_sid = (prev or {}).get("state_id") if isinstance(prev, dict) else None
        if prev_sid != sid:
            transitions.append(
                {
                    "t": t_iso,
                    "issue_id": iid,
                    "identifier": ident,
                    "state_id": sid,
                    "state_name": sname,
                    "state_type": stype,
                }
            )
        last[iid] = {
            "state_id": sid,
            "state_name": sname,
            "state_type": stype,
            "t": t_iso,
        }

    store["linear_last_sample"] = last
    store["linear_transitions"] = transitions
    write_metrics_store(store, path)


def display_days_round_up(days: float) -> float:
    """
    Format fractional days for UI: one decimal place, always rounded up (e.g. 6.462 → 6.5).
    Non-positive values become 0.0.
    """
    if days <= 0:
        return 0.0
    return math.ceil(days * 10.0 - 1e-9) / 10.0


def _parse_t_iso(iso_s: str) -> Optional[datetime]:
    if not iso_s:
        return None
    try:
        s = iso_s.replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def compute_linear_dwell_by_state(
    store: dict[str, Any],
    _cycle_start_states: list[str],
    now_utc: Optional[datetime] = None,
) -> dict[str, Any]:
    """
    Per issue: total time spent in each Linear workflow state (by state name).

    Uses the full transition timeline (sorted by time): each segment from t_i to t_{i+1}
    is credited to the state recorded at t_i. If the latest event is not a terminal
    Linear state (completed / canceled), time from that event until now is credited to
    that state.

    _cycle_start_states is ignored (kept so callers / settings stay unchanged); older
    versions only counted time after a configured "cycle start" state, which produced
    zeros when state names did not match or work never hit that state.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    transitions = store.get("linear_transitions") or []
    if not isinstance(transitions, list):
        return {"per_state_days": {}, "issues_tracked": 0, "per_state_issues": {}}

    by_issue: dict[str, list[dict]] = {}
    for tr in transitions:
        if not isinstance(tr, dict):
            continue
        iid = tr.get("issue_id")
        if not iid:
            continue
        by_issue.setdefault(iid, []).append(tr)

    per_state_seconds: dict[str, float] = {}
    per_state_issue_ids: dict[str, set[str]] = {}
    issues_count = 0

    def _add_dwell(state_key: str, seconds: float, ident_label: str) -> None:
        if seconds <= 0:
            return
        per_state_seconds[state_key] = per_state_seconds.get(state_key, 0.0) + seconds
        if ident_label:
            per_state_issue_ids.setdefault(state_key, set()).add(ident_label)

    for _iid, rows in by_issue.items():
        sorted_rows = sorted(rows, key=lambda r: (r.get("t") or ""))
        issue_ident = ""
        for r in sorted_rows:
            v = r.get("identifier")
            if v:
                issue_ident = str(v)
                break
        ident_label = issue_ident or (str(_iid)[:12] if _iid else "")
        events: list[tuple[datetime, str, str, str]] = []
        for r in sorted_rows:
            dt = _parse_t_iso(r.get("t"))
            if not dt:
                continue
            events.append(
                (
                    dt,
                    str(r.get("state_id") or ""),
                    str(r.get("state_name") or ""),
                    str(r.get("state_type") or ""),
                )
            )
        if len(events) < 1:
            continue

        issues_count += 1
        for j in range(0, len(events) - 1):
            t0, sid0, name0, _type0 = events[j]
            t1, _sid1, _name1, _type1 = events[j + 1]
            dur = (t1 - t0).total_seconds()
            key = name0 or sid0 or "unknown"
            _add_dwell(key, dur, ident_label)

        _t_last, sid_last, name_last, type_last = events[-1]
        if type_last not in ("completed", "canceled"):
            dur = (now_utc - events[-1][0]).total_seconds()
            key = name_last or sid_last or "unknown"
            _add_dwell(key, dur, ident_label)

    per_state_days = {k: display_days_round_up(v / 86400.0) for k, v in per_state_seconds.items()}
    per_state_issues = {k: sorted(v) for k, v in per_state_issue_ids.items()}
    return {
        "per_state_days": per_state_days,
        "issues_tracked": issues_count,
        "per_state_issues": per_state_issues,
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _parse_github_next_link(link_header: Optional[str]) -> Optional[str]:
    """Return URL for rel=\"next\" from a GitHub Link response header."""
    if not link_header:
        return None
    for segment in link_header.split(","):
        segment = segment.strip()
        if 'rel="next"' not in segment:
            continue
        try:
            start = segment.index("<") + 1
            end = segment.index(">")
            return segment[start:end]
        except ValueError:
            continue
    return None


def list_repo_pulls_for_discovery(
    sess: requests.Session,
    owner: str,
    repo: str,
    token: str,
    *,
    max_pages: Optional[int] = None,
    per_page: int = 100,
) -> list[dict[str, Any]]:
    """
    List recent PRs via REST pulls API (works with normal repo read access).
    Avoids /search/issues, which often returns 403 for SAML orgs or fine-grained tokens.
    """
    pages = max_pages if max_pages is not None else _github_pull_list_max_pages()
    all_pulls: list[dict[str, Any]] = []
    base_url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    params: dict[str, Any] = {
        "state": "all",
        "sort": "updated",
        "direction": "desc",
        "per_page": per_page,
    }
    next_url: Optional[str] = None
    page_num = 0
    for _ in range(pages):
        page_num += 1
        logging.info("  GitHub: fetching PR list %s/%s (page %d)…", owner, repo, page_num)
        url = next_url or base_url
        req_params = {} if next_url else params
        resp = sess.get(url, headers=_github_headers(token), params=req_params, timeout=60)
        if resp.status_code in (403, 404):
            logging.warning(
                "GitHub pulls list failed for %s/%s (%s). For org repos, authorize the token for SSO "
                "or use a PAT with repository access.",
                owner,
                repo,
                resp.status_code,
            )
            return []
        resp.raise_for_status()
        batch = resp.json()
        if not isinstance(batch, list):
            break
        all_pulls.extend(batch)
        if len(batch) < per_page:
            break
        next_url = _parse_github_next_link(resp.headers.get("Link"))
        if not next_url:
            break
    logging.info("  GitHub: found %d PRs in %s/%s", len(all_pulls), owner, repo)
    return all_pulls


def github_pr_ready_and_terminal(
    owner: str,
    repo: str,
    pr_number: int,
    token: str,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    """Fetch PR JSON + timeline; return ready_at, merged_at, closed_at, draft, outcomes."""
    logging.info("  GitHub: fetching PR %s/%s#%d…", owner, repo, pr_number)
    sess = session or requests.Session()
    base = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"
    r = sess.get(base, headers=_github_headers(token), timeout=30)
    r.raise_for_status()
    pr = r.json()
    created_at = pr.get("created_at")
    merged_at = pr.get("merged_at")
    closed_at = pr.get("closed_at")
    draft = bool(pr.get("draft"))
    user = (pr.get("user") or {}).get("login")

    tl_url = f"https://api.github.com/repos/{owner}/{repo}/issues/{pr_number}/timeline"
    tl_r = sess.get(
        tl_url,
        headers={**_github_headers(token), "Accept": "application/vnd.github.mockingbird-preview+json"},
        timeout=30,
    )
    ready_at = None
    if tl_r.status_code == 200:
        try:
            events = tl_r.json()
            if isinstance(events, list):
                last_ready = None
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    if ev.get("event") == "ready_for_review":
                        last_ready = ev.get("created_at")
                    elif ev.get("event") == "convert_to_draft":
                        last_ready = None
                ready_at = last_ready
        except (ValueError, TypeError):
            pass

    if not ready_at and not draft:
        ready_at = created_at
    if draft and not ready_at:
        ready_at = None

    outcome = "open"
    if merged_at:
        outcome = "merged"
    elif closed_at:
        outcome = "closed_unmerged"

    open_seconds = None
    ra_dt = _parse_t_iso(ready_at) if ready_at else None
    if ra_dt and merged_at:
        m_dt = _parse_t_iso(merged_at.replace("Z", "+00:00") if merged_at else "")
        if m_dt:
            open_seconds = max(0.0, (m_dt - ra_dt).total_seconds())
    elif ra_dt and closed_at and not merged_at:
        c_dt = _parse_t_iso(closed_at.replace("Z", "+00:00") if closed_at else "")
        if c_dt:
            open_seconds = max(0.0, (c_dt - ra_dt).total_seconds())

    return {
        "owner": owner,
        "repo": repo,
        "number": pr_number,
        "html_url": pr.get("html_url"),
        "title": pr.get("title"),
        "user": user,
        "created_at": created_at,
        "ready_at": ready_at,
        "merged_at": merged_at,
        "closed_at": closed_at,
        "draft": draft,
        "outcome": outcome,
        "open_seconds_non_draft": open_seconds,
    }


def discover_pr_refs_for_issue(
    identifier: str,
    description: str,
    org: str,
    repos: list[str],
    token: str,
    github_login: str,
    session: Optional[requests.Session] = None,
    pull_cache: Optional[dict[tuple[str, str], list[dict[str, Any]]]] = None,
) -> list[tuple[str, str, int]]:
    """
    Linked PRs from issue description URLs, then fallback: scan recent repo PRs for
    author + identifier in title or head branch (no Search API).
    """
    sess = session or requests.Session()
    found: list[tuple[str, str, int]] = []
    seen: set[tuple[str, str, int]] = set()
    cache = pull_cache if pull_cache is not None else {}

    for o, r, n in parse_github_pr_urls(description or ""):
        key = (o.lower(), r.lower(), n)
        if key not in seen:
            seen.add(key)
            found.append((o, r, n))

    if not github_login or not identifier:
        return found

    id_upper = identifier.upper()
    # Match the full identifier — require a non-digit (or end) after the numeric suffix so
    # that e.g. "PK-15" does not claim PRs for "PK-1516", "PK-150", etc.
    id_pattern = re.compile(re.escape(id_upper) + r"(?!\d)", re.IGNORECASE)
    login_l = github_login.strip().lower()
    for repo_name in repos:
        owner, repo = org, repo_name.strip()
        if not repo:
            continue
        ckey = (owner.lower(), repo.lower())
        if ckey not in cache:
            cache[ckey] = list_repo_pulls_for_discovery(sess, owner, repo, token)
        for pr in cache[ckey]:
            if not isinstance(pr, dict):
                continue
            user = ((pr.get("user") or {}) if isinstance(pr.get("user"), dict) else {}).get("login") or ""
            if user.strip().lower() != login_l:
                continue
            title = (pr.get("title") or "") or ""
            head = pr.get("head") if isinstance(pr.get("head"), dict) else {}
            ref = (head.get("ref") or "") if isinstance(head, dict) else ""
            blob = f"{title} {ref}"
            if not id_pattern.search(blob):
                continue
            num = pr.get("number")
            if num is None:
                continue
            pkey = (owner.lower(), repo.lower(), int(num))
            if pkey not in seen:
                seen.add(pkey)
                found.append((owner, repo, int(num)))

    return found


def pr_cache_key(owner: str, repo: str, num: int) -> str:
    return f"{owner.lower()}/{repo.lower()}#{num}"


def enrich_github_prs_for_issues(
    issues: list[dict[str, Any]],
    site: dict[str, Any],
    token: str,
    path: Optional[Path] = None,
    session: Optional[requests.Session] = None,
    *,
    refresh_pr_meta: bool = False,
) -> int:
    """
    For each issue, discover PRs and merge metadata into store github_prs.
    When refresh_pr_meta is False, skips re-fetching PR JSON/timeline if cache entry is still fresh.
    Returns count of PR records updated (including refreshed rows).
    """
    merged_site = merge_site_defaults(site)
    gh = merged_site[SITE_GITHUB_KEY]
    org = gh["org"]
    repos = gh["repos"]
    login = gh.get("login") or ""
    store = read_metrics_store(path)
    gprs = store.get("github_prs") or {}
    if not isinstance(gprs, dict):
        gprs = {}
    updated = 0
    skipped_fresh = 0
    sess = session or requests.Session()
    pull_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    now_utc = datetime.now(timezone.utc)
    fresh_sec = _github_pr_meta_fresh_seconds()

    logging.info(
        "GitHub enrich: scanning %d issue(s) across %d repo(s)%s",
        len(issues),
        len(repos),
        " (force refresh)" if refresh_pr_meta else "",
    )

    for issue in issues:
        ident = issue.get("identifier") or ""
        desc = issue.get("description") or ""
        refs = discover_pr_refs_for_issue(
            ident, desc, org, repos, token, login, sess, pull_cache=pull_cache
        )
        for owner, repo, num in refs:
            key = pr_cache_key(owner, repo, num)
            existing = gprs.get(key)
            if (
                not refresh_pr_meta
                and isinstance(existing, dict)
                and _github_pr_entry_is_fresh(existing, fresh_sec, now_utc)
            ):
                if existing.get("linear_identifier") != ident:
                    gprs[key] = {**existing, "linear_identifier": ident}
                    updated += 1
                else:
                    skipped_fresh += 1
                continue
            try:
                meta = github_pr_ready_and_terminal(owner, repo, num, token, sess)
                gprs[key] = {
                    **meta,
                    "linear_identifier": ident,
                    "updated_at": now_utc.isoformat().replace("+00:00", "Z"),
                }
                updated += 1
            except requests.HTTPError as e:
                logging.warning("GitHub PR fetch failed %s: %s", key, e)

    logging.info(
        "GitHub enrich done: %d PR record(s) updated, %d skipped (cache fresh)",
        updated,
        skipped_fresh,
    )
    store["github_prs"] = gprs
    store["last_github_enrich_at"] = now_utc.isoformat().replace("+00:00", "Z")
    store["last_github_enrich_skipped_pr_meta"] = skipped_fresh
    write_metrics_store(store, path)
    return updated


def overlay_entry_eligible_for_backfill_personal(entry: Any) -> bool:
    """True if completed overlay row has personal_status Completed (Linear state checked separately)."""
    return isinstance(entry, dict) and (entry.get("personal_status") or "") == BACKFILL_PERSONAL_STATUS


def linear_fetch_issue_for_backfill(
    identifier: str,
    linear_request_fn,
) -> Optional[dict[str, Any]]:
    """
    linear_request_fn: (query, variables) -> data dict from Linear GraphQL.
    Returns { id, identifier, description, createdAt, state { name type id } } or None.
    """
    parsed = parse_identifier_team_number(identifier)
    if not parsed:
        return None
    team_key, num = parsed
    query = """
    query IssueByTeamNum($teamKey: String!, $num: Float!) {
      issues(
        filter: { team: { key: { eq: $teamKey } }, number: { eq: $num } }
        first: 1
      ) {
        nodes {
          id
          identifier
          description
          createdAt
          state { id name type }
        }
      }
    }
    """
    try:
        data = linear_request_fn(query, {"teamKey": team_key, "num": num})
        issues = (data.get("issues") or {}).get("nodes") or []
        if not issues:
            return None
        n = issues[0]
        if not isinstance(n, dict):
            return None
        return n
    except Exception:
        logging.exception("Linear fetch failed for %s", identifier)
        return None


def _looks_like_linear_issue_uuid(value: str) -> bool:
    """Linear issue ids are UUIDs; skip stray keys (e.g. legacy test data) before calling the API."""
    try:
        uuid.UUID(str(value).strip())
        return True
    except (ValueError, TypeError, AttributeError):
        return False


def linear_fetch_issue_by_id(issue_id: str, linear_request_fn) -> Optional[dict[str, Any]]:
    """Fetch issue by Linear issue UUID via issues(filter:) — root issue(id:) can 400 on some API responses."""
    if not _looks_like_linear_issue_uuid(issue_id):
        return None
    query = """
    query IssueByUuidFilter($issueId: String!) {
      issues(filter: { id: { eq: $issueId } }, first: 1) {
        nodes {
          id
          identifier
          createdAt
          state { id name type }
        }
      }
    }
    """
    try:
        data = linear_request_fn(query, {"issueId": str(issue_id).strip()})
        nodes = (data.get("issues") or {}).get("nodes") or []
        if not nodes or not isinstance(nodes[0], dict):
            return None
        n = nodes[0]
        return n if n.get("id") else None
    except Exception as e:
        logging.warning("Linear fetch by id failed for %s: %s", issue_id, e)
        return None


def issue_ids_referenced_in_metrics_store(store: dict[str, Any]) -> set[str]:
    """Issue ids appearing in linear_transitions or linear_last_sample (Sidecar has seen them before)."""
    out: set[str] = set()
    for t in store.get("linear_transitions") or []:
        if isinstance(t, dict):
            iid = t.get("issue_id")
            if iid:
                out.add(str(iid))
    last = store.get("linear_last_sample") or {}
    if isinstance(last, dict):
        for k in last:
            if k and isinstance(k, str):
                out.add(k)
    return out


def collect_issue_ids_for_dwell_backfill_pass(
    store: dict[str, Any],
    transitions: list[dict[str, Any]],
    last: dict[str, Any],
) -> set[str]:
    """Union of ids on disk and ids in the in-memory transition list / last_sample being built."""
    ids = issue_ids_referenced_in_metrics_store(store)
    for t in transitions:
        if isinstance(t, dict) and t.get("issue_id"):
            ids.add(str(t["issue_id"]))
    if isinstance(last, dict):
        for k in last:
            if k and isinstance(k, str):
                ids.add(k)
    return ids


def _normalize_transition_timestamp(iso_s: str) -> str:
    """Store transition times like record_linear_snapshots (Z suffix)."""
    if not iso_s or not isinstance(iso_s, str):
        return iso_s
    s = iso_s.strip()
    if s.endswith("+00:00"):
        return s.replace("+00:00", "Z")
    return s


def _transition_dict(
    t_iso: str,
    issue_id: str,
    identifier: str,
    sid: str,
    sname: str,
    stype: str,
    *,
    source: str = "linear_history_backfill",
) -> dict[str, Any]:
    return {
        "t": _normalize_transition_timestamp(t_iso),
        "issue_id": issue_id,
        "identifier": identifier,
        "state_id": sid,
        "state_name": sname,
        "state_type": stype,
        "source": source,
    }


def history_nodes_to_transition_rows(
    issue_id: str,
    identifier: str,
    created_at: Optional[str],
    history_nodes: list[Any],
) -> list[dict[str, Any]]:
    """
    Linear IssueHistory: one row per state change (toState). Matches record_linear_snapshots row shape.
    """
    raw: list[tuple[str, dict[str, Any], Optional[dict[str, Any]]]] = []
    for n in history_nodes:
        if not isinstance(n, dict):
            continue
        ts = n.get("toState")
        if not isinstance(ts, dict) or not ts.get("id"):
            continue
        t = n.get("createdAt")
        if not t or not isinstance(t, str):
            continue
        fs = n.get("fromState")
        if fs is not None and not isinstance(fs, dict):
            fs = None
        raw.append((t, ts, fs))

    raw.sort(key=lambda x: x[0])

    out: list[dict[str, Any]] = []
    prev_to_id: Optional[str] = None
    for t, ts, fs in raw:
        tid = str(ts["id"])
        if tid == prev_to_id:
            continue
        if not out and fs and fs.get("id") and str(fs["id"]) != tid:
            ca = created_at
            if ca:
                out.append(
                    _transition_dict(
                        ca,
                        issue_id,
                        identifier,
                        str(fs["id"]),
                        str(fs.get("name") or ""),
                        str(fs.get("type") or ""),
                    )
                )
        out.append(
            _transition_dict(
                t,
                issue_id,
                identifier,
                tid,
                str(ts.get("name") or ""),
                str(ts.get("type") or ""),
            )
        )
        prev_to_id = tid

    return out


def fetch_all_issue_history_nodes(issue_id: str, linear_request_fn) -> list[dict[str, Any]]:
    """Paginate Linear issue.history (IssueHistory state changes). Uses issues(filter:) like linear_fetch_issue_by_id."""
    if not _looks_like_linear_issue_uuid(issue_id):
        return []
    query = """
    query IssueHistoryPage($issueId: String!, $after: String) {
      issues(filter: { id: { eq: $issueId } }, first: 1) {
        nodes {
          id
          history(first: 50, after: $after) {
            pageInfo { hasNextPage endCursor }
            nodes {
              ... on IssueHistory {
                createdAt
                fromState { id name type }
                toState { id name type }
              }
            }
          }
        }
      }
    }
    """
    nodes_out: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        variables: dict[str, Any] = {"issueId": str(issue_id).strip()}
        if cursor:
            variables["after"] = cursor
        data = linear_request_fn(query, variables)
        issues_block = data.get("issues") if isinstance(data, dict) else None
        issue_list = (issues_block or {}).get("nodes") or []
        issue = issue_list[0] if issue_list and isinstance(issue_list[0], dict) else None
        if not isinstance(issue, dict):
            break
        hist = issue.get("history") or {}
        page = hist.get("nodes") or []
        for n in page:
            if isinstance(n, dict):
                nodes_out.append(n)
        pi = hist.get("pageInfo") or {}
        if not isinstance(pi, dict) or not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        if not cursor:
            break
    return nodes_out


def apply_linear_history_for_issue(
    issue_id: str,
    identifier: str,
    created_at: Optional[str],
    state: dict[str, Any],
    linear_request_fn,
    transitions: list[dict[str, Any]],
    last: dict[str, Any],
    t_now: str,
) -> tuple[bool, Optional[str]]:
    """
    Replace all linear_transitions for this issue with rows rebuilt from Linear issue.history.
    Mutates transitions and last. Returns (True, None) or (False, skip_reason).
    """
    iid = str(issue_id)
    ident = str(identifier or "")
    logging.info("  Linear history: importing %s…", ident or iid)
    try:
        hist_nodes = fetch_all_issue_history_nodes(iid, linear_request_fn)
    except Exception as e:
        logging.warning("Linear history fetch failed for %s: %s", ident or iid, e)
        return False, f"linear_history_error:{e}"

    ca = created_at if isinstance(created_at, str) else None
    new_rows = history_nodes_to_transition_rows(iid, ident, ca, hist_nodes)
    st = state if isinstance(state, dict) else {}
    if not new_rows:
        # Linear sometimes returns an empty history connection; seed one row from createdAt + current
        # state so dwell is not "since first Sidecar snapshot" only.
        if ca and st.get("id"):
            new_rows = [
                _transition_dict(
                    ca,
                    iid,
                    ident,
                    str(st["id"]),
                    str(st.get("name") or ""),
                    str(st.get("type") or ""),
                    source="linear_backfill_fallback_no_history",
                )
            ]
        else:
            return False, "no_history_transitions"

    transitions[:] = [t for t in transitions if str(t.get("issue_id") or "") != iid]
    transitions.extend(new_rows)
    last[iid] = {
        "state_id": st.get("id", ""),
        "state_name": st.get("name", ""),
        "state_type": st.get("type", ""),
        "t": t_now,
    }
    return True, None


def run_linear_dwell_backfill(
    completed_overlay: dict[str, Any],
    linear_request_fn,
    terminal_state_name: str,
    path: Optional[Path] = None,
    assigned_nodes: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Rebuild linear_transitions from Linear issue history for:
    1) Completed-overlay issues with personal Completed + Linear terminal state (GitHub backfill cohort).
    2) Every issue in assigned_nodes (caller should pass the full assignee list, e.g. for_backfill fetch
       with no 6-month cutoff on completed).
    3) Any issue id already present in the metrics store (transitions / last_sample) but not processed yet,
       via issue(id) — covers issues unassigned from you or missing from the assignee list.

    If Linear returns no history rows, falls back to one transition at issue createdAt with current state.

    Each issue_id is processed at most once. On failure after mutations, transitions for that id are left
    unchanged only when we return early before mutation; apply always replaces on success.
    """
    processed: list[str] = []
    skipped: list[dict[str, Any]] = []
    from_completed_overlay = 0
    from_assigned = 0
    from_metrics_store = 0
    store = read_metrics_store(path)
    transitions: list[dict[str, Any]] = [
        t for t in (store.get("linear_transitions") or []) if isinstance(t, dict)
    ]
    last = store.get("linear_last_sample") or {}
    if not isinstance(last, dict):
        last = {}
    now_utc = datetime.now(timezone.utc)
    t_now = now_utc.isoformat().replace("+00:00", "Z")
    term = (terminal_state_name or "").strip()
    seen_ids: set[str] = set()

    n_overlay = sum(1 for k, v in completed_overlay.items()
                    if k not in ("column_visibility", "column_preferences") and isinstance(v, dict)
                    and (v.get("personal_status") or "") == BACKFILL_PERSONAL_STATUS)
    logging.info(
        "Linear dwell backfill: %d completed overlay candidate(s), %d assigned node(s)",
        n_overlay,
        len(assigned_nodes) if assigned_nodes else 0,
    )

    for key, entry in completed_overlay.items():
        if key in ("column_visibility", "column_preferences") or not isinstance(entry, dict):
            continue
        if (entry.get("personal_status") or "") != BACKFILL_PERSONAL_STATUS:
            skipped.append({"identifier": key, "reason": "personal_status_not_completed"})
            continue
        node = linear_fetch_issue_for_backfill(key, linear_request_fn)
        if not node:
            skipped.append({"identifier": key, "reason": "linear_not_found"})
            continue
        st = node.get("state") or {}
        st_name = (st.get("name") or "").strip()
        if st_name != term:
            skipped.append({"identifier": key, "reason": f"linear_state_not_terminal:{st_name}"})
            continue

        issue_id = node.get("id")
        if not issue_id:
            skipped.append({"identifier": key, "reason": "missing_issue_id"})
            continue
        iid = str(issue_id)
        if iid in seen_ids:
            continue
        ident = node.get("identifier") or key
        created_at = node.get("createdAt") if isinstance(node.get("createdAt"), str) else None

        ok, reason = apply_linear_history_for_issue(
            iid,
            str(ident),
            created_at,
            st,
            linear_request_fn,
            transitions,
            last,
            t_now,
        )
        if ok:
            seen_ids.add(iid)
            processed.append(str(ident))
            from_completed_overlay += 1
        else:
            skipped.append({"identifier": str(ident), "reason": reason or "unknown"})

    for node in assigned_nodes or []:
        if not isinstance(node, dict):
            continue
        issue_id = node.get("id")
        if not issue_id:
            continue
        iid = str(issue_id)
        if iid in seen_ids:
            continue
        ident = node.get("identifier") or iid
        created_at = node.get("createdAt") if isinstance(node.get("createdAt"), str) else None
        st = node.get("state") or {}
        if not isinstance(st, dict):
            st = {}

        ok, reason = apply_linear_history_for_issue(
            iid,
            str(ident),
            created_at,
            st,
            linear_request_fn,
            transitions,
            last,
            t_now,
        )
        if ok:
            seen_ids.add(iid)
            processed.append(str(ident))
            from_assigned += 1
        else:
            skipped.append({"identifier": str(ident), "reason": reason or "unknown"})

    store_ids = collect_issue_ids_for_dwell_backfill_pass(store, transitions, last)
    for iid in sorted(store_ids):
        if iid in seen_ids:
            continue
        if not _looks_like_linear_issue_uuid(iid):
            continue
        node = linear_fetch_issue_by_id(iid, linear_request_fn)
        if not node:
            skipped.append({"identifier": iid, "reason": "linear_issue_by_id_not_found"})
            continue
        ident = node.get("identifier") or iid
        created_at = node.get("createdAt") if isinstance(node.get("createdAt"), str) else None
        st = node.get("state") or {}
        if not isinstance(st, dict):
            st = {}

        ok, reason = apply_linear_history_for_issue(
            iid,
            str(ident),
            created_at,
            st,
            linear_request_fn,
            transitions,
            last,
            t_now,
        )
        if ok:
            seen_ids.add(iid)
            processed.append(str(ident))
            from_metrics_store += 1
        else:
            skipped.append({"identifier": str(ident), "reason": reason or "unknown"})

    logging.info(
        "Linear dwell backfill done: %d processed (overlay=%d, assigned=%d, store=%d), %d skipped, %d transition rows",
        len(processed),
        from_completed_overlay,
        from_assigned,
        from_metrics_store,
        len(skipped),
        len(transitions),
    )
    store["linear_transitions"] = transitions
    store["linear_last_sample"] = last
    write_metrics_store(store, path)
    return {
        "processed": processed,
        "skipped": skipped,
        "transition_rows_total": len(transitions),
        "from_completed_overlay": from_completed_overlay,
        "from_assigned": from_assigned,
        "from_metrics_store": from_metrics_store,
    }


def run_github_backfill(
    completed_overlay: dict[str, Any],
    site: dict[str, Any],
    token: str,
    linear_request_fn,
    terminal_state_name: str,
    path: Optional[Path] = None,
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    """
    For each completed overlay entry with personal Completed, fetch Linear state;
    if state.name == terminal_state_name, run GitHub PR discovery + metadata fetch.
    """
    merged = merge_site_defaults(site)
    gh = merged[SITE_GITHUB_KEY]
    org = gh["org"]
    repos = gh["repos"]
    login = gh.get("login") or ""

    processed = []
    skipped = []
    sess = session or requests.Session()
    pull_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    store = read_metrics_store(path)
    gprs = store.get("github_prs") or {}
    if not isinstance(gprs, dict):
        gprs = {}

    n_candidates = sum(
        1 for k, v in completed_overlay.items()
        if k not in ("column_visibility", "column_preferences")
        and isinstance(v, dict)
        and (v.get("personal_status") or "") == BACKFILL_PERSONAL_STATUS
    )
    logging.info("GitHub backfill: %d completed overlay candidate(s) to check", n_candidates)

    for key, entry in completed_overlay.items():
        if key in ("column_visibility", "column_preferences") or not isinstance(entry, dict):
            continue
        if (entry.get("personal_status") or "") != BACKFILL_PERSONAL_STATUS:
            skipped.append({"identifier": key, "reason": "personal_status_not_completed"})
            continue
        logging.info("  GitHub backfill: checking %s…", key)
        node = linear_fetch_issue_for_backfill(key, linear_request_fn)
        if not node:
            skipped.append({"identifier": key, "reason": "linear_not_found"})
            continue
        st = node.get("state") or {}
        st_name = (st.get("name") or "").strip()
        if st_name != (terminal_state_name or "").strip():
            logging.info("    Skipping %s — Linear state is '%s', not '%s'", key, st_name, terminal_state_name)
            skipped.append({"identifier": key, "reason": f"linear_state_not_terminal:{st_name}"})
            continue

        ident = node.get("identifier") or key
        desc = node.get("description") or ""
        refs = discover_pr_refs_for_issue(
            ident, desc, org, repos, token, login, sess, pull_cache=pull_cache
        )
        if not refs:
            logging.info("    No PRs found for %s", ident)
        for owner, repo, num in refs:
            pk = pr_cache_key(owner, repo, num)
            try:
                meta = github_pr_ready_and_terminal(owner, repo, num, token, sess)
                gprs[pk] = {
                    **meta,
                    "linear_identifier": ident,
                    "backfill": True,
                    "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
            except requests.HTTPError as e:
                skipped.append({"identifier": ident, "reason": f"github:{pk}:{e}"})
        processed.append(ident)

    logging.info(
        "GitHub backfill done: %d processed, %d skipped, %d total PRs cached",
        len(processed), len(skipped), len(gprs),
    )
    store["github_prs"] = gprs
    write_metrics_store(store, path)
    return {"processed": processed, "skipped": skipped, "prs_total": len(gprs)}


def build_metrics_api_payload(
    store: dict[str, Any],
    site: dict[str, Any],
    now_utc: Optional[datetime] = None,
) -> dict[str, Any]:
    merged = merge_site_defaults(site)
    mcfg = merged[SITE_METRICS_KEY]
    dwell = compute_linear_dwell_by_state(store, mcfg.get("cycle_start_states") or ["In Progress"], now_utc)

    gprs = store.get("github_prs") or {}
    merged_list = []
    closed_unmerged = []
    if isinstance(gprs, dict):
        for _k, pr in gprs.items():
            if not isinstance(pr, dict):
                continue
            sec = pr.get("open_seconds_non_draft")
            days = (
                display_days_round_up(float(sec) / 86400.0) if sec is not None else None
            )
            row = {
                "repo": f"{pr.get('owner')}/{pr.get('repo')}",
                "number": pr.get("number"),
                "title": pr.get("title"),
                "outcome": pr.get("outcome"),
                "open_days_non_draft": days,
                "ready_at": pr.get("ready_at"),
                "merged_at": pr.get("merged_at"),
                "created_at": pr.get("created_at"),
                "draft": pr.get("draft", False),
                "linear_identifier": pr.get("linear_identifier"),
                "html_url": pr.get("html_url"),
            }
            merged_list.append(row)
            if pr.get("outcome") == "closed_unmerged":
                closed_unmerged.append(row)

    merged_only = [r for r in merged_list if r.get("outcome") == "merged" and r.get("open_days_non_draft") is not None]

    return {
        "linear_dwell_days_by_state": dwell["per_state_days"],
        "linear_dwell_issues_by_state": dwell["per_state_issues"],
        "linear_issues_tracked": dwell["issues_tracked"],
        "github_prs": merged_list,
        "github_merged_count": len([r for r in merged_list if r.get("outcome") == "merged"]),
        "github_closed_unmerged_count": len(closed_unmerged),
        "github_open_count": len([r for r in merged_list if r.get("outcome") == "open"]),
        "github_merged_open_days": [r["open_days_non_draft"] for r in merged_only if r["open_days_non_draft"] is not None],
        "last_github_enrich_at": store.get("last_github_enrich_at"),
    }
