# Agent guidance for Sidecar

This file is for AI coding agents (e.g. Cursor) working on this codebase.

## Project summary

**Sidecar** is a personal overlay dashboard for Linear tickets. It runs locally (Flask on localhost:5000), fetches issues assigned to the user from the Linear GraphQL API (**read-only**), and stores **personal context** (notes, personal priority, personal status) in local JSON files next to the app. It is not a Linear replacement; it is a private overlay that never sends personal data to Linear or anywhere else.

## Architecture

- **Backend:** Python, Flask. `app.py`: routes, Linear API client, overlay file I/O, merge logic. `metrics.py`: local metrics store, Linear snapshot aggregation, GitHub PR helpers (read-only APIs).
- **Data:** Local JSON files next to the app: `settings.json` (column preferences and optional `site` block for GitHub/metrics settings), `inprogress.json` and `completed.json` (per-issue overlay split by Linear completion state), `metrics_store.json` (Linear transition log and GitHub PR cache). Existing `overlay.json` is migrated once to this split and renamed to `overlay.old`. No database.
- **Frontend:** Jinja templates under `templates/`: landing (`index.html`), dashboard (`dashboard.html`), metrics, settings; shared `nav.html` include. Each page uses embedded `<style>` and `<script>` only (no repo-level `.css`/`.js` bundles). Metrics may load Chart.js from a CDN. No React or other SPA framework.
- **Linear:** Read-only. All requests use `Authorization: <token>` (no "Bearer"). Token from `.env` via `python-dotenv` (`LINEAR_GRAPHQL_API`).
- **GitHub (metrics only):** Read-only REST usage. Token from `SIDECAR_TOKEN` (preferred) or `GITHUB_TOKEN` in the environment / `.env` (see `.env.example`). Never commit tokens. Optional tuning: `SIDECAR_GITHUB_REFRESH_COOLDOWN_SEC` (skip heavy GitHub sync on repeated Refresh), `SIDECAR_GITHUB_PR_META_FRESH_SEC`, `SIDECAR_GITHUB_PULL_PAGES` — see `.env.example`.

## Environment setup

- Copy `.env.example` to `.env`.
- Set `LINEAR_GRAPHQL_API` to the user’s Linear personal API key.
- Run `pip install -r requirements.txt`. Tests use `pytest` and `pytest-mock` (in requirements).

## Key conventions

1. **Linear is read-only.** Never write or update data in Linear from this app.
2. **Overlay data stays local.** All notes, personal priority, and personal status live only in `inprogress.json` and `completed.json` (and settings in `settings.json`). Never send overlay data to Linear or any external service. Metrics aggregates and PR cache stay in `metrics_store.json` locally; do not ship personal overlay contents to third parties.
3. **Refresh must not overwrite overlay.** When the user clicks Refresh, the app refetches from Linear and merges with the existing overlay file. The overlay file is only written when the user explicitly saves from the UI (`POST /api/overlay/<issue_id>`).
4. **Tests required.** Every new feature or fix must include or update unit tests. A task is not done until `pytest tests/` passes.
5. **Templates stay self-contained.** Add new pages as single Jinja files with embedded `<style>` and `<script>`; use `nav.html` for the top bar. Do not add separate CSS/JS asset files or a frontend framework.

## Personal status badge colors

Badge colors in `templates/dashboard.html` follow these rules so the palette stays consistent:

- **Warm colors** (red, amber, orange) = urgent or blocking states (e.g. Blocked, Waiting on Me, Waiting on Someone, Waiting on Testing).
- **Cool colors** (blue, teal, purple, green) = neutral or flow states (e.g. In Progress, Not Started, Testing, Pair Testing, Waiting on Review, Ready to Close).
- **Light neutral colors** (light gray) = undefined or unset (No Status).

When adding or changing a personal status badge, pick a color that matches its meaning under this scheme.

## Branch naming

- Use `feature/Sidecar-###` where `###` is the GitHub issue number or the next PR number (e.g. `feature/Sidecar-12`).
- Branch from `main`.

## Commit and PR format

- **Commit message:** `(#branch-name) type: short description`  
  Examples: `Sidecar-12: feat: add review mode`, `Sidecar-7: fix: overlay not saving on refresh`, `Sidecar-3: chore: add pytest to requirements`.  
  Types: `feat`, `fix`, `chore`, `docs`, `test`, `refactor`.
- **PR title:** Same format as commit messages; reference the issue number.

## Running tests

```bash
pytest tests/
```

All tests must pass before considering a task complete. Mock all Linear API calls; never make real HTTP requests in tests.

## What to avoid

- No external frontend frameworks (React, Vue, etc.).
- No database (only local JSON files: `settings.json`, `inprogress.json`, `completed.json`, `metrics_store.json`).
- No user authentication (single-user local app).
- No background polling (user triggers refresh via the UI).
- No writing to Linear.

## MCP (Cursor)

This project is developed in Cursor with MCP access to **Linear** and **GitHub**.

- **GitHub MCP:** Use it to create feature branches (`feature/Sidecar-###`), open PRs with correct titles and “closes #N”, and check CI status. Prefer MCP over asking the user to do these steps manually.
- **Linear MCP:** Use it to read issue details or update Linear issue status when work starts or completes. **Do not** create Linear issues for this repo’s work—use **GitHub Issues** for Sidecar project tracking.
