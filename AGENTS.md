# Agent guidance for Sidecar

This file is for AI coding agents (e.g. Cursor) working on this codebase.

## Project summary

**Sidecar** is a personal overlay dashboard for Linear tickets. It runs locally (Flask on localhost:5000), fetches issues assigned to the user from the Linear GraphQL API (**read-only**), and stores **personal context** (notes, personal priority, personal status) in a single local JSON file (`overlay.json`). It is not a Linear replacement; it is a private overlay that never sends personal data to Linear or anywhere else.

## Architecture

- **Backend:** Python, Flask. Single module `app.py`: routes, Linear API client, overlay file I/O, merge logic.
- **Data:** One JSON file `overlay.json` next to the app; keyed by Linear issue ID (e.g. `LIN-123`). No database.
- **Frontend:** One HTML file `templates/index.html` with embedded CSS and JavaScript. No React, no build tools, no separate JS/CSS files.
- **Linear:** Read-only. All requests use `Authorization: <token>` (no "Bearer"). Token from `.env` via `python-dotenv` (`LINEAR_GRAPHQL_API`).

## Environment setup

- Copy `.env.example` to `.env`.
- Set `LINEAR_GRAPHQL_API` to the user’s Linear personal API key.
- Run `pip install -r requirements.txt`. Tests use `pytest` and `pytest-mock` (in requirements).

## Key conventions

1. **Linear is read-only.** Never write or update data in Linear from this app.
2. **Overlay data stays local.** All notes, personal priority, and personal status live only in `overlay.json`. Never send overlay data to Linear or any external service.
3. **Refresh must not overwrite overlay.** When the user clicks Refresh, the app refetches from Linear and merges with the existing overlay file. The overlay file is only written when the user explicitly saves from the UI (`POST /api/overlay/<issue_id>`).
4. **Tests required.** Every new feature or fix must include or update unit tests. A task is not done until `pytest tests/` passes.
5. **Single frontend file.** Keep the UI in one `templates/index.html` with embedded `<style>` and `<script>`. Do not split into separate CSS/JS files or introduce a frontend framework.

## Personal status badge colors

Badge colors in `templates/index.html` follow these rules so the palette stays consistent:

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
- No database (only `overlay.json`).
- No user authentication (single-user local app).
- No background polling (user triggers refresh via the UI).
- No writing to Linear.

## MCP (Cursor)

This project is developed in Cursor with MCP access to **Linear** and **GitHub**.

- **GitHub MCP:** Use it to create feature branches (`feature/Sidecar-###`), open PRs with correct titles and “closes #N”, and check CI status. Prefer MCP over asking the user to do these steps manually.
- **Linear MCP:** Use it to read issue details or update Linear issue status when work starts or completes. **Do not** create Linear issues for this repo’s work—use **GitHub Issues** for Sidecar project tracking.
