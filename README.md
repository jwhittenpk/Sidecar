# Sidecar — A personal overlay dashboard for your Linear tickets

[![Tests](https://github.com/jwhittenpk/Sidecar/actions/workflows/test.yml/badge.svg)](https://github.com/jwhittenpk/Sidecar/actions/workflows/test.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight, locally-run personal dashboard for software engineers who use [Linear](https://linear.app). Sidecar pulls your assigned Linear issues and lets you add **private context** that never leaves your machine: personal priority, personal status, and notes. It is **not** a replacement for Linear—it’s a personal overlay on top of it.

**Screenshot:** *Screenshot coming soon.*

---

## Features

- **Sync with Linear** — Fetches issues assigned to you (active + completed/cancelled in the last 6 months) via the Linear GraphQL API (read-only).
- **Personal priority** — Assign your own priority (1–5) per ticket, independent of Linear’s priority.
- **Personal status** — Track “In Progress”, “Blocked”, “Waiting On Someone”, “Ready to Close”, etc.
- **Private notes** — Add notes like “waiting on backend” or “blocked by X”; stored only in a local JSON file.
- **Completed tickets** — View closed/cancelled tickets with all your notes for performance reviews or follow-ups.
- **Filter & sort** — Filter by All / Active only / Completed only; sort by personal priority, Linear priority, status, or last updated; search by title or notes.

---

## Prerequisites

- **Python 3.9+**
- A [Linear](https://linear.app) account
- A Linear **personal API key** (see below)

---

## Installation & setup

1. **Clone the repo**
   ```bash
   git clone https://github.com/jwhittenpk/Sidecar.git
   cd Sidecar
   ```

2. **Create your environment file**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and set your Linear API token. Either paste the key directly, or point to the file that contains it (e.g. the same file you use in your shell):
   ```
   LINEAR_GRAPHQL_API=your_linear_personal_api_key_here
   ```
   Or, to use the same file as in your shell (e.g. `export LINEAR_GRAPHQL_API=$(cat "...")` in zshrc):
   ```
   LINEAR_GRAPHQL_API_FILE=/full/path/to/linear_graphql_api.txt
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the app**
   ```bash
   python app.py
   ```
   Open [http://localhost:5000](http://localhost:5000) in your browser.

---

## How to get a Linear API token

1. In Linear, go to **Settings** (gear icon) → **API** → **Personal API Keys**.
2. Create a new key and copy it.
3. Paste it into `.env` as `LINEAR_GRAPHQL_API=...` (no quotes).

Never commit `.env` or share your token; it has access to your Linear data.

---

## Project structure

| Path | Description |
|------|-------------|
| `app.py` | Flask app: routes, Linear API client, overlay read/write, merge logic |
| `overlay.json` | Local file storing your notes, personal priority, and personal status (created on first save; do not commit) |
| `.env` | Your Linear API key (do not commit) |
| `templates/index.html` | Single-page dashboard UI (HTML + embedded CSS and JS) |
| `tests/` | Unit tests for routes, overlay, and Linear parsing |
| `.github/workflows/` | CI: tests on PRs, release + CHANGELOG on push to main |

---

## Contributing

Contributions are welcome. Please open a pull request; **all PRs must pass the unit tests** (`pytest tests/`) before merge.

---

## License

MIT. See [LICENSE](LICENSE).
