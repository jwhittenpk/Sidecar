## v0.2.2

- Merge pull request #11 from jwhittenpk/feature/sidecar-10
- Sidecar-10: feat: add Linear Status badge colors
- chore: update CHANGELOG and VERSION for v0.2.1

## v0.2.1

- Merge pull request #10 from jwhittenpk/feature/Sidecar-9
- Sidecar-9: test: unit tests for move-to-last and cache invalidation; docs: README features and project structure
- Sidecar-9: fix: personal priority reorder, dropdown size, overlay sync, and cache invalidation
- chore: update CHANGELOG and VERSION for v0.2.0

## v0.2.0

- Merge pull request #9 from jwhittenpk/feature/Sidecar-8
- Sidecar-8: show version in header (vX.Y.Z), move gear to right
- Sidecar-8: feat: column reorder, preferences persistence, filter popover updates
- Sidecar-8: feat: Customizable columns
- chore: update CHANGELOG for v0.1.5

## v0.1.5

- Merge pull request #8 from jwhittenpk/feature/Sidecar-7
- Sidecar-7: feat: New Personal Status colors
- Sidecar-7: feat: add new personal status values (Testing, Pair Testing, Waiting on Testing)
- chore: update CHANGELOG for v0.1.4

## v0.1.4

- Merge pull request #7 from jwhittenpk/feature/Sidecar-6
- Sidecar-6: fix: Search box behaves as expected
- chore: update CHANGELOG for v0.1.3

## v0.1.3

- Merge pull request #6 from jwhittenpk/feature/Sidecar-5
- Sidecar-6: Remove sort dropdown.  Fix filter button.
- Sidecar-5: feat: filter popover, filter options, sortable column headers
- chore: update CHANGELOG for v0.1.2

## v0.1.2

- Merge pull request #5 from jwhittenpk/feature/Sidecar-4
- Sidecar-4: feat: New personal statuses.  fix: inaccurate click
- Sidecar-4: Update AGENTS.md
- feat: ordered personal priority, remove team column, default sort, date display (#4)
- chore: update CHANGELOG for v0.1.1

## v0.1.1

- Merge pull request #4 from jwhittenpk/feature/Sidecar-3
- feat: Active/Completed tabs, split Updated columns, Cycle sort (#3)
- chore: update CHANGELOG for v0.1.0

## v0.1.0

- Merge pull request #3 from jwhittenpk/feature/Sidecar-2
- Sidecar-2: Fix 403 error when pushing to release
- Merge pull request #1 from jwhittenpk/feature/Sidecar-1
- Sidecar-1: Initial commit of application
- Initial commit

# Changelog

## [Unreleased]

## v0.2.0

- feat: customizable column system
- Column registry (single source of truth) with default and optional columns
- Gear icon: column visibility panel; show/hide optional columns (Cycle, Team, Labels)
- Column visibility persisted in overlay.json; GET/POST /api/config/columns
- Dynamic filter popover: only shows filters for currently visible columns
- All visible columns sortable (identifier, title, cycle, team, labels, notes, etc.)
- Optional columns: Cycle, Team, Labels (from Linear); labels as colored badges
- Identifier and Title always visible; at least one other column required
