"""Unit tests for overlay read/write logic."""

import copy
import json
import pytest
from pathlib import Path

# Import app module and patch OVERLAY_PATH in tests
import app as app_module


@pytest.fixture
def temp_overlay_path(tmp_path):
    """Use a temp file for overlay.json during tests."""
    path = tmp_path / "overlay.json"
    original = app_module.OVERLAY_PATH
    app_module.OVERLAY_PATH = path
    yield path
    app_module.OVERLAY_PATH = original


def test_read_missing_overlay_returns_empty_dict(temp_overlay_path):
    """Reading a non-existent overlay file returns empty dict."""
    assert not temp_overlay_path.exists()
    result = app_module.read_overlay()
    assert result == {}


def test_write_entry_creates_file(temp_overlay_path):
    """Writing one entry creates the overlay file with correct content."""
    app_module.write_overlay_entry("LIN-1", {
        "personal_priority": 2,
        "personal_status": "In Progress",
        "notes": "Some notes",
    })
    assert temp_overlay_path.exists()
    with open(temp_overlay_path, encoding="utf-8") as f:
        data = json.load(f)
    assert "LIN-1" in data
    assert data["LIN-1"]["personal_priority"] == 2
    assert data["LIN-1"]["personal_status"] == "In Progress"
    assert data["LIN-1"]["notes"] == "Some notes"
    assert "last_updated" in data["LIN-1"]


@pytest.mark.parametrize("status", ["Testing", "Pair Testing", "Waiting on Testing"])
def test_write_entry_saves_new_personal_statuses(temp_overlay_path, status):
    """Each of the three new personal status values can be saved to overlay.json without error."""
    app_module.write_overlay_entry("LIN-1", {"personal_status": status})
    assert temp_overlay_path.exists()
    with open(temp_overlay_path, encoding="utf-8") as f:
        data = json.load(f)
    assert "LIN-1" in data
    assert data["LIN-1"]["personal_status"] == status


def test_write_second_entry_preserves_first(temp_overlay_path):
    """Writing a second overlay entry preserves the first."""
    app_module.write_overlay_entry("LIN-1", {"notes": "First"})
    app_module.write_overlay_entry("LIN-2", {"notes": "Second"})
    overlay = app_module.read_overlay()
    assert overlay["LIN-1"]["notes"] == "First"
    assert overlay["LIN-2"]["notes"] == "Second"


def test_write_updates_existing_entry(temp_overlay_path):
    """Writing to an existing issue id updates that entry."""
    app_module.write_overlay_entry("LIN-1", {"notes": "Original"})
    app_module.write_overlay_entry("LIN-1", {"notes": "Updated"})
    overlay = app_module.read_overlay()
    assert len(overlay) == 1
    assert overlay["LIN-1"]["notes"] == "Updated"


def test_merge_with_missing_overlay_keys_uses_defaults():
    """Merging issues with no overlay entry yields defaults for overlay fields."""
    linear_issues = [
        {"id": "uuid-1", "identifier": "LIN-1", "title": "Foo", "linear_status": "In Progress"}
    ]
    overlay = {}
    merged = app_module.merge_issues(linear_issues, overlay)
    assert len(merged) == 1
    assert merged[0]["personal_priority"] is None
    assert merged[0]["personal_status"] == ""
    assert merged[0]["notes"] == ""
    assert merged[0]["last_updated"] is None


def test_merge_with_partial_overlay_entry():
    """Overlay entry with only some keys still merges; missing keys get defaults."""
    linear_issues = [
        {"id": "uuid-1", "identifier": "LIN-1", "title": "Foo"}
    ]
    overlay = {"LIN-1": {"notes": "Only notes"}}
    merged = app_module.merge_issues(linear_issues, overlay)
    assert merged[0]["notes"] == "Only notes"
    assert merged[0]["personal_priority"] is None
    assert merged[0]["personal_status"] == ""


def test_merge_without_overlay_has_last_updated_none():
    """My Last Edit column shows '—' when no overlay entry exists (last_updated is None)."""
    linear_issues = [
        {"id": "uuid-1", "identifier": "LIN-1", "title": "Foo", "linear_status": "Todo"}
    ]
    overlay = {}
    merged = app_module.merge_issues(linear_issues, overlay)
    assert len(merged) == 1
    assert merged[0]["last_updated"] is None


def test_merge_with_overlay_has_last_updated():
    """My Last Edit reflects last_updated from overlay.json when it exists."""
    linear_issues = [
        {"id": "uuid-1", "identifier": "LIN-1", "title": "Foo"}
    ]
    overlay = {"LIN-1": {"notes": "Note", "last_updated": "2025-02-15T14:30:00"}}
    merged = app_module.merge_issues(linear_issues, overlay)
    assert len(merged) == 1
    assert merged[0]["last_updated"] == "2025-02-15T14:30:00"


# --- Personal priority rebalancing (pure functions) ---


def test_rebalance_assign_n_when_n_taken():
    """Assigning priority N when another issue has N shifts that issue and all >= N down by 1."""
    overlay = {
        "LIN-1": {"personal_priority": 1, "notes": "a"},
        "LIN-2": {"personal_priority": 2, "notes": "b"},
        "LIN-3": {"personal_priority": 3, "notes": "c"},
    }
    result = app_module.rebalance_overlay_after_assign(overlay, "LIN-4", 2)
    assert result["LIN-4"]["personal_priority"] == 2
    assert result["LIN-2"]["personal_priority"] == 3
    assert result["LIN-3"]["personal_priority"] == 4
    assert result["LIN-1"]["personal_priority"] == 1
    priorities = [result[k]["personal_priority"] for k in result if result[k].get("personal_priority") is not None]
    assert sorted(priorities) == list(range(1, len(priorities) + 1))


def test_rebalance_assign_n_when_n_free():
    """Assigning priority N when N is free assigns directly with no other changes."""
    overlay = {
        "LIN-1": {"personal_priority": 1, "notes": "a"},
        "LIN-2": {"personal_priority": 3, "notes": "b"},
    }
    result = app_module.rebalance_overlay_after_assign(overlay, "LIN-3", 2)
    assert result["LIN-3"]["personal_priority"] == 2
    assert result["LIN-1"]["personal_priority"] == 1
    assert result["LIN-2"]["personal_priority"] == 3


def test_rebalance_remove_n_closes_gap():
    """Removing priority N decrements everyone above N by 1 so list stays contiguous."""
    overlay = {
        "LIN-1": {"personal_priority": 1, "notes": "a"},
        "LIN-2": {"personal_priority": 2, "notes": "b"},
        "LIN-3": {"personal_priority": 3, "notes": "c"},
        "LIN-4": {"personal_priority": 4, "notes": "d"},
    }
    result = app_module.rebalance_overlay_after_remove(overlay, "LIN-2")
    assert result["LIN-2"].get("personal_priority") is None
    assert result["LIN-1"]["personal_priority"] == 1
    assert result["LIN-3"]["personal_priority"] == 2
    assert result["LIN-4"]["personal_priority"] == 3


def test_rebalance_does_not_mutate_input():
    """Rebalance functions return a new dict and do not mutate the input overlay."""
    overlay = {"LIN-1": {"personal_priority": 1, "notes": "a"}}
    orig = copy.deepcopy(overlay)
    result = app_module.rebalance_overlay_after_assign(overlay, "LIN-2", 2)
    assert result != overlay
    assert overlay == orig
    result2 = app_module.rebalance_overlay_after_remove(overlay, "LIN-1")
    assert overlay == orig


def test_resolve_priority_conflicts_produces_contiguous():
    """If two entries share the same priority, resolve_priority_conflicts produces contiguous 1,2,3."""
    overlay = {
        "LIN-1": {"personal_priority": 2, "last_updated": "2025-02-10T10:00:00"},
        "LIN-2": {"personal_priority": 2, "last_updated": "2025-02-15T10:00:00"},
        "LIN-3": {"personal_priority": 3, "last_updated": "2025-02-12T10:00:00"},
    }
    result = app_module.resolve_priority_conflicts(overlay)
    priorities = [result[k]["personal_priority"] for k in result]
    assert len(priorities) == len(set(priorities))
    assert sorted(priorities) == [1, 2, 3]
    # Newest (LIN-2) gets 1, then LIN-3, then LIN-1
    assert result["LIN-2"]["personal_priority"] == 1
    assert result["LIN-3"]["personal_priority"] == 2
    assert result["LIN-1"]["personal_priority"] == 3


def test_read_overlay_resolves_conflicts_and_returns_contiguous(temp_overlay_path):
    """On load, if overlay has duplicate priorities, read_overlay returns resolved overlay without error."""
    app_module.write_overlay({
        "LIN-1": {"personal_priority": 1, "last_updated": "2025-02-10T10:00:00"},
        "LIN-2": {"personal_priority": 1, "last_updated": "2025-02-15T10:00:00"},
    })
    result = app_module.read_overlay()
    priorities = [result[k]["personal_priority"] for k in result if result[k].get("personal_priority") is not None]
    assert len(priorities) == len(set(priorities))
    assert sorted(priorities) == [1, 2]


def test_change_existing_priority_rebalances():
    """Changing an issue's priority from K to N reassigns correctly and rebalances the rest."""
    overlay = {
        "LIN-1": {"personal_priority": 1, "notes": "a"},
        "LIN-2": {"personal_priority": 2, "notes": "b"},
        "LIN-3": {"personal_priority": 3, "notes": "c"},
    }
    # Move LIN-3 from 3 to 1; LIN-1 and LIN-2 shift down
    result = app_module.rebalance_overlay_after_assign(overlay, "LIN-3", 1)
    assert result["LIN-3"]["personal_priority"] == 1
    assert result["LIN-1"]["personal_priority"] == 2
    assert result["LIN-2"]["personal_priority"] == 3
