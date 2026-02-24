"""Unit tests for overlay read/write logic."""

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
