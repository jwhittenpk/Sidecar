"""Shared pytest fixtures for overlay split (settings.json, inprogress.json, completed.json)."""

import json
import pytest

import app as app_module


def _default_settings():
    default_vis = {c["id"]: c["default_visible"] for c in app_module.COLUMN_REGISTRY}
    return {
        app_module.COLUMN_PREFERENCES_KEY: {
            "order": list(app_module.DEFAULT_COLUMN_ORDER),
            "visibility": default_vis,
        }
    }


@pytest.fixture
def temp_overlay_path(tmp_path):
    """Patch overlay paths to tmp_path and create split layout (settings, inprogress, completed).
    Use for tests that write/read overlay or column preferences."""
    orig = (
        app_module.SETTINGS_PATH,
        app_module.INPROGRESS_PATH,
        app_module.COMPLETED_PATH,
        app_module.OVERLAY_LEGACY_PATH,
        app_module.OVERLAY_OLD_PATH,
    )
    app_module.SETTINGS_PATH = tmp_path / "settings.json"
    app_module.INPROGRESS_PATH = tmp_path / "inprogress.json"
    app_module.COMPLETED_PATH = tmp_path / "completed.json"
    app_module.OVERLAY_LEGACY_PATH = tmp_path / "overlay.json"
    app_module.OVERLAY_OLD_PATH = tmp_path / "overlay.old"
    (tmp_path / "settings.json").write_text(json.dumps(_default_settings(), indent=2), encoding="utf-8")
    (tmp_path / "inprogress.json").write_text("{}", encoding="utf-8")
    (tmp_path / "completed.json").write_text("{}", encoding="utf-8")
    yield tmp_path
    app_module.SETTINGS_PATH = orig[0]
    app_module.INPROGRESS_PATH = orig[1]
    app_module.COMPLETED_PATH = orig[2]
    app_module.OVERLAY_LEGACY_PATH = orig[3]
    app_module.OVERLAY_OLD_PATH = orig[4]


@pytest.fixture
def temp_overlay_path_no_files(tmp_path):
    """Patch overlay paths only; do not create files. Use for test_read_missing_overlay_returns_empty_dict."""
    orig = (
        app_module.SETTINGS_PATH,
        app_module.INPROGRESS_PATH,
        app_module.COMPLETED_PATH,
        app_module.OVERLAY_LEGACY_PATH,
        app_module.OVERLAY_OLD_PATH,
    )
    app_module.SETTINGS_PATH = tmp_path / "settings.json"
    app_module.INPROGRESS_PATH = tmp_path / "inprogress.json"
    app_module.COMPLETED_PATH = tmp_path / "completed.json"
    app_module.OVERLAY_LEGACY_PATH = tmp_path / "overlay.json"
    app_module.OVERLAY_OLD_PATH = tmp_path / "overlay.old"
    yield tmp_path
    app_module.SETTINGS_PATH = orig[0]
    app_module.INPROGRESS_PATH = orig[1]
    app_module.COMPLETED_PATH = orig[2]
    app_module.OVERLAY_LEGACY_PATH = orig[3]
    app_module.OVERLAY_OLD_PATH = orig[4]
