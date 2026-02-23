import os
import json
from baseline_manager import save_baseline, load_baseline, update_with_row


def test_save_and_load_lock(tmp_path):
    path = tmp_path / "baseline.json"
    bl = {"hourly": {str(i): {"count": 1, "sum": 10.0, "mean": 10.0} for i in range(24)}, "by_weekday_hour": {}, "last_updated": None}
    save_baseline(bl, path=str(path))
    loaded = load_baseline(path=str(path))
    assert loaded.get("hourly") is not None


def test_update_with_row_lock(tmp_path):
    path = tmp_path / "baseline.json"
    # ensure baseline file exists
    save_baseline({"hourly": {}, "by_weekday_hour": {str(d): {} for d in range(7)}, "last_updated": None}, path=str(path))
    update_with_row({"time": "2026-02-01T12:00:00+00:00", "power_mW": 123.4}, baseline_path=str(path))
    bl = load_baseline(path=str(path))
    h = "12"
    wd = "0"
    assert bl["hourly"][h]["count"] >= 1
