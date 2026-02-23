import os
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
from baseline_manager import rebuild_baseline_from_csv, load_baseline, update_with_row, expected_power, apply_decay


def make_csv(tmp_path, rows):
    fn = tmp_path / "test_electric.csv"
    df = pd.DataFrame(rows)
    df.to_csv(fn, index=False)
    return str(fn)


def test_rebuild_and_expected(tmp_path):
    # Create synthetic rows across two weekdays and hours
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    rows = []
    # Monday (weekday 0) hour 9 -> values 100, 200
    monday = now - timedelta(days=(now.weekday() - 0))
    rows.append({"time": monday.isoformat(), "power_mW": 100})
    rows.append({"time": (monday + timedelta(hours=0)).isoformat(), "power_mW": 200})
    # Tuesday (weekday 1) hour 9 -> 50
    tuesday = now - timedelta(days=(now.weekday() - 1))
    rows.append({"time": tuesday.isoformat(), "power_mW": 50})

    fn = make_csv(tmp_path, rows)
    bl = rebuild_baseline_from_csv(csv_path=fn)
    assert bl is not None

    # expected for Monday hour should be 150
    m_hour = str(datetime.fromisoformat(rows[0]["time"]).hour)
    wd = str(datetime.fromisoformat(rows[0]["time"]).weekday())
    monday_mean = bl["by_weekday_hour"][wd][m_hour]["mean"]
    assert abs(monday_mean - 150) < 1e-6

    # expected_power for monday timestamp
    ep = expected_power(datetime.fromisoformat(rows[0]["time"]), bl)
    assert abs(ep - 150) < 1e-6


def test_update_and_decay(tmp_path):
    # Start with empty baseline file
    path = tmp_path / "baseline.json"
    if path.exists():
        os.remove(path)

    # Update with a row
    ts = datetime.now(timezone.utc)
    update_with_row({"time": ts.isoformat(), "power_mW": 300}, baseline_path=str(path))
    bl = load_baseline(str(path))
    hour = str(ts.hour)
    wd = str(ts.weekday())
    assert bl["hourly"][hour]["count"] >= 1
    assert bl["by_weekday_hour"][wd][hour]["count"] >= 1

    # Apply decay and verify counts/sums reduced
    before_sum = bl["hourly"][hour]["sum"]
    bl2 = apply_decay(decay_factor=0.5, path=str(path))
    after_sum = bl2["hourly"][hour]["sum"]
    assert after_sum < before_sum + 1e-6
