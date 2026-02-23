import os
from datetime import datetime, timedelta, timezone
import pandas as pd
from billing_predictor import compute_monthly_energy_kwh, project_monthly_energy_and_cost

CSV = "electric_data.csv"


def write_rows(rows):
    df = pd.DataFrame(rows)
    df.to_csv(CSV, index=False)


def test_compute_monthly_energy_simple(tmp_path):
    # Create synthetic data: constant 1000 mW (1 W) for 24 hours -> 24 Wh = 0.024 kWh
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = []
    # one measurement every hour, constant power 1000 mW
    for i in range(25):
        rows.append({"time": (now + timedelta(hours=i)).isoformat(), "power_mW": 1000})
    write_rows(rows)

    year = now.year
    month = now.month
    kwh = compute_monthly_energy_kwh(year, month, csv_path=CSV)
    # energy should be ~24 Wh = 0.024 kWh for the first day (since we only have 24h of data)
    assert abs(kwh - 0.024) < 1e-6


def test_project_monthly_energy_and_cost(tmp_path):
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = []
    # 3 days of consistent 1000 mW -> 24*3 Wh = 72 Wh = 0.072 kWh
    for i in range(4*24):
        rows.append({"time": (now + timedelta(hours=i)).isoformat(), "power_mW": 1000})
    write_rows(rows)

    res = project_monthly_energy_and_cost(as_of=now + timedelta(days=3), price_vnd_per_kwh=2000, csv_path=CSV)
    assert "kwh_so_far" in res
    assert res["projected_cost_vnd"] >= 0
