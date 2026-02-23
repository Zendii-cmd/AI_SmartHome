"""billing_predictor.py

Functions to compute monthly energy consumption (kWh) from time-series power readings
and to project monthly consumption and cost.

Approach:
- Parse `electric_data.csv` (time in UTC, power_mW).
- Integrate power over time using piecewise-constant assumption between measurements.
- Clip intervals to month boundaries when computing monthly totals.
- Projection: use average daily consumption observed so far to predict remaining days.
- Cost: simple flat rate (VND per kWh) read from env `ELECTRICITY_PRICE` or default.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
import os
import pandas as pd

DEFAULT_PRICE_VND_PER_KWH = float(os.environ.get("ELECTRICITY_PRICE", 3000))  # default VND/kWh
CSV_FILE = "electric_data.csv"


def _month_bounds(year: int, month: int):
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def _read_power_df(csv_path: str = CSV_FILE) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["time"], header=0)
    from time_utils import parse_time_series
    df["time"] = parse_time_series(df["time"])
    df["power_mW"] = pd.to_numeric(df.get("power_mW"), errors="coerce")
    df = df.dropna(subset=["time", "power_mW"]).copy()
    df = df.sort_values("time")
    return df


def _integrate_energy_for_interval(p_mw: float, t0: datetime, t1: datetime) -> float:
    """Return energy in Wh for interval [t0, t1) assuming constant power p_mw (milliwatts)."""
    seconds = (t1 - t0).total_seconds()
    hours = seconds / 3600.0
    watts = p_mw / 1000.0
    wh = watts * hours
    return wh


def compute_monthly_energy_kwh(year: int, month: int, csv_path: str = CSV_FILE) -> float:
    """Compute total energy in kWh for the given month from CSV data."""
    start, end = _month_bounds(year, month)
    df = _read_power_df(csv_path)
    if df.empty:
        return 0.0

    # If first measurement is after start, we assume previous state unknown: we start integration from first measurement
    total_wh = 0.0

    # Iterate over intervals defined by consecutive rows
    for i in range(len(df) - 1):
        t0 = df["time"].iloc[i]
        t1 = df["time"].iloc[i + 1]
        # Clip the interval to [start, end)
        if t1 <= start or t0 >= end:
            continue
        clip_t0 = max(t0, start)
        clip_t1 = min(t1, end)
        if clip_t1 <= clip_t0:
            continue
        p_mw = df["power_mW"].iloc[i]
        total_wh += _integrate_energy_for_interval(p_mw, clip_t0, clip_t1)

    # Edge case: if only single row in month, can't integrate; assume last reading holds until end or 0.
    # We'll ignore extrapolation to avoid overestimation.

    return total_wh / 1000.0  # kWh


def compute_daily_energy_series(csv_path: str = CSV_FILE, days: Optional[int] = None) -> pd.Series:
    """Return series indexed by date (UTC date) with daily energy in kWh.

    If days is provided, return last `days` days up to the latest timestamp in CSV.
    """
    df = _read_power_df(csv_path)
    if df.empty:
        return pd.Series(dtype=float)

    # Build energy per interval and assign to the day of the interval start (UTC)
    records = []
    for i in range(len(df) - 1):
        t0 = df["time"].iloc[i]
        t1 = df["time"].iloc[i + 1]
        p_mw = df["power_mW"].iloc[i]
        wh = _integrate_energy_for_interval(p_mw, t0, t1)
        # allocate energy to the day of t0 (UTC)
        day = t0.date()
        records.append((day, wh / 1000.0))  # kWh

    if not records:
        return pd.Series(dtype=float)

    s = pd.DataFrame(records, columns=["date", "kwh"]).groupby("date")["kwh"].sum()
    s.index = pd.to_datetime(s.index).date
    if days is not None:
        last = df["time"].iloc[-1].date()
        first = last - timedelta(days=days - 1)
        idx = pd.date_range(first, last).date
        s = s.reindex(idx, fill_value=0.0)
    return s


def project_monthly_energy_and_cost(as_of: Optional[datetime] = None, price_vnd_per_kwh: Optional[float] = None, csv_path: str = CSV_FILE) -> Dict:
    """Project total kWh and cost for the current month based on data so far.

    Returns a dict: {
      'month': 'YYYY-MM',
      'kwh_so_far': float,
      'projected_kwh': float,
      'projected_cost_vnd': float,
      'confidence_interval_kwh': (low, high)
    }
    """
    if price_vnd_per_kwh is None:
        price_vnd_per_kwh = DEFAULT_PRICE_VND_PER_KWH

    if as_of is None:
        as_of = datetime.now(timezone.utc)
    year = as_of.year
    month = as_of.month
    start, end = _month_bounds(year, month)

    kwh_so_far = compute_monthly_energy_kwh(year, month, csv_path=csv_path)

    # Determine days in month and days elapsed
    days_in_month = (end - start).days
    elapsed_days = (as_of - start).total_seconds() / (24 * 3600)
    elapsed_days = max(elapsed_days, 1.0 / 24.0)  # at least 1 hour to avoid div0

    # Compute daily series for days observed so far
    daily = compute_daily_energy_series(csv_path=csv_path, days=int(max(1, int(elapsed_days) + 1)))
    if daily.empty:
        mean_daily = kwh_so_far / elapsed_days
        std_daily = 0.0
        n_days = max(1, int(elapsed_days))
    else:
        # Use days up to as_of.date()
        # Filter days <= as_of.date()
        daily_obs = daily[daily.index <= as_of.date()]
        if daily_obs.empty:
            mean_daily = kwh_so_far / elapsed_days
            std_daily = 0.0
            n_days = max(1, int(elapsed_days))
        else:
            mean_daily = float(daily_obs.mean())
            std_daily = float(daily_obs.std(ddof=0)) if len(daily_obs) > 1 else 0.0
            n_days = len(daily_obs)

    remaining_days = days_in_month - elapsed_days
    if remaining_days < 0:
        remaining_days = 0

    projected_kwh = kwh_so_far + mean_daily * remaining_days

    # Confidence interval (approx): use normal approx of mean_daily * remaining_days
    import math
    if n_days > 0 and std_daily > 0:
        se = std_daily / math.sqrt(n_days)
        margin = 1.96 * se * remaining_days
        ci_low = max(0.0, projected_kwh - margin)
        ci_high = projected_kwh + margin
    else:
        ci_low = max(0.0, projected_kwh * 0.9)
        ci_high = projected_kwh * 1.1

    projected_cost = projected_kwh * price_vnd_per_kwh

    res = {
        "month": f"{year:04d}-{month:02d}",
        "kwh_so_far": kwh_so_far,
        "projected_kwh": projected_kwh,
        "projected_cost_vnd": projected_cost,
        "confidence_interval_kwh": (ci_low, ci_high),
        "price_vnd_per_kwh": price_vnd_per_kwh,
    }
    return res


def format_monthly_report(res: Dict) -> str:
    """Return a human-readable report string from the projection dict."""
    month = res.get("month")
    kwh = res.get("kwh_so_far", 0.0)
    proj = res.get("projected_kwh", 0.0)
    low, high = res.get("confidence_interval_kwh", (0.0, 0.0))
    price = res.get("price_vnd_per_kwh")
    cost = res.get("projected_cost_vnd", 0.0)

    lines = [
        f"[Billing Report] Month: {month}",
        f" - kWh so far: {kwh:.3f} kWh",
        f" - Projected kWh: {proj:.3f} kWh (CI: {low:.3f} - {high:.3f})",
        f" - Projected cost: {cost:.0f} VND (@ {price:.0f} VND/kWh)"
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Billing predictor CLI")
    parser.add_argument("--month", type=str, default=None, help="YYYY-MM to compute (default: current month)")
    parser.add_argument("--price", type=float, default=None, help="VND per kWh")
    args = parser.parse_args()

    if args.month:
        y, m = map(int, args.month.split("-"))
        kwh = compute_monthly_energy_kwh(y, m)
        print(f"Energy in {args.month}: {kwh:.3f} kWh | cost {kwh * (args.price or DEFAULT_PRICE_VND_PER_KWH):.0f} VND")
    else:
        res = project_monthly_energy_and_cost(price_vnd_per_kwh=args.price)
        print(res)