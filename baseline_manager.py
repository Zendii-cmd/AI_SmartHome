"""baseline_manager.py

Functions to compute and maintain baseline power consumption profiles
- Hourly baseline (mean) from historical CSV data
- Persist baseline to `baseline.json` with counts/sums for incremental updates
- Helpers: load_baseline(), save_baseline(), rebuild_baseline_from_csv(), update_with_row(), expected_power()
"""
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
import os
import pandas as pd
from filelock import FileLock, Timeout

# Logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

LOCK_TIMEOUT = 5  # seconds

BASELINE_FILE = "baseline.json"
CSV_FILE = "electric_data.csv"


def _default_baseline_structure() -> Dict:
    # hourly: legacy per-hour baseline
    # by_weekday_hour: weekday (0=Mon .. 6=Sun) -> hour -> stats
    return {
        "hourly": {str(h): {"count": 0, "sum": 0.0, "mean": None} for h in range(24)},
        "by_weekday_hour": {str(d): {str(h): {"count": 0, "sum": 0.0, "mean": None} for h in range(24)} for d in range(7)},
        "last_updated": None
    }


def load_baseline(path: str = BASELINE_FILE, use_lock: bool = True) -> Dict:
    lock_path = f"{path}.lock"
    if not os.path.exists(path):
        logger.debug("Baseline file missing, returning default structure")
        return _default_baseline_structure()

    try:
        if use_lock:
            lock = FileLock(lock_path, timeout=LOCK_TIMEOUT)
            with lock:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
        else:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        return data
    except Timeout:
        logger.warning("Timeout acquiring lock for loading baseline; returning default structure")
        return _default_baseline_structure()
    except Exception:
        logger.exception("Error reading baseline file; returning default structure")
        return _default_baseline_structure()


def save_baseline(baseline: Dict, path: str = BASELINE_FILE, use_lock: bool = True) -> None:
    baseline["last_updated"] = datetime.now(timezone.utc).isoformat()
    lock_path = f"{path}.lock"
    try:
        if use_lock:
            lock = FileLock(lock_path, timeout=LOCK_TIMEOUT)
            with lock:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(baseline, f, indent=2)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except Exception:
                        logger.exception("fsync failed when saving baseline")
        else:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(baseline, f, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    logger.exception("fsync failed when saving baseline")
        logger.info("Baseline saved to %s", path)
    except Timeout:
        logger.warning("Timeout acquiring lock for saving baseline; skipping save")
    except Exception:
        logger.exception("Error saving baseline to %s", path)


from datetime import timedelta


def rebuild_baseline_from_csv(csv_path: str = CSV_FILE, days_window: Optional[int] = None, min_count: int = 1) -> Optional[Dict]:
    """Recompute baseline means from the CSV file and return baseline dict.

    - days_window: if set, only use rows within the last `days_window` days
    - min_count: for a (weekday,hour) bucket to be considered valid it must have at least min_count rows

    Returns None if CSV missing/no valid data.
    """
    if not os.path.exists(csv_path):
        return None

    try:
        df = pd.read_csv(csv_path, parse_dates=["time"], header=0)
        from time_utils import parse_time_series
        df["time"] = parse_time_series(df["time"]) 
    except Exception:
        # Try headerless read
        try:
            df = pd.read_csv(csv_path, header=None, names=["time", "power_mW", "current_mA", "voltage", "led1", "led2"])
            from time_utils import parse_time_series
            df["time"] = parse_time_series(df["time"]) 
        except Exception:
            return None

    df["power_mW"] = pd.to_numeric(df.get("power_mW"), errors="coerce")
    df = df.dropna(subset=["time", "power_mW"]).copy()
    if df.empty:
        return None

    if days_window is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_window)
        df = df[df["time"] >= cutoff]
        if df.empty:
            return None

    # Build both hourly and weekday-hour baselines
    baseline = _default_baseline_structure()

    # Hourly (legacy)
    df["hour"] = df["time"].dt.hour
    hg = df.groupby("hour")["power_mW"].agg(["count", "sum", "mean"]).to_dict(orient="index")
    for h, stats in hg.items():
        baseline["hourly"][str(int(h))]["count"] = int(stats["count"])
        baseline["hourly"][str(int(h))]["sum"] = float(stats["sum"])
        baseline["hourly"][str(int(h))]["mean"] = float(stats["mean"])

    # Weekday-hour
    df["weekday"] = df["time"].dt.weekday  # 0=Mon
    wgh = df.groupby(["weekday", "hour"])["power_mW"].agg(["count", "sum", "mean"]).to_dict(orient="index")
    for (wd, h), stats in wgh.items():
        wd_s, h_s = str(int(wd)), str(int(h))
        if int(stats["count"]) >= min_count:
            baseline["by_weekday_hour"][wd_s][h_s]["count"] = int(stats["count"])
            baseline["by_weekday_hour"][wd_s][h_s]["sum"] = float(stats["sum"])
            baseline["by_weekday_hour"][wd_s][h_s]["mean"] = float(stats["mean"])

    save_baseline(baseline)
    return baseline


def apply_decay(decay_factor: float = 0.9, path: str = BASELINE_FILE) -> Dict:
    """Apply decay to baseline sums/counts to slowly discount older data.

    Multiply 'sum' and 'count' by decay_factor and recompute means. Returns updated baseline.
    """
    baseline = load_baseline(path)
    for hour, entry in baseline.get("hourly", {}).items():
        entry["count"] = entry.get("count", 0) * decay_factor
        entry["sum"] = entry.get("sum", 0.0) * decay_factor
        entry["mean"] = (entry["sum"] / entry["count"]) if entry["count"] > 0 else None

    for wd, hours in baseline.get("by_weekday_hour", {}).items():
        for hour, entry in hours.items():
            entry["count"] = entry.get("count", 0) * decay_factor
            entry["sum"] = entry.get("sum", 0.0) * decay_factor
            entry["mean"] = (entry["sum"] / entry["count"]) if entry["count"] > 0 else None

    save_baseline(baseline, path)
    return baseline


# Scheduler utilities
import threading
import time
import queue

_scheduler_thread = None
_scheduler_stop = False

# Writer-queue utilities (single writer thread to serialize baseline writes)
_writer_queue: "queue.Queue[Dict]" = queue.Queue()
_writer_thread: threading.Thread | None = None
_writer_stop_event = threading.Event()
_WRITER_FLUSH_INTERVAL = 2.0  # seconds
_writer_lock = threading.Lock()
_FLUSH_TOKEN = object()


def start_baseline_writer(flush_interval: float = _WRITER_FLUSH_INTERVAL):
    """Start the background writer thread that consumes updates from an in-memory queue
    and persists them to baseline.json. Safe to call multiple times."""
    global _writer_thread, _writer_stop_event
    if _writer_thread and _writer_thread.is_alive():
        return
    _writer_stop_event.clear()
    _writer_thread = threading.Thread(target=_writer_loop, args=(flush_interval,), daemon=True)
    _writer_thread.start()


def stop_baseline_writer(timeout: float = 5.0):
    """Signal the writer to stop, flush remaining items, and join the thread."""
    global _writer_thread, _writer_stop_event
    _writer_stop_event.set()
    # wake the thread if it's waiting
    try:
        _writer_queue.put_nowait(None)
    except Exception:
        pass
    if _writer_thread:
        _writer_thread.join(timeout=timeout)
        _writer_thread = None


def _writer_loop(flush_interval: float):
    """Consume queued rows and update baseline in memory, flushing to disk periodically.

    The queue accepts dict rows; a sentinel value of None indicates a stop/flush request.
    """
    last_flush = time.time()
    # Load baseline into memory to avoid repeated loads
    try:
        baseline = load_baseline()
    except Exception:
        baseline = _default_baseline_structure()

    dirty = False

    while not _writer_stop_event.is_set():
        try:
            item = _writer_queue.get(timeout=flush_interval)
        except queue.Empty:
            item = None

        # Handle flush sentinel explicitly
        if item is _FLUSH_TOKEN:
            try:
                logger.debug("Writer received FLUSH token; saving baseline")
                save_baseline(baseline)
                dirty = False
                last_flush = time.time()
            except Exception:
                logger.exception("Writer failed to save baseline on FLUSH token")
            continue

        if item is None:
            # Timeout; flush if dirty
            if dirty:
                try:
                    logger.debug("Writer periodic flush: saving baseline (dirty=%s)", dirty)
                    save_baseline(baseline)
                    dirty = False
                except Exception:
                    logger.exception("Writer failed to save baseline during periodic flush")
            if _writer_stop_event.is_set():
                break
            continue

        # Process row update (non-blocking, only update in-memory baseline)
        try:
            row = item
            # parse timestamp like update_with_row
            try:
                ts = row.get("time")
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                elif not isinstance(ts, datetime):
                    ts = datetime.now(timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)

            try:
                power = float(row.get("power_mW"))
            except Exception:
                logger.debug("Writer skipping baseline update: invalid power value")
                continue

            hour = str(ts.hour)
            wd = str(ts.weekday())

            with _writer_lock:
                # update legacy hourly
                entry = baseline["hourly"].get(hour)
                if entry is None:
                    entry = {"count": 0, "sum": 0.0, "mean": None}
                    baseline["hourly"][hour] = entry
                entry["count"] = entry.get("count", 0) + 1
                entry["sum"] = entry.get("sum", 0.0) + power
                entry["mean"] = entry["sum"] / entry["count"]

                # update weekday-hour
                wentry = baseline["by_weekday_hour"][wd].get(hour)
                if wentry is None:
                    wentry = {"count": 0, "sum": 0.0, "mean": None}
                    baseline["by_weekday_hour"][wd][hour] = wentry
                wentry["count"] = wentry.get("count", 0) + 1
                wentry["sum"] = wentry.get("sum", 0.0) + power
                wentry["mean"] = wentry["sum"] / wentry["count"]

                dirty = True

            # Consider flushing after each processed item to ensure persistence in tests and on stop
            try:
                logger.debug("Writer saving baseline after processing item")
                save_baseline(baseline)
                dirty = False
                last_flush = time.time()
            except Exception:
                logger.exception("Writer failed to save baseline after processing item")

        except Exception:
            logger.exception("Error processing queued baseline update")

    # Final flush on exit
    try:
        if dirty:
            save_baseline(baseline)
    except Exception:
        logger.exception("Error saving baseline on writer shutdown")


def _scheduler_loop(interval_days: int):
    global _scheduler_stop
    while not _scheduler_stop:
        try:
            rebuild_baseline_from_csv(days_window=None)
        except Exception:
            pass
        # Sleep for interval_days
        for _ in range(int(interval_days * 24 * 60 * 60)):
            if _scheduler_stop:
                break
            time.sleep(1)


def start_scheduler(interval_days: int = 7):
    """Start a background thread to rebuild baseline every `interval_days` days."""
    global _scheduler_thread, _scheduler_stop
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _scheduler_stop = False
    _scheduler_thread = threading.Thread(target=_scheduler_loop, args=(interval_days,), daemon=True)
    _scheduler_thread.start()


def stop_scheduler():
    global _scheduler_stop
    _scheduler_stop = True


def expected_power(ts: Optional[datetime] = None, baseline: Optional[Dict] = None) -> Optional[float]:
    """Return expected power (mean) for the hour of ts using baseline.

    Priority: weekday-hour -> hourly -> overall mean. Returns None if no data.
    """
    if baseline is None:
        baseline = load_baseline()

    if ts is None:
        ts = datetime.now(timezone.utc)

    wd = str(ts.weekday())
    hour = str(ts.hour)

    # weekday-hour preferred
    wh_entry = baseline.get("by_weekday_hour", {}).get(wd, {}).get(hour)
    if wh_entry and wh_entry.get("mean") is not None:
        return float(wh_entry["mean"])

    # legacy hourly
    hour_entry = baseline.get("hourly", {}).get(hour, None)
    if hour_entry and hour_entry.get("mean") is not None:
        return float(hour_entry["mean"])

    # fallback: compute overall mean from weekday_hour if available
    sums = []
    counts = []
    for wd_map in baseline.get("by_weekday_hour", {}).values():
        for v in wd_map.values():
            if v.get("count", 0) > 0:
                sums.append(v["sum"])
                counts.append(v["count"])
    if sums and sum(counts) > 0:
        return float(sum(sums) / sum(counts))

    return None


def update_with_row(row: Dict, baseline_path: str = BASELINE_FILE) -> None:
    """Backward-compatible synchronous update (keeps previous behavior).

    This will immediately acquire the file lock, update baseline and save. Use
    `enqueue_update_with_row` for asynchronous, queued updates processed by the writer thread.
    """
    # Load/modify/save under lock (unchanged behavior)
    lock_path = f"{baseline_path}.lock"
    lock = FileLock(lock_path, timeout=LOCK_TIMEOUT)
    try:
        with lock:
            baseline = load_baseline(baseline_path)

            try:
                ts = row.get("time")
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                elif not isinstance(ts, datetime):
                    ts = datetime.now(timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)

            try:
                power = float(row.get("power_mW"))
            except Exception:
                logger.debug("Skipping baseline update: invalid power value")
                return

            hour = str(ts.hour)
            wd = str(ts.weekday())

            # update legacy hourly
            entry = baseline["hourly"].get(hour)
            if entry is None:
                entry = {"count": 0, "sum": 0.0, "mean": None}
                baseline["hourly"][hour] = entry
            entry["count"] = entry.get("count", 0) + 1
            entry["sum"] = entry.get("sum", 0.0) + power
            entry["mean"] = entry["sum"] / entry["count"]

            # update weekday-hour
            wentry = baseline["by_weekday_hour"][wd].get(hour)
            if wentry is None:
                wentry = {"count": 0, "sum": 0.0, "mean": None}
                baseline["by_weekday_hour"][wd][hour] = wentry
            wentry["count"] = wentry.get("count", 0) + 1
            wentry["sum"] = wentry.get("sum", 0.0) + power
            wentry["mean"] = wentry["sum"] / wentry["count"]

            # use use_lock=False because we're already under lock
            save_baseline(baseline, baseline_path, use_lock=False)
    except Timeout:
        logger.warning("Timeout acquiring lock for update_with_row; skipping update")
    except Exception:
        logger.exception("Error in update_with_row")


def enqueue_update_with_row(row: Dict, baseline_path: str = BASELINE_FILE, block: bool = False, timeout: float = 0.1) -> bool:
    """Enqueue a baseline update for asynchronous processing by the writer thread.

    Returns True if queued successfully; False otherwise (queue full or writer not running).
    If the writer thread is not running it will be started automatically.
    """
    # Ensure writer started
    start_baseline_writer()
    try:
        if block:
            _writer_queue.put(row, timeout=timeout)
        else:
            _writer_queue.put_nowait(row)
        # Ask writer to flush soon so tests and short-lived processes see updates quickly
        try:
            _writer_queue.put_nowait(_FLUSH_TOKEN)
        except queue.Full:
            pass
        return True
    except queue.Full:
        logger.warning("Baseline writer queue full; skipping enqueue")
        return False
    except Exception:
        logger.exception("Failed to enqueue baseline update")
        return False

def is_significant_deviation(current_power: float, ts: Optional[datetime] = None, factor: float = 1.4) -> bool:
    """Return True if current_power exceeds baseline expected value by factor."""
    exp = expected_power(ts)
    if exp is None:
        return False
    return current_power > exp * factor


def _cli():
    import argparse
    parser = argparse.ArgumentParser(description="Baseline manager CLI")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild baseline from CSV now")
    parser.add_argument("--days", type=int, default=None, help="Only use last N days when rebuilding")
    parser.add_argument("--min-count", type=int, default=1, help="Minimum count per (weekday,hour) to include")
    parser.add_argument("--start-scheduler", action="store_true", help="Start background scheduler (rebuild periodically)")
    parser.add_argument("--stop-scheduler", action="store_true", help="Stop background scheduler")
    parser.add_argument("--decay", type=float, default=None, help="Apply decay factor (e.g., 0.9) and save")
    args = parser.parse_args()

    if args.rebuild:
        bl = rebuild_baseline_from_csv(days_window=args.days, min_count=args.min_count)
        if bl is None:
            print("No valid CSV data to build baseline.")
        else:
            print("Baseline rebuilt and saved to", BASELINE_FILE)

    if args.decay is not None:
        apply_decay(decay_factor=args.decay)
        print("Decay applied with factor", args.decay)

    if args.start_scheduler:
        start_scheduler()
        print("Scheduler started in background")

    if args.stop_scheduler:
        stop_scheduler()
        print("Scheduler stop requested")


if __name__ == "__main__":
    _cli()