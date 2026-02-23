# data_logger.py
import csv
from datetime import datetime, timezone
import os
from filelock import FileLock, Timeout
import logging

logger = logging.getLogger(__name__)
LOCK_TIMEOUT_CSV = 5  # seconds

CSV_FILE = "electric_data.csv"
DEFAULT_COLS = ["time", "power_mW", "current_mA", "voltage", "led1", "led2"]

def init_csv():
    """Ensure CSV exists and has header. If file exists but header is missing, prepend header."""
    # Create file with header if it doesn't exist
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(DEFAULT_COLS)
        return

    # File exists: check first line for header presence
    try:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            first = f.readline().strip()
    except Exception:
        first = ""

    if not first:
        # Empty file -> write header
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(DEFAULT_COLS)
        return

    lower = first.lower()
    # If expected column names not present in the first line, assume header missing
    if not ("time" in lower or "power" in lower or "power_mw" in lower):
        try:
            with open(CSV_FILE, "r", encoding="utf-8") as f:
                rest = f.read()
        except Exception:
            rest = ""
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(DEFAULT_COLS)
            # preserve existing data
            if rest:
                f.write(rest)


def save_data(data):
    # Ensure header exists before appending
    init_csv()

    lock_path = CSV_FILE + ".lock"
    try:
        with FileLock(lock_path, timeout=LOCK_TIMEOUT_CSV):
            mode = 'a' if os.path.exists(CSV_FILE) else 'w'
            with open(CSV_FILE, mode, newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.now(timezone.utc).isoformat(),
                    data.get("power_mW"),
                    data.get("current_mA"),
                    data.get("voltage"),
                    data.get("led1"),
                    data.get("led2")
                ])
                # Ensure the data is flushed to disk (reduce risk of loss on sudden shutdown)
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except Exception:
                    # On some filesystems os.fsync may fail; we log and continue
                    pass
    except Timeout:
        logger.error("Timeout acquiring CSV file lock - failed to write row")
