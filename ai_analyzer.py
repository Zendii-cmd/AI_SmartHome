# ai_analyzer.py
import logging
import pandas as pd
from sklearn.ensemble import IsolationForest

logger = logging.getLogger(__name__)

CSV_FILE = "electric_data.csv"
DEFAULT_COLS = ["time", "power_mW", "current_mA", "voltage", "led1", "led2"]

# If more than this fraction of timestamps are invalid we emit a warning.
TIMESTAMP_INVALID_THRESHOLD = 0.01  # 1%

def _read_csv(parse_dates_time: bool = False) -> pd.DataFrame:
    """Read CSV safely. If header is missing, read without header and assign DEFAULT_COLS.

    If parse_dates_time is True, attempt to parse the 'time' column to datetime.
    """
    try:
        if parse_dates_time:
            df = pd.read_csv(CSV_FILE, parse_dates=["time"])
            from time_utils import parse_time_series
            df["time"] = parse_time_series(df["time"], threshold_fraction=TIMESTAMP_INVALID_THRESHOLD)
            # Warn if too many timestamps failed to parse — helps detect dirty data early
            try:
                total = len(df)
                if total > 0:
                    invalid = df["time"].isna().sum()
                    threshold_count = max(1, int(TIMESTAMP_INVALID_THRESHOLD * total))
                    if invalid > threshold_count:
                        logger.warning("Detected %d invalid timestamps out of %d rows (>%.1f%%). Consider normalizing timestamp formats or specifying a parse format.", invalid, total, TIMESTAMP_INVALID_THRESHOLD*100)
            except Exception:
                pass
        else:
            df = pd.read_csv(CSV_FILE)
    except ValueError:
        # Possibly the CSV has no header; read as headerless and assign names
        df = pd.read_csv(CSV_FILE, header=None, names=DEFAULT_COLS)
        if parse_dates_time:
            from time_utils import parse_time_series
            df["time"] = parse_time_series(df["time"], threshold_fraction=TIMESTAMP_INVALID_THRESHOLD)
            try:
                total = len(df)
                if total > 0:
                    invalid = df["time"].isna().sum()
                    threshold_count = max(1, int(TIMESTAMP_INVALID_THRESHOLD * total))
                    if invalid > threshold_count:
                        logger.warning("Detected %d invalid timestamps out of %d rows (>%.1f%%). Consider normalizing timestamp formats or specifying a parse format.", invalid, total, TIMESTAMP_INVALID_THRESHOLD*100)
            except Exception:
                pass
    except pd.errors.EmptyDataError:
        df = pd.DataFrame(columns=DEFAULT_COLS)

    # If expected columns are not present (pandas may have used first row as header),
    # re-read as headerless and assign DEFAULT_COLS
    if not set(["power_mW"]).issubset(df.columns):
        try:
            df = pd.read_csv(CSV_FILE, header=None, names=DEFAULT_COLS)
        except pd.errors.EmptyDataError:
            df = pd.DataFrame(columns=DEFAULT_COLS)
        if parse_dates_time:
            from time_utils import parse_time_series
            df["time"] = parse_time_series(df["time"], threshold_fraction=TIMESTAMP_INVALID_THRESHOLD)
            try:
                total = len(df)
                if total > 0:
                    invalid = df["time"].isna().sum()
                    threshold_count = max(1, int(TIMESTAMP_INVALID_THRESHOLD * total))
                    if invalid > threshold_count:
                        logger.warning("Detected %d invalid timestamps out of %d rows (>%.1f%%). Consider normalizing timestamp formats or specifying a parse format.", invalid, total, TIMESTAMP_INVALID_THRESHOLD*100)
            except Exception:
                pass

    return df


def analyze_realtime():
    df = _read_csv()

    if len(df) < 10:
        return "📊 AI: Chưa đủ dữ liệu để phân tích"

    df["power_mW"] = pd.to_numeric(df["power_mW"], errors="coerce")
    avg_power = df["power_mW"].mean()
    current_power = df["power_mW"].iloc[-1]

    if pd.isna(current_power) or pd.isna(avg_power):
        return "📊 AI: Dữ liệu không hợp lệ để phân tích"

    # Baseline-aware check (prefer baseline when available)
    try:
        from baseline_manager import expected_power, is_significant_deviation
        import datetime as _dt

        now = _dt.datetime.now(_dt.timezone.utc)
        exp = expected_power(now)
        if exp is not None:
            # If current is significantly above baseline, warn with percent
            if is_significant_deviation(float(current_power), now, factor=1.4):
                pct = (float(current_power) / exp - 1.0) * 100
                return f"⚠️ AI Cảnh báo: Công suất hiện tại cao hơn baseline khoảng {pct:.0f}%"
    except Exception:
        # If baseline not available or error, fall back to avg comparison
        pass

    if current_power > avg_power * 1.4:
        return "⚠️ AI Cảnh báo: Công suất hiện tại cao bất thường"

    return "✅ AI: Mức tiêu thụ điện ổn định"


def detect_anomaly(persistent_count: int = 2, contamination: float = 0.05):
    """Detect anomalies using IsolationForest with LED-aware preprocessing.

    - Adds `led1`/`led2` as features so model learns power patterns conditioned on LED state.
    - Suppresses anomaly if the last row has an LED state change compared to previous row.
    - Requires the last `persistent_count` rows to all be anomalous before raising alert (reduces flakiness).
    """
    df = _read_csv()

    if len(df) < 30:
        return None

    df["power_mW"] = pd.to_numeric(df["power_mW"], errors="coerce")

    # Ensure led columns exist and are numeric (0/1)
    for col in ["led1", "led2"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map({"true": 1, "false": 0, "1": 1, "0": 0}).fillna(0).astype(int)
        else:
            df[col] = 0

    df = df.dropna(subset=["power_mW"]).copy()

    if len(df) < 30:
        return None

    # If last row is a state change (led toggled), suppress anomaly detection for this row
    if len(df) >= 2:
        last = df.iloc[-1]
        prev = df.iloc[-2]
        if int(last["led1"]) != int(prev["led1"]) or int(last["led2"]) != int(prev["led2"]):
            # Logically this is expected behavior when a device state changed
            return None

    # Build feature matrix including LED states
    X = df[["power_mW", "led1", "led2"]]

    model = IsolationForest(contamination=contamination, random_state=42)
    df["anomaly"] = model.fit_predict(X)

    # Require persistence: last persistent_count rows must be anomalous
    if df["anomaly"].iloc[-1] == -1:
        if len(df) >= persistent_count:
            last_n = df["anomaly"].iloc[-persistent_count:]
            if (last_n == -1).all():
                return "🚨 AI ML: Phát hiện hành vi tiêu thụ điện bất thường"
            else:
                return None
        else:
            return None

    return None


def daily_summary():
    df = _read_csv(parse_dates_time=True)

    if df.empty or "time" not in df.columns:
        return "📅 Không có dữ liệu để tổng hợp"

    df["date"] = df["time"].dt.date

    # Ensure numeric power column (coerce non-numeric -> NaN) and drop invalid rows
    df["power_mW"] = pd.to_numeric(df["power_mW"], errors="coerce")
    summary = df.dropna(subset=["power_mW"]).groupby("date")["power_mW"].mean().tail(1)
    if summary.empty:
        return "📅 Không có dữ liệu để tổng hợp"

    return f"📅 Trung bình công suất hôm nay: {summary.values[0]:.0f} mW"
