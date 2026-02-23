import pandas as pd
from time_utils import parse_time_series
import ai_analyzer


def test_parse_various_timestamp_formats():
    s = pd.Series([
        '2026-02-03T07:04:00+00:00',
        '2026-02-03T07:39:11.266917+00:00',
        '2026-02-03T07:04:00Z',
        '2026-02-03 07:04:00+00:00',
        '1675430000',  # epoch seconds
    ])
    parsed = parse_time_series(s)
    assert parsed.isna().sum() == 0


def test_csv_parses_below_threshold():
    # Ensure the repository sample CSV doesn't trigger the invalid-timestamp warning
    s = pd.read_csv(ai_analyzer.CSV_FILE, parse_dates=['time'])['time'].astype(str)
    parsed = parse_time_series(s, threshold_fraction=ai_analyzer.TIMESTAMP_INVALID_THRESHOLD)
    invalid = int(parsed.isna().sum())
    threshold_count = max(1, int(ai_analyzer.TIMESTAMP_INVALID_THRESHOLD * len(parsed)))
    assert invalid <= threshold_count
