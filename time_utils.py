import warnings
import logging
from typing import List
import pandas as pd

logger = logging.getLogger(__name__)

# Common formats to try (ISO-like and common variants)
COMMON_TIME_FORMATS: List[str] = [
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
]


def parse_time_series(series: pd.Series, formats: List[str] = None, threshold_fraction: float = 0.01) -> pd.Series:
    """Parse a pandas Series of timestamps.

    Strategy:
      1. Try a small list of explicit formats and pick the one with the fewest invalid parses.
      2. If the best explicit format yields <= threshold_fraction invalid values, use it.
      3. Otherwise, fall back to pandas' generic parse but suppress the specific "Could not infer format" UserWarning.

    Returns a timezone-aware (UTC) datetime Series with errors coerced to NaT.
    """
    if formats is None:
        formats = COMMON_TIME_FORMATS

    s = series.astype(str).copy()
    # Basic normalization: strip whitespace, normalize timezone offsets and Zulu 'Z',
    # and convert comma decimal separators to dot to help parsing.
    s = s.str.strip()
    s = s.str.replace(r'([+-]\d{2}):(\d{2})$', r'\1\2', regex=True)
    s = s.str.replace(r'Z$', '+0000', regex=True)
    s = s.str.replace(',', '.', regex=False)

    total = len(s)
    if total == 0:
        return pd.to_datetime(s, utc=True, errors="coerce")

    best_parsed = None
    best_invalid = total + 1

    # Try explicit formats and pick the one with the fewest invalid parses
    for fmt in formats:
        try:
            parsed = pd.to_datetime(s, format=fmt, utc=True, errors="coerce")
            parsed = pd.Series(parsed, index=s.index)
            invalid = int(parsed.isna().sum())
            if invalid < best_invalid:
                best_invalid = invalid
                best_parsed = parsed
        except Exception:
            # ignore formats that raise
            continue

    threshold_count = max(1, int(threshold_fraction * total))
    if best_parsed is not None and best_invalid <= threshold_count:
        logger.debug("Parsed times using explicit format with %d invalid out of %d", best_invalid, total)
        return best_parsed

    # fallback: use pandas' parser but suppress the specific user warning
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Could not infer format, so each element will be parsed individually, falling back to `dateutil`.", category=UserWarning)
        parsed = pd.to_datetime(s, utc=True, errors="coerce")
    parsed = pd.Series(parsed, index=s.index)

    # Handle epoch-like numeric timestamps (e.g., '1675430000') by parsing as seconds
    try:
        mask_epoch = s.str.match(r"^\d{9,}$")
        if mask_epoch.any():
            nums = pd.to_numeric(s[mask_epoch], errors="coerce")
            if not nums.isna().all():
                epoch_parsed = pd.to_datetime(nums.astype('int64'), unit='s', utc=True)
                parsed.loc[mask_epoch] = pd.Series(epoch_parsed, index=parsed.loc[mask_epoch].index)
    except Exception:
        # If epoch parsing fails, ignore and continue to element-wise fallback
        pass

    invalid = int(parsed.isna().sum())
    if invalid <= threshold_count:
        return parsed

    # Element-wise fallback using dateutil for mixed/odd formats (only on remaining invalids)
    try:
        from dateutil import parser as _parser

        def _parse_one(x: str):
            try:
                dt = _parser.parse(x)
                import datetime as _dt
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_dt.timezone.utc)
                else:
                    dt = dt.astimezone(_dt.timezone.utc)
                return pd.Timestamp(dt)
            except Exception:
                return pd.NaT

        invalid_mask = parsed.isna()
        if invalid_mask.any():
            parsed_vals = s[invalid_mask].map(_parse_one)
            parsed.loc[invalid_mask] = parsed_vals
            parsed = pd.to_datetime(parsed, utc=True, errors='coerce')
    except Exception:
        # dateutil may not be available or parsing may fail; fall back to current parsed
        pass

    # If still many invalids, log a small sample for diagnostics
    invalid = int(parsed.isna().sum())
    if invalid > threshold_count:
        try:
            samples = s[parsed.isna()].unique()[:10].tolist()
            logger.debug("parse_time_series: %d invalid timestamps (sample: %s)", invalid, samples)
        except Exception:
            pass

    return parsed
