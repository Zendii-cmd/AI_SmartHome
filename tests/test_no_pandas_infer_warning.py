import warnings
from datetime import datetime, timezone, timedelta
from pathlib import Path
import ai_analyzer
import billing_predictor


def write_mixed_csv(path):
    now = datetime.now(timezone.utc)
    rows = ["time,power_mW,current_mA,voltage,led1,led2\n"]
    # Some ISO with timezone, some without, some fractional seconds
    rows.append(f"{now.strftime('%Y-%m-%dT%H:%M:%S%z')},{1000},,,0,0\n")
    rows.append(f"{now.isoformat()},{1100},,,0,0\n")
    rows.append(f"{(now + timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')},{1200},,,0,0\n")
    rows.append(f"{(now + timedelta(seconds=2)).strftime('%Y-%m-%dT%H:%M:%S.%f%z')},{1300},,,0,0\n")
    path.write_text(''.join(rows))


def test_no_pandas_infer_warning(tmp_path):
    csvfile = tmp_path / "electric_data.csv"
    write_mixed_csv(csvfile)

    ai_analyzer.CSV_FILE = str(csvfile)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = ai_analyzer._read_csv(parse_dates_time=True)
        assert not any("Could not infer format" in str(w.message) for w in caught)

    # also test billing parser
    billing_predictor.CSV_FILE = str(csvfile)
    with warnings.catch_warnings(record=True) as caught2:
        warnings.simplefilter("always")
        _ = billing_predictor._read_power_df(str(csvfile))
        assert not any("Could not infer format" in str(w.message) for w in caught2)
