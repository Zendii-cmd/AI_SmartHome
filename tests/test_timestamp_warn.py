import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import ai_analyzer


def test_invalid_timestamp_warning(tmp_path, caplog):
    csvfile = tmp_path / "electric_data.csv"
    ai_analyzer.CSV_FILE = str(csvfile)

    now = datetime.now(timezone.utc)
    rows = ["time,power_mW,current_mA,voltage,led1,led2\n"]
    # 45 valid, 5 invalid -> 5/50 = 10% > 1% threshold
    for i in range(45):
        rows.append(f"{(now + timedelta(seconds=i)).isoformat()},{1000+i},,,0,0\n")
    for i in range(5):
        rows.append(f"badtime,{1000+i},,,0,0\n")

    csvfile.write_text("".join(rows))

    caplog.set_level(logging.WARNING)
    with caplog.at_level(logging.WARNING):
        _ = ai_analyzer._read_csv(parse_dates_time=True)

    assert any("invalid timestamps" in rec.message for rec in caplog.records)
