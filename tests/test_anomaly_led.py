import pandas as pd
from datetime import datetime, timedelta, timezone
from ai_analyzer import detect_anomaly

CSV = "electric_data.csv"


def write_csv(rows):
    df = pd.DataFrame(rows)
    df.to_csv(CSV, index=False)


def test_led_toggle_suppresses_anomaly(tmp_path, monkeypatch):
    # Create a history of stable data (>=30 rows), led1=False, power around 1000
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    rows = []
    for i in range(35):
        rows.append({
            "time": (now - timedelta(minutes=(35 - i))).isoformat(),
            "power_mW": 1000 + (i % 5),
            "current_mA": 250,
            "voltage": 3.6,
            "led1": False,
            "led2": False,
        })

    # Now toggle led1 True and drop power abruptly in last row (expected behavior)
    rows.append({
        "time": now.isoformat(),
        "power_mW": 200,  # drop due to LED change
        "current_mA": 50,
        "voltage": 3.6,
        "led1": True,
        "led2": False,
    })

    write_csv(rows)

    # Should not report anomaly because led toggled
    assert detect_anomaly() is None


def test_persistent_anomaly_without_led_change(tmp_path):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    rows = []
    for i in range(35):
        rows.append({
            "time": (now - timedelta(minutes=(35 - i))).isoformat(),
            "power_mW": 1000 + (i % 5),
            "current_mA": 250,
            "voltage": 3.6,
            "led1": False,
            "led2": False,
        })

    # Last two rows have sudden large increase and led states unchanged => persistent anomaly
    rows.append({
        "time": (now + timedelta(minutes=1)).isoformat(),
        "power_mW": 5000,
        "current_mA": 1300,
        "voltage": 3.6,
        "led1": False,
        "led2": False,
    })
    rows.append({
        "time": (now + timedelta(minutes=2)).isoformat(),
        "power_mW": 5200,
        "current_mA": 1400,
        "voltage": 3.6,
        "led1": False,
        "led2": False,
    })

    write_csv(rows)

    assert detect_anomaly() is not None
