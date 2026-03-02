"""test_led_analyzer.py - Unit tests cho LED analyzer module"""
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from led_analyzer import compute_led_daily_energy, get_led_daily_summary, format_led_report

CSV = "electric_data.csv"


def write_test_csv(rows):
    """Ghi rows vào CSV file để test"""
    df = pd.DataFrame(rows)
    df.to_csv(CSV, index=False)


def test_led_energy_calculation_simple():
    """Test tính energy LED - trường hợp đơn giản"""
    # Tạo 24 rows, mỗi row cách nhau 1 giờ
    # LED1=True, LED2=False trong 10 giờ đầu, sau đó ngược lại
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = []
    
    for i in range(25):
        rows.append({
            "time": (now + timedelta(hours=i)).isoformat(),
            "power_mW": 1000,
            "current_mA": 250,
            "voltage": 3.6,
            "led1": i < 10,  # LED1 ON cho 10 giờ đầu
            "led2": i >= 10  # LED2 ON cho 14 giờ sau
        })
    
    write_test_csv(rows)
    
    # Tính energy
    result = compute_led_daily_energy(CSV)
    today = now.date().isoformat()
    
    print(f"\n✓ Test case: Simple LED energy calculation")
    print(f"  Date: {today}")
    print(f"  Result: {result.get(today, {})}")
    
    # LED1 ON 10 hours, power 500mW (default)
    # Energy = 0.5W * 10h = 5 Wh
    assert today in result, f"Expected date {today} in result"
    assert result[today]["led1_on_minutes"] == 600, f"LED1 should be on for 600 minutes, got {result[today]['led1_on_minutes']}"
    
    # LED2 ON 14 hours, power 800mW (default)
    # Energy = 0.8W * 14h = 11.2 Wh (approximately)
    assert result[today]["led2_on_minutes"] == 840, f"LED2 should be on for 840 minutes, got {result[today]['led2_on_minutes']}"
    
    print(f"  ✓ LED1: {result[today]['led1_wh']:.2f} Wh, {result[today]['led1_on_minutes']:.0f} min")
    print(f"  ✓ LED2: {result[today]['led2_wh']:.2f} Wh, {result[today]['led2_on_minutes']:.0f} min")


def test_get_led_daily_summary():
    """Test lấy summary LED cho 1 ngày"""
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = []
    
    for i in range(25):
        rows.append({
            "time": (now + timedelta(hours=i)).isoformat(),
            "power_mW": 1000,
            "current_mA": 250,
            "voltage": 3.6,
            "led1": True,
            "led2": False
        })
    
    write_test_csv(rows)
    
    today = now.date().isoformat()
    summary = get_led_daily_summary(today)
    
    print(f"\n✓ Test case: Get LED daily summary for {today}")
    print(f"  Summary: {summary}")
    
    assert summary["led1_wh"] > 0, "LED1 should have energy consumption"
    assert summary["led2_wh"] == 0, "LED2 should not have energy consumption"
    
    print(f"  ✓ Valid summary structure")


def test_format_led_report():
    """Test báo cáo LED"""
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = []
    
    for i in range(25):
        rows.append({
            "time": (now + timedelta(hours=i)).isoformat(),
            "power_mW": 1000,
            "current_mA": 250,
            "voltage": 3.6,
            "led1": i % 2 == 0,
            "led2": i % 2 == 1
        })
    
    write_test_csv(rows)
    
    today = now.date().isoformat()
    report = format_led_report(today)
    
    print(f"\n✓ Test case: LED report format")
    print(report)
    
    assert "LED1" in report, "Report should contain LED1"
    assert "LED2" in report, "Report should contain LED2"
    assert "kWh" in report or "Wh" in report, "Report should contain energy unit"
    
    print(f"  ✓ Report formatted correctly")


def test_multiple_days():
    """Test khi dữ liệu span nhiều ngày"""
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = []
    
    # Tạo dữ liệu cho 3 ngày
    for day in range(3):
        for hour in range(24):
            rows.append({
                "time": (now + timedelta(days=day, hours=hour)).isoformat(),
                "power_mW": 1000,
                "current_mA": 250,
                "voltage": 3.6,
                "led1": hour < 12,
                "led2": hour >= 12
            })
    
    write_test_csv(rows)
    
    result = compute_led_daily_energy(CSV)
    
    print(f"\n✓ Test case: Multiple days LED consumption")
    print(f"  Total days in result: {len(result)}")
    
    for day_key in sorted(result.keys()):
        print(f"  {day_key}: LED1={result[day_key]['led1_wh']:.2f}Wh, LED2={result[day_key]['led2_wh']:.2f}Wh")
    
    assert len(result) >= 3, "Should have at least 3 days of data"
    print(f"  ✓ Multiple days tracked correctly")


# Cleanup
def cleanup():
    if os.path.exists(CSV):
        os.remove(CSV)
        print(f"\n🧹 Cleaned up test CSV file")


if __name__ == "__main__":
    print("=" * 50)
    print("🧪 LED ANALYZER TESTS")
    print("=" * 50)
    
    try:
        test_led_energy_calculation_simple()
        test_get_led_daily_summary()
        test_format_led_report()
        test_multiple_days()
        
        print("\n" + "=" * 50)
        print("✅ ALL TESTS PASSED")
        print("=" * 50)
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
    finally:
        cleanup()
