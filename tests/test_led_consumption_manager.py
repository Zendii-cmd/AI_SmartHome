"""test_led_consumption_manager.py - Unit tests cho LED consumption manager"""
import os
import json
from datetime import datetime, timezone
from led_consumption_manager import (
    load_led_consumption,
    save_led_consumption,
    update_led_daily,
    get_led_consumption_for_date,
    clear_led_consumption
)

TEST_FILE = "test_led_consumption.json"


def cleanup():
    """Xóa test files"""
    for f in [TEST_FILE, f"{TEST_FILE}.lock"]:
        if os.path.exists(f):
            os.remove(f)


def test_save_and_load():
    """Test save và load LED consumption data"""
    print("\n✓ Test: Save and Load LED consumption")
    
    cleanup()
    
    # Tạo data
    data = {
        "daily": {
            "2026-03-02": {
                "led1_wh": 2500.5,
                "led2_wh": 3800.0,
                "led1_on_minutes": 300,
                "led2_on_minutes": 475
            }
        },
        "last_updated": datetime.now(timezone.utc).isoformat()
    }
    
    # Save
    save_led_consumption(data, TEST_FILE)
    assert os.path.exists(TEST_FILE), "File should be created"
    print(f"  ✓ File saved: {TEST_FILE}")
    
    # Load
    loaded = load_led_consumption(TEST_FILE)
    assert "daily" in loaded, "Loaded data should have 'daily' key"
    assert "2026-03-02" in loaded["daily"], "Should have the date"
    
    led_data = loaded["daily"]["2026-03-02"]
    assert led_data["led1_wh"] == 2500.5, f"LED1 energy mismatch: {led_data['led1_wh']}"
    assert led_data["led2_wh"] == 3800.0, f"LED2 energy mismatch: {led_data['led2_wh']}"
    
    print(f"  ✓ Data loaded correctly")
    print(f"    LED1: {led_data['led1_wh']}Wh, {led_data['led1_on_minutes']}min")
    print(f"    LED2: {led_data['led2_wh']}Wh, {led_data['led2_on_minutes']}min")


def test_update_led_daily():
    """Test update LED daily data"""
    print("\n✓ Test: Update LED daily consumption")
    
    cleanup()
    
    # Update day 1
    success = update_led_daily("2026-03-01", 1500.0, 2000.0, 180, 240, TEST_FILE)
    assert success, "Update should succeed"
    print(f"  ✓ Updated 2026-03-01")
    
    # Update day 2
    success = update_led_daily("2026-03-02", 2500.0, 3800.0, 300, 475, TEST_FILE)
    assert success, "Update should succeed"
    print(f"  ✓ Updated 2026-03-02")
    
    # Load and verify
    loaded = load_led_consumption(TEST_FILE)
    assert len(loaded["daily"]) == 2, "Should have 2 days"
    
    for date_key in ["2026-03-01", "2026-03-02"]:
        assert date_key in loaded["daily"], f"Date {date_key} should exist"
    
    print(f"  ✓ All updates saved correctly")


def test_get_led_consumption_for_date():
    """Test lấy LED consumption cho 1 ngày"""
    print("\n✓ Test: Get LED consumption for specific date")
    
    cleanup()
    
    # Update data
    update_led_daily("2026-03-05", 5000.0, 6000.0, 600, 720, TEST_FILE)
    
    # Retrieve
    data = get_led_consumption_for_date("2026-03-05", TEST_FILE)
    assert data["led1_wh"] == 5000.0, f"LED1 mismatch: {data['led1_wh']}"
    assert data["led2_wh"] == 6000.0, f"LED2 mismatch: {data['led2_wh']}"
    assert data["led1_on_minutes"] == 600, f"LED1 minutes mismatch"
    
    print(f"  ✓ Retrieved data for 2026-03-05:")
    print(f"    LED1: {data['led1_wh']}Wh, {data['led1_on_minutes']}min")
    print(f"    LED2: {data['led2_wh']}Wh, {data['led2_on_minutes']}min")


def test_get_nonexistent_date():
    """Test lấy data cho ngày không tồn tại"""
    print("\n✓ Test: Get data for non-existent date")
    
    cleanup()
    
    update_led_daily("2026-03-01", 1000.0, 1500.0, 120, 180, TEST_FILE)
    
    # Lấy ngày không tồn tại
    data = get_led_consumption_for_date("2026-03-99", TEST_FILE)
    assert data["led1_wh"] == 0.0, "Should return default (0) for non-existent date"
    assert data["led2_wh"] == 0.0, "Should return default (0) for non-existent date"
    
    print(f"  ✓ Returns zeros for non-existent date")


def test_clear_led_consumption():
    """Test xóa toàn bộ LED consumption data"""
    print("\n✓ Test: Clear LED consumption data")
    
    cleanup()
    
    # Add data
    update_led_daily("2026-03-01", 1000.0, 1500.0, 120, 180, TEST_FILE)
    update_led_daily("2026-03-02", 2000.0, 3000.0, 240, 360, TEST_FILE)
    
    loaded = load_led_consumption(TEST_FILE)
    assert len(loaded["daily"]) > 0, "Should have data before clear"
    print(f"  ✓ Data present before clear: {len(loaded['daily'])} days")
    
    # Clear
    clear_led_consumption(TEST_FILE)
    
    # Verify
    loaded = load_led_consumption(TEST_FILE)
    assert len(loaded["daily"]) == 0, "Data should be empty after clear"
    print(f"  ✓ Data cleared successfully")


def test_multiple_updates_same_date():
    """Test update multiple times cho cùng 1 ngày (phải overwrite)"""
    print("\n✓ Test: Multiple updates for same date")
    
    cleanup()
    
    # Update 1
    update_led_daily("2026-03-10", 1000.0, 2000.0, 120, 240, TEST_FILE)
    data1 = get_led_consumption_for_date("2026-03-10", TEST_FILE)
    print(f"  ✓ First update: LED1={data1['led1_wh']}Wh")
    
    # Update 2 (overwrite)
    update_led_daily("2026-03-10", 5000.0, 6000.0, 600, 720, TEST_FILE)
    data2 = get_led_consumption_for_date("2026-03-10", TEST_FILE)
    print(f"  ✓ Second update: LED1={data2['led1_wh']}Wh")
    
    assert data2["led1_wh"] == 5000.0, "Should overwrite with new values"
    assert data2["led1_on_minutes"] == 600, "Should overwrite minutes"
    
    print(f"  ✓ Overwrite working correctly")


if __name__ == "__main__":
    print("=" * 50)
    print("🧪 LED CONSUMPTION MANAGER TESTS")
    print("=" * 50)
    
    try:
        test_save_and_load()
        test_update_led_daily()
        test_get_led_consumption_for_date()
        test_get_nonexistent_date()
        test_clear_led_consumption()
        test_multiple_updates_same_date()
        
        print("\n" + "=" * 50)
        print("✅ ALL TESTS PASSED")
        print("=" * 50)
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup()
