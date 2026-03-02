"""led_consumption_manager.py - Quản lý lưu trữ và cập nhật LED consumption data"""
import json
import logging
import os
from datetime import datetime, timezone
from filelock import FileLock, Timeout

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

LED_CONSUMPTION_FILE = "led_consumption.json"
LOCK_TIMEOUT = 5  # seconds


def _default_led_structure() -> dict:
    """Tạo cấu trúc mặc định cho LED consumption data"""
    return {
        "daily": {},
        "last_updated": None
    }


def load_led_consumption(path: str = LED_CONSUMPTION_FILE) -> dict:
    """
    Load LED consumption data từ JSON file.
    
    Returns: dict với structure {
        "daily": {
            "2026-03-02": {
                "led1_wh": float,
                "led2_wh": float,
                "led1_on_minutes": int,
                "led2_on_minutes": int
            },
            ...
        },
        "last_updated": datetime_string
    }
    """
    lock_path = f"{path}.lock"
    
    if not os.path.exists(path):
        logger.debug("LED consumption file not found, returning default structure")
        return _default_led_structure()
    
    try:
        lock = FileLock(lock_path, timeout=LOCK_TIMEOUT)
        with lock:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        logger.debug("LED consumption data loaded")
        return data
    except Timeout:
        logger.warning("Timeout acquiring lock for loading LED consumption; returning default structure")
        return _default_led_structure()
    except Exception:
        logger.exception("Error reading LED consumption file; returning default structure")
        return _default_led_structure()


def save_led_consumption(data: dict, path: str = LED_CONSUMPTION_FILE) -> None:
    """
    Lưu LED consumption data vào JSON file với file lock.
    
    Args:
        data: dict built từ load_led_consumption() hoặc tương tự
        path: đường dẫn tới file JSON
    """
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    lock_path = f"{path}.lock"
    
    try:
        lock = FileLock(lock_path, timeout=LOCK_TIMEOUT)
        with lock:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    logger.debug("fsync failed when saving LED consumption")
        logger.info("LED consumption data saved to %s", path)
    except Timeout:
        logger.warning("Timeout acquiring lock for saving LED consumption; skipping save")
    except Exception:
        logger.exception("Error saving LED consumption to %s", path)


def update_led_daily(date_key: str, led1_wh: float, led2_wh: float,
                     led1_minutes: int, led2_minutes: int,
                     path: str = LED_CONSUMPTION_FILE) -> bool:
    """
    Cập nhật LED consumption cho 1 ngày cụ thể.
    
    Args:
        date_key: ISO date string (e.g., "2026-03-02")
        led1_wh: Năng lượng LED1 (Wh)
        led2_wh: Năng lượng LED2 (Wh)
        led1_minutes: Phút LED1 ON
        led2_minutes: Phút LED2 ON
        path: đường dẫn tới file JSON
    
    Returns:
        True nếu update thành công, False nếu thất bại
    """
    try:
        data = load_led_consumption(path)
        
        if "daily" not in data:
            data["daily"] = {}
        
        data["daily"][date_key] = {
            "led1_wh": float(led1_wh),
            "led2_wh": float(led2_wh),
            "led1_on_minutes": int(led1_minutes),
            "led2_on_minutes": int(led2_minutes)
        }
        
        save_led_consumption(data, path)
        logger.debug("LED daily data updated for %s", date_key)
        return True
    except Exception:
        logger.exception("Failed to update LED daily consumption")
        return False


def get_led_consumption_for_date(date_key: str, path: str = LED_CONSUMPTION_FILE) -> dict:
    """
    Lấy LED consumption data cho 1 ngày cụ thể.
    
    Args:
        date_key: ISO date string (e.g., "2026-03-02")
        path: đường dẫn tới file JSON
    
    Returns:
        dict với led1_wh, led2_wh, led1_on_minutes, led2_on_minutes
    """
    data = load_led_consumption(path)
    
    if date_key in data.get("daily", {}):
        return data["daily"][date_key]
    else:
        return {
            "led1_wh": 0.0,
            "led2_wh": 0.0,
            "led1_on_minutes": 0,
            "led2_on_minutes": 0
        }


def get_all_led_consumption(path: str = LED_CONSUMPTION_FILE) -> dict:
    """Lấy toàn bộ LED consumption data"""
    return load_led_consumption(path)


def clear_led_consumption(path: str = LED_CONSUMPTION_FILE) -> None:
    """Xóa toàn bộ LED consumption data"""
    save_led_consumption(_default_led_structure(), path)
    logger.info("LED consumption data cleared")
