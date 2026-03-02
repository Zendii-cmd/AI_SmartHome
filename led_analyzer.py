"""led_analyzer.py - Tính điện tiêu thụ của từng LED theo thời gian bật/tắt"""
import pandas as pd
from datetime import datetime, timezone
import os
import logging

logger = logging.getLogger(__name__)

CSV_FILE = "electric_data.csv"
LED1_POWER_MW = float(os.environ.get("LED1_POWER_MW", 500))    # Công suất LED1 (mW) khi ON
LED2_POWER_MW = float(os.environ.get("LED2_POWER_MW", 800))    # Công suất LED2 (mW) khi ON


def compute_led_daily_energy(csv_path: str = CSV_FILE):
    """
    Tính năng lượng hàng ngày cho mỗi LED từ CSV.
    
    Phương pháp:
    - Giữa 2 timestamps liên tiếp, giả sử trạng thái LED không thay đổi (piecewise-constant)
    - Tích phân công suất LED từng khoảng thời gian
    - Gộp dữ liệu theo ngày (UTC)
    
    Returns: dict[date_str] = {
      "led1_wh": float,           # Wh tiêu thụ LED1 trong ngày
      "led2_wh": float,           # Wh tiêu thụ LED2 trong ngày
      "led1_on_minutes": int,     # Phút LED1 ở trạng thái ON
      "led2_on_minutes": int      # Phút LED2 ở trạng thái ON
    }
    """
    if not os.path.exists(csv_path):
        logger.debug("CSV file not found for LED analysis")
        return {}
    
    try:
        df = pd.read_csv(csv_path, parse_dates=["time"], header=0)
        from time_utils import parse_time_series
        df["time"] = parse_time_series(df["time"])
    except ValueError:
        # Thử read headerless
        try:
            df = pd.read_csv(csv_path, header=None, names=["time", "power_mW", "current_mA", "voltage", "led1", "led2"])
            from time_utils import parse_time_series
            df["time"] = parse_time_series(df["time"])
        except Exception:
            logger.exception("Failed to read CSV for LED analysis")
            return {}
    except Exception:
        logger.exception("Failed to read CSV for LED analysis")
        return {}
    
    # Chuyển LED columns thành boolean (xử lý string "True"/"False")
    try:
        if "led1" in df.columns:
            # Xử lý string "True"/"False" và numeric 0/1
            df["led1"] = df["led1"].apply(lambda x: str(x).lower() in ("true", "1") if pd.notna(x) else False)
        else:
            df["led1"] = False
            
        if "led2" in df.columns:
            df["led2"] = df["led2"].apply(lambda x: str(x).lower() in ("true", "1") if pd.notna(x) else False)
        else:
            df["led2"] = False
    except Exception:
        logger.warning("Could not convert LED columns to bool, using defaults")
        df["led1"] = False
        df["led2"] = False
    
    # Sắp xếp theo time và loại bỏ rows không có timestamp
    df = df.sort_values("time").dropna(subset=["time"]).copy()
    
    if len(df) < 2:
        logger.debug("Not enough rows in CSV for LED analysis (need at least 2)")
        return {}
    
    daily_consumption = {}
    
    # Duyệt qua từng khoảng thời gian liên tiếp (từ i đến i+1)
    for i in range(len(df) - 1):
        t0 = df["time"].iloc[i]
        t1 = df["time"].iloc[i + 1]
        
        # Tính khoảng thời gian (seconds và hours)
        duration_seconds = (t1 - t0).total_seconds()
        if duration_seconds <= 0:
            continue  # Bỏ qua nếu khoảng thời gian không hợp lệ
        
        duration_minutes = duration_seconds / 60.0
        duration_hours = duration_seconds / 3600.0
        
        # Ngày (ISO format: "2026-03-02") từ t0
        day_key = t0.date().isoformat()
        
        # Khởi tạo entry cho ngày nếu chưa có
        if day_key not in daily_consumption:
            daily_consumption[day_key] = {
                "led1_wh": 0.0,
                "led2_wh": 0.0,
                "led1_on_minutes": 0.0,
                "led2_on_minutes": 0.0
            }
        
        # LED1: nếu ON trong khoảng [t0, t1), tính energy
        try:
            if pd.notna(df["led1"].iloc[i]) and bool(df["led1"].iloc[i]):
                watts = LED1_POWER_MW / 1000.0  # Convert mW to W
                wh = watts * duration_hours
                daily_consumption[day_key]["led1_wh"] += wh
                daily_consumption[day_key]["led1_on_minutes"] += duration_minutes
        except Exception:
            pass
        
        # LED2: nếu ON trong khoảng [t0, t1), tính energy
        try:
            if pd.notna(df["led2"].iloc[i]) and bool(df["led2"].iloc[i]):
                watts = LED2_POWER_MW / 1000.0  # Convert mW to W
                wh = watts * duration_hours
                daily_consumption[day_key]["led2_wh"] += wh
                daily_consumption[day_key]["led2_on_minutes"] += duration_minutes
        except Exception:
            pass
    
    return daily_consumption


def get_led_daily_summary(date_str: str = None):
    """
    Lấy tóm tắt LED consumption cho 1 ngày cụ thể.
    
    Args:
        date_str: ISO format date string (e.g., "2026-03-02"), mặc định là hôm nay
    
    Returns:
        dict với led1_wh, led2_wh, led1_on_minutes, led2_on_minutes
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).date().isoformat()
    
    data = compute_led_daily_energy()
    
    if date_str in data:
        return data[date_str]
    else:
        return {
            "led1_wh": 0.0,
            "led2_wh": 0.0,
            "led1_on_minutes": 0,
            "led2_on_minutes": 0
        }


def format_led_report(date_str: str = None) -> str:
    """Tạo báo cáo LED dạng text"""
    if date_str is None:
        date_str = datetime.now(timezone.utc).date().isoformat()
    
    led_data = get_led_daily_summary(date_str)
    
    led1_kwh = led_data["led1_wh"] / 1000.0
    led2_kwh = led_data["led2_wh"] / 1000.0
    led1_hours = led_data["led1_on_minutes"] / 60.0
    led2_hours = led_data["led2_on_minutes"] / 60.0
    
    report = f"""
┌───────────────────────────────────────────┐
│    💡 LED CONSUMPTION REPORT - {date_str}   │
├───────────────────────────────────────────┤
│ LED1 (Power: {LED1_POWER_MW}mW):
│   - Energy: {led1_kwh:.3f} kWh ({led_data["led1_wh"]:.1f} Wh)
│   - Duration: {led_data["led1_on_minutes"]:.0f} min ({led1_hours:.1f}h)
│
│ LED2 (Power: {LED2_POWER_MW}mW):
│   - Energy: {led2_kwh:.3f} kWh ({led_data["led2_wh"]:.1f} Wh)
│   - Duration: {led_data["led2_on_minutes"]:.0f} min ({led2_hours:.1f}h)
│
│ TOTAL: {led1_kwh + led2_kwh:.3f} kWh
└───────────────────────────────────────────┘
"""
    return report
