# AI Smart Home — Hướng dẫn sử dụng


## Cài đặt môi trường
- Tạo venv & cài dependencies (đã dùng `.venv` trong project):
  & "E:/class/Nam 4/Tot_nghiep_2026/Detai_Moi/AI_Py_SmartHome/.venv/Scripts/python.exe" -m pip install -r requirements.txt

## Chạy local (test)
- Chạy AI chính:
  & "path/to/python" "ai_mqtt.py"

- Tuỳ chọn khởi động scheduler tự động (khuyến nghị **OFF** trên máy dev):
  - Bật bằng biến môi trường: `SET ENABLE_BASELINE_SCHED=1` (Windows PowerShell: `$env:ENABLE_BASELINE_SCHED='1'`)
  - Hoặc khi chạy: `python ai_mqtt.py --enable-baseline-scheduler`

- Rebuild baseline thủ công (một lần):
  `python ai_mqtt.py --rebuild-baseline --rebuild-days 30`  # dùng 30 ngày gần nhất

## CLI cho baseline
- Sử dụng module `baseline_manager.py` trực tiếp:
  - Rebuild: `python -m baseline_manager --rebuild --days 30`
  - Áp decay: `python -m baseline_manager --decay 0.9`
  - Bật scheduler: `python -m baseline_manager --start-scheduler`
  - Dừng scheduler: `python -m baseline_manager --stop-scheduler`

## Ghi dữ liệu & timezone
- Tất cả timestamp đều được lưu ở UTC (ISO 8601, có +00:00). Điều này giúp tránh nhầm lẫn khi deploy server ở múi giờ khác.

## Dự đoán tiền điện (billing)
- `billing_predictor.py` cung cấp:
  - `compute_monthly_energy_kwh(year, month)` — tính kWh từ `electric_data.csv` bằng cách tích phân công suất theo thời gian.
  - `project_monthly_energy_and_cost(as_of=None, price_vnd_per_kwh=None)` — dự báo kWh & chi phí cho tháng hiện tại, trả về khoảng tin cậy đơn giản.
- Cấu hình đơn giản: đặt biến môi trường `ELECTRICITY_PRICE` để thay đổi đơn giá (VND/kWh). Mặc định: 3000 VND/kWh.
- Bảo lưu: module sử dụng phương pháp piecewise-constant giữa các mẫu; càng có nhiều mẫu trong tháng thì dự báo càng chính xác.
- Lệnh CLI ví dụ:
  - `python -m billing_predictor --month 2026-02`  # tính kWh cho tháng
  - `python -m billing_predictor`  # dự báo cho tháng hiện tại

## Logs & Daily Reports
- Logs are written to `logs/ai_smart_home.log` (rotating file handler).
- Daily billing reports (if enabled) are emitted to the log and saved to `logs/reports/report-YYYY-MM-DD.txt`.
- Enable daily reports:
  - Env var: `ENABLE_DAILY_REPORT=1` and optionally `REPORT_HOUR=0` (UTC hour), or
  - CLI: `python ai_mqtt.py --enable-daily-report --report-hour 0`

## Khi deploy lên server
- **Không** dùng scheduler in-process lâu dài; khuyến nghị dùng cron / systemd timer / Windows Task Scheduler để chạy:
  - `python -m baseline_manager --rebuild --days 7`
  - (hoặc) chạy `python -m baseline_manager --decay 0.95` định kỳ

- Thiết lập logging, rotate logs, và backup `baseline.json` định kỳ.
- Đảm bảo `filelock` (đã thêm) được cài khi multi-process truy cập `baseline.json`.

## Tests
- Chạy test: `python -m pytest -q` (project đã có tests cơ bản cho baseline và locks)

## Lưu ý khác
- Hiện các datetime đều là timezone-aware UTC. Nếu bạn muốn chuyển thành múi giờ địa phương khi hiển thị, hãy convert trước khi show.
- Scheduler mặc định tắt khi phát triển trên máy cá nhân để tiết kiệm tài nguyên; bật khi deploy server.

---