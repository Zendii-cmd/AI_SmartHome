# ai_mqtt.py
import json
try:
    import paho.mqtt.client as mqtt
except ModuleNotFoundError as e:
    raise ModuleNotFoundError("Missing required package 'paho-mqtt'. Install with:\n    pip install -r requirements.txt\nor\n    pip install paho-mqtt") from e

from data_logger import init_csv, save_data
from ai_analyzer import analyze_realtime, detect_anomaly, daily_summary
from ai_advisor import energy_advice
from baseline_manager import update_with_row, enqueue_update_with_row, start_scheduler, stop_scheduler, load_baseline, save_baseline, start_baseline_writer, stop_baseline_writer
from datetime import datetime, timezone, timedelta
import signal
import sys
import threading
import os
import logging
from dotenv import load_dotenv

# Load .env into environment (local dev convenience)
load_dotenv()

# Setup basic logging early so we can use logger in startup checks
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# Ensure stdout/stderr use UTF-8 when possible to avoid UnicodeEncodeError on some consoles
try:
    import sys as _sys
    try:
        _sys.stdout.reconfigure(encoding='utf-8')
        _sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        # reconfigure may not be supported in some environments; ignore and continue
        pass
except Exception:
    pass

# A shutdown event used by background threads
shutdown_event = threading.Event()

# ========== MQTT CONFIG ==========
MQTT_BROKER = os.environ.get("MQTT_BROKER", "nfa1412a.ala.asia-southeast1.emqxsl.com")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "TN/IOT/smarthome/tt")
MQTT_USER = os.environ.get("MQTT_USER", None)
MQTT_PASS = os.environ.get("MQTT_PASS", None)

# Optionally require credentials to be present (useful for production)
REQUIRE_CREDS = os.environ.get("REQUIRE_MQTT_CREDS", "0") == "1"

if not MQTT_USER or not MQTT_PASS:
    if REQUIRE_CREDS:
        logger.error("MQTT credentials required but missing; refusing to start. Set MQTT_USER and MQTT_PASS in environment or unset REQUIRE_MQTT_CREDS.")
        sys.exit(1)
    else:
        logger.warning("MQTT credentials not provided via env vars; using empty credentials is insecure.\nSet MQTT_USER and MQTT_PASS in environment to avoid hardcoding secrets.")

import argparse

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--enable-baseline-scheduler", action="store_true", help="Enable background baseline scheduler")
parser.add_argument("--rebuild-baseline", action="store_true", help="Rebuild baseline from CSV on startup")
parser.add_argument("--rebuild-days", type=int, default=None, help="Use only last N days for rebuild")
parser.add_argument("--enable-daily-report", action="store_true", help="Enable daily billing report (logs)")
parser.add_argument("--report-hour", type=int, default=None, help="UTC hour (0-23) to emit daily report; default=0 or env REPORT_HOUR")
args, _ = parser.parse_known_args()

init_csv()
# Setup rotating log file
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
try:
    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(os.path.join(LOG_DIR, "ai_smart_home.log"), maxBytes=1_000_000, backupCount=7, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
    logger.addHandler(file_handler)
except Exception:
    logger.exception("Could not set up file logger")

# Start the background baseline writer thread (handles queued updates and periodic flush)
try:
    start_baseline_writer()
    logger.info("Baseline writer started (background)")
except Exception:
    logger.exception("Failed to start baseline writer")

# Optionally start baseline scheduler if env var set or CLI flag used
enable_sched_env = os.environ.get("ENABLE_BASELINE_SCHED", "0") == "1"
if enable_sched_env or args.enable_baseline_scheduler:
    try:
        start_scheduler(interval_days=7)
        logger.info("Baseline scheduler started (background)")
    except Exception:
        logger.exception("Failed to start baseline scheduler")

# Optionally rebuild baseline on startup
if args.rebuild_baseline:
    try:
        from baseline_manager import rebuild_baseline_from_csv
        rebuild_baseline_from_csv(days_window=args.rebuild_days)
        logger.info("Baseline rebuilt on startup")
    except Exception:
        logger.exception("Baseline rebuild failed on startup")

# Optionally start daily report thread
enable_report_env = os.environ.get("ENABLE_DAILY_REPORT", "0") == "1"
report_hour_env = os.environ.get("REPORT_HOUR", None)
report_hour = args.report_hour if args.report_hour is not None else (int(report_hour_env) if report_hour_env is not None else 0)
if enable_report_env or args.enable_daily_report:
    try:
        from billing_predictor import project_monthly_energy_and_cost, format_monthly_report

        def _reporter_loop(hour_utc: int):
            logger.info("Daily reporter started, will emit report at hour %s UTC", hour_utc)
            while not shutdown_event.is_set():
                now = datetime.now(timezone.utc)
                next_run = datetime(now.year, now.month, now.day, hour_utc, tzinfo=timezone.utc)
                if next_run <= now:
                    next_run += timedelta(days=1)
                wait_seconds = (next_run - now).total_seconds()
                # Wait until next run or shutdown
                shutdown_event.wait(timeout=wait_seconds)
                if shutdown_event.is_set():
                    break
                try:
                    res = project_monthly_energy_and_cost(as_of=datetime.now(timezone.utc))
                    report_text = format_monthly_report(res)
                    logger.info(report_text)
                    # append to a per-day report file
                    os.makedirs(os.path.join(LOG_DIR, "reports"), exist_ok=True)
                    fname = os.path.join(LOG_DIR, "reports", datetime.now(timezone.utc).strftime('report-%Y-%m-%d.txt'))
                    with open(fname, "a", encoding="utf-8") as rf:
                        rf.write(report_text + "\n")
                        rf.flush()
                        try:
                            os.fsync(rf.fileno())
                        except Exception:
                            pass
                except Exception:
                    logger.exception("Error while generating daily report")

        reporter_thread = threading.Thread(target=_reporter_loop, args=(report_hour,), daemon=True)
        reporter_thread.start()
    except Exception:
        logger.exception("Failed to start daily reporter")

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())

    logger.info("📥 DỮ LIỆU MỚI: %s", data)

    save_data(data)

    # Update baseline incrementally with the new row (enqueue for background writer)
    try:
        row = {"time": datetime.now(timezone.utc).isoformat(), "power_mW": data.get("power_mW")}
        from baseline_manager import enqueue_update_with_row
        enqueued = enqueue_update_with_row(row)
        if not enqueued:
            # Fallback: do synchronous update to avoid losing the datapoint
            update_with_row(row)
    except Exception:
        logger.exception("Failed to update baseline with new row")

    logger.info(analyze_realtime())

    anomaly = detect_anomaly()
    if anomaly:
        logger.warning(anomaly)

    logger.info(daily_summary())

    for tip in energy_advice(data):
        logger.info(tip)

client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.tls_set()
client.on_message = on_message

def _graceful_shutdown(signum, frame):
    logger.info("Received signal %s, shutting down gracefully...", signum)
    try:
        # stop MQTT loop and disconnect
        try:
            client.disconnect()
            client.loop_stop()
        except Exception:
            logger.exception("Error stopping MQTT client")

        # stop background scheduler
        try:
            stop_scheduler()
        except Exception:
            logger.exception("Error stopping scheduler")

        # stop baseline writer (flush remaining updates)
        try:
            stop_baseline_writer()
        except Exception:
            logger.exception("Error stopping baseline writer")

        # save baseline to ensure latest state persisted
        try:
            bl = load_baseline()
            save_baseline(bl)
        except Exception:
            logger.exception("Error saving baseline during shutdown")

    finally:
        shutdown_event.set()
        # exit process
        try:
            sys.exit(0)
        except SystemExit:
            pass

# register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, _graceful_shutdown)
try:
    signal.signal(signal.SIGTERM, _graceful_shutdown)
except Exception:
    # Windows may not support SIGTERM in the same way
    pass

client.connect(MQTT_BROKER, MQTT_PORT)
client.subscribe(MQTT_TOPIC)

logger.info("🤖 AI SMART HOME ĐANG CHẠY...")
try:
    client.loop_forever()
except KeyboardInterrupt:
    # fallback if signal did not trigger
    _graceful_shutdown(signal.SIGINT, None)

# Wait a moment for shutdown tasks to complete
shutdown_event.wait(timeout=5)
