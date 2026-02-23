import time
from datetime import datetime, timezone
import baseline_manager
import os


def test_enqueue_and_process(tmp_path):
    # Ensure clean baseline file
    path = tmp_path / "baseline.json"
    if path.exists():
        os.remove(path)

    baseline_manager.BASELINE_FILE = str(path)

    # Start writer
    baseline_manager.start_baseline_writer(flush_interval=0.5)

    # Enqueue multiple rows
    ts = datetime.now(timezone.utc)
    for i in range(10):
        row = {"time": ts.isoformat(), "power_mW": 100 + i}
        assert baseline_manager.enqueue_update_with_row(row)
        print('after enqueue', i, 'qsize=', baseline_manager._writer_queue.qsize(), 'thread_alive=', bool(baseline_manager._writer_thread and baseline_manager._writer_thread.is_alive()))

    # Give writer some time to process
    time.sleep(1.5)

    print('post-sleep qsize=', baseline_manager._writer_queue.qsize())
    bl = baseline_manager.load_baseline(str(path))
    hour = str(ts.hour)
    assert bl["hourly"][hour]["count"] >= 10

    # Stop writer and ensure final flush
    baseline_manager.stop_baseline_writer()


def test_writer_flush_on_stop(tmp_path):
    path = tmp_path / "baseline2.json"
    if path.exists():
        os.remove(path)
    baseline_manager.BASELINE_FILE = str(path)
    baseline_manager.start_baseline_writer(flush_interval=0.5)

    ts = datetime.now(timezone.utc)
    row = {"time": ts.isoformat(), "power_mW": 555}
    assert baseline_manager.enqueue_update_with_row(row)

    # Stop immediately; writer should flush on stop
    baseline_manager.stop_baseline_writer()

    bl = baseline_manager.load_baseline(str(path))
    hour = str(ts.hour)
    assert bl["hourly"][hour]["count"] >= 1
