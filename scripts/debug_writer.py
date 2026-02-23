import baseline_manager, time
from datetime import datetime, timezone
import os, tempfile
p = os.path.join(tempfile.gettempdir(), 'bm_test.json')
baseline_manager.BASELINE_FILE = p
if os.path.exists(p): os.remove(p)
baseline_manager.start_baseline_writer(flush_interval=0.5)
ts = datetime.now(timezone.utc)
for i in range(5):
    baseline_manager.enqueue_update_with_row({"time": ts.isoformat(), "power_mW": 100+i})
    print('enqueued', i, 'qsize=', baseline_manager._writer_queue.qsize())
print('sleeping')
time.sleep(2)
print('after sleep qsize=', baseline_manager._writer_queue.qsize())
print('baseline exists?', os.path.exists(p))
print('content=', open(p).read() if os.path.exists(p) else None)
baseline_manager.stop_baseline_writer()
print('done')
