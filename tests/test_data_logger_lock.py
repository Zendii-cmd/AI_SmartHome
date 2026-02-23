import csv
import time
from multiprocessing import Process


def worker(csv_path, values):
    # import inside child process
    import data_logger
    data_logger.CSV_FILE = csv_path
    # Ensure header exists
    data_logger.init_csv()
    for v in values:
        data_logger.save_data({"power_mW": v, "current_mA": None, "voltage": None, "led1": 0, "led2": 0})
        # small sleep to increase chance of contention
        time.sleep(0.005)


def test_concurrent_writes(tmp_path):
    csvfile = str(tmp_path / "electric_data.csv")
    import data_logger
    data_logger.CSV_FILE = csvfile
    data_logger.init_csv()

    N = 50
    p1 = Process(target=worker, args=(csvfile, ["1"] * N))
    p2 = Process(target=worker, args=(csvfile, ["2"] * N))
    p1.start()
    p2.start()
    p1.join()
    p2.join()

    # read back
    with open(csvfile, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    # header + N*2 rows
    assert len(rows) == 1 + N * 2
    values = [r[1] for r in rows[1:]]  # power_mW is second column
    assert values.count("1") == N
    assert values.count("2") == N
