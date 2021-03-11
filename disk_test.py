import os
from time_efficiency import time_efficiency_decorator
import psutil
import time
import threading
import pandas as pd

disk_write_times = []
def print_disk_utilization():
    while True:
        disk_write_time = psutil.disk_io_counters(perdisk=False).write_time / 1000
        print(disk_write_time)
        disk_write_times.append(disk_write_time)
        time.sleep(5)


def write_to_disk(file, buffer):
    os.write(file, buffer)
    os.fsync(file)

@time_efficiency_decorator
def write_test(file, block_size, block_count):
    f = os.open(file, os.O_CREAT | os.O_WRONLY)

    for i in range(block_count):
        buffer = os.urandom(block_size) # random bytes
        write_to_disk(f, buffer)

if __name__ == '__main__':
    results = {
        'Trial': [],
        'Time Taken': [],
        'Disk Write Times': []
    }
    disk_stats_thread = threading.Thread(target=print_disk_utilization, name='disk_stats_thread')
    disk_stats_thread.daemon = True
    disk_stats_thread.start()
    for i in range(100):
        time_taken = write_test('disk_test.txt', 104857600, 10)
        disk_util_string = ', '.join(str(o) for o in disk_write_times)
        f = open('disk_test.txt', 'r+')
        f.truncate(0)
        os.remove('disk_test.txt')
        results['Trial'].append(i+1)
        results['Time Taken'].append(time_taken)
        results['Disk Write Times'].append(disk_util_string)
        disk_write_times.clear()
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('cpu_test_results.csv', index=False)
        