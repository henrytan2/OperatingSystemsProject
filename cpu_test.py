import psutil
import threading
import math
from time_efficiency import time_efficiency_decorator
import pandas as pd

cpu_utilization = []
def print_cpu_utilization():
    """
    start this to print cpu utilization every 5 sec
    """
    while True:
        cpu_util = psutil.cpu_percent(interval=5)
        print(f"CPU percent: {cpu_util}")
        cpu_utilization.append(cpu_util)


@time_efficiency_decorator
def cpu_benchmark():
    """
    floating point math to test cpu
    """
    for i in range(1000):
        for x in range(1, 1000):
            math.pi * 2 ** x
        for x in range(1, 100000):
            float(x) / math.pi
        for x in range(1, 10000):
            math.pi / x

def get_thread_by_name(name):
    threads = threading.enumerate()
    for thread in threads:
        if thread.name == name:
            return thread

if __name__ == '__main__':
    results = {
        'Trial': [], 
        'Time Taken': [], 
        'CPU Utilization': []
    }
    for i in range(100):
        cpu_stats_thread = threading.Thread(target=print_cpu_utilization, name='cpu_stats_thread')
        cpu_stats_thread.daemon = True
        cpu_stats_thread.start()
        time_taken = cpu_benchmark()
        t = get_thread_by_name('cpu_stats_thread')
        results['Trial'].append(i)
        results['Time Taken'].append(time_taken)
        cpu_util_string = ', '.join(str(o) for o in cpu_utilization)
        results['CPU Utilization'].append(cpu_util_string)
        cpu_utilization.clear()
        t.do_run = False
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('cpu_test_results.csv', index=False)

