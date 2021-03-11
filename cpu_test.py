import psutil
import threading
import math
import time
from time_efficiency import time_efficiency_decorator
import pandas as pd

cpu_utilization = []
def print_cpu_utilization():
    """
    start this to print cpu utilization every 5 sec
    """
    while True:
        cpu_util = psutil.cpu_percent()
        print(f"CPU percent: {cpu_util}")
        cpu_utilization.append(cpu_util)
        time.sleep(5)


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

if __name__ == '__main__':
    results = {
        'Trial': [], 
        'Time Taken': [], 
        'CPU Utilization': []
    }
    cpu_stats_thread = threading.Thread(target=print_cpu_utilization, name='cpu_stats_thread')
    cpu_stats_thread.daemon = True
    cpu_stats_thread.start()
    for i in range(100):
        time_taken = cpu_benchmark()
        results['Trial'].append(i + 1)
        results['Time Taken'].append(time_taken)
        cpu_util_string = ', '.join(str(o) for o in cpu_utilization)
        results['CPU Utilization'].append(cpu_util_string)
        cpu_utilization.clear()
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('cpu_test_results.csv', index=False)

