import psutil
import time
import threading
from time_efficiency import time_efficiency_decorator
import pandas as pd

memory_utilization = []
def print_mem_utilization():
    """
    start this to print mem utilization every 5 sec
    """
    while True:
        memory_util = psutil.virtual_memory().percent
        print("Memory Utilization: {0}".format(memory_util))
        memory_utilization.append(memory_util)
        time.sleep(5)


@time_efficiency_decorator
def memory_benchmark():
    """
    keep adding * to a list to increase memory utilization
    """
    bytearr = bytearray()
    for i in range(0, 1024*1024*256):
        bytearr.append(5)

if __name__ == '__main__':
    results = {
        'Trial': [],
        'Time Taken': [],
        'Memory Utilization': []
    }
    mem_stats_thread = threading.Thread(target=print_mem_utilization, name='mem_stats_thread')
    mem_stats_thread.daemon = True
    mem_stats_thread.start()
    for i in range(100):
        time_taken = memory_benchmark()
        mem_util_string = ', '.join(str(o) for o in memory_utilization)
        results['Trial'].append(i+1)
        results['Time Taken'].append(time_taken)
        results['Memory Utilization'].append(mem_util_string)
        memory_utilization.clear()
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('mem_test_results.csv', index=False)
