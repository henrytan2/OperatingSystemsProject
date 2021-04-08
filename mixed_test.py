import os
from time_efficiency import time_efficiency_decorator
import psutil
import time
import threading
import pandas as pd
import math

cpu_utilization = []
cpu_test_running = False
mem_test_running = False
disk_test_running = False
def print_cpu_utilization():
    """
    start this to print cpu utilization every 5 sec
    """
    while True:
        cpu_util = psutil.cpu_percent()
        print("CPU percent: {0}".format(cpu_util))
        cpu_utilization.append(cpu_util)
        time.sleep(5)

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

disk_write_times = []
def print_disk_utilization():
    while True:
        disk_write_time = psutil.disk_io_counters(perdisk=False).write_time / 1000
        print(disk_write_time)
        disk_write_times.append(disk_write_time)
        time.sleep(5)

def cpu_benchmark():
    """
    floating point math to test cpu
    """
    global cpu_test_running
    cpu_test_running = True
    for i in range(250):
        for x in range(1, 1000):
            math.pi * 2 ** x
        for x in range(1, 100000):
            float(x) / math.pi
        # for x in range(1, 10000):
        #     math.pi / x
    cpu_test_running = False
    print('cpu_test done')

def memory_benchmark():
    """
    keep adding * to a list to increase memory utilization
    """
    global mem_test_running
    mem_test_running = True
    bytearr = bytearray()
    for i in range(0, 1024*1024*32):
        bytearr.append(5)
    mem_test_running = False
    print('mem_test done')


def write_to_disk(file, buffer):
    os.write(file, buffer)
    os.fsync(file)

def disk_benchmark(file, block_size, block_count):
    global disk_test_running
    disk_test_running = True
    f = os.open(file, os.O_CREAT | os.O_WRONLY)

    for i in range(block_count):
        buffer = os.urandom(block_size) # random bytes
        write_to_disk(f, buffer)
    disk_test_running = False
    print('disk_test done')

@time_efficiency_decorator
def run_tests():
    cpu_test_thread = threading.Thread(target=cpu_benchmark, name='cpu_test_thread')
    mem_test_thread = threading.Thread(target=memory_benchmark, name='mem_test_thread')
    disk_test_thread = threading.Thread(target=disk_benchmark, args=('disk_test.txt', 1048576, 1), name='disk_test_thread')
    cpu_test_thread.start()
    mem_test_thread.start()
    disk_test_thread.start()
    while cpu_test_running or mem_test_running or disk_test_running:
        pass

if __name__ == '__main__':
    results = {
        'Trial': [],
        'Time Taken': [],
        'CPU Utilization': [],
        'Memory Utilization': [],
    }
    cpu_stats_thread = threading.Thread(target=print_cpu_utilization, name='cpu_stats_thread')
    cpu_stats_thread.daemon = True
    cpu_stats_thread.start()
    mem_stats_thread = threading.Thread(target=print_mem_utilization, name='mem_stats_thread')
    mem_stats_thread.daemon = True
    mem_stats_thread.start()
    disk_stats_thread = threading.Thread(target=print_disk_utilization, name='disk_stats_thread')
    disk_stats_thread.daemon = True
    disk_stats_thread.start()
    for i in range(100):
        time_taken = run_tests()
        results['Trial'].append(i+1)
        cpu_util_string = ', '.join(str(o) for o in cpu_utilization)
        results['CPU Utilization'].append(cpu_util_string)
        cpu_utilization.clear()
        mem_util_string = ', '.join(str(o) for o in memory_utilization)
        results['Time Taken'].append(time_taken)
        results['Memory Utilization'].append(mem_util_string)        
        memory_utilization.clear()
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('mixed_test_results.csv', index=False)