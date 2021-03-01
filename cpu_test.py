import psutil
import threading
from timeit import default_timer as timer
import math

def time_efficiency_decorator(func):
    """
    decorator to wrap function and time it
    """
    def wrapper(*args):
        start = timer()
        func(*args)
        end = timer()
        print("Starts at: {0}".format(start))
        print("Ends at: {0}".format(end))
        time_taken = end - start
        print("Time taken to execute the function: {0}".format(time_taken))
    return wrapper

def print_cpu_utilization():
    """
    start this to print cpu utilization every 5 sec
    """
    while True:
        print(f"CPU percent: {psutil.cpu_percent(interval=5, percpu=True)}")
        print(f"CPU freq: {psutil.cpu_freq()}")

@time_efficiency_decorator
def cpu_benchmark():
    """
    floating point math to test cpu
    """
    for i in range(1000):
        for x in range(1, 1000):
            math.pi * 2 ** x
        for x in range(1, 100000):
            float(x) / 3.14592
        for x in range(1, 10000):
            math.pi / x

if __name__ == '__main__':
    cpu_stats_thread = threading.Thread(target=print_cpu_utilization, name='cpu_stats_thread')
    cpu_stats_thread.daemon = True
    cpu_stats_thread.start()
    cpu_benchmark()
