import psutil
import random
import time
import threading
from time_efficiency import time_efficiency_decorator

random.seed(time.time())

def print_cpu_and_mem_utilization():
    """
    start this to print cpu utilization every 5 sec
    """
    while True:
        print(f"CPU percent: {psutil.cpu_percent(interval=5, percpu=True)}")
        print(f"Memory Utilization: {psutil.virtual_memory()}")

@time_efficiency_decorator
def memory_benchmark():
    """
    keep adding * to a list to increase memory utilization
    """
    lst = []
    for i in range(0, 1024*1024*1024):
        lst.append('*' * 1024)
        


if __name__ == '__main__':
    cpu_stats_thread = threading.Thread(target=print_cpu_and_mem_utilization, name='cpu_stats_thread')
    cpu_stats_thread.daemon = True
    cpu_stats_thread.start()
    memory_benchmark()