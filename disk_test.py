import sys
import os
from time_efficiency import time_efficiency_decorator


@time_efficiency_decorator
def write_to_disk(file, buffer):
    os.write(file, buffer)
    os.fsync(file)

def write_test(file, block_size, block_count):
    f = os.open(file, os.O_CREAT | os.O_WRONLY)

    time_taken_list = []
    for i in range(block_count):
        buffer = os.urandom(block_size) # random bytes
        time_taken = write_to_disk(f, buffer)
        time_taken_list.append(time_taken)

if __name__ == '__main__':
    write_test('disk_test.txt', 1000000000, 10)
        