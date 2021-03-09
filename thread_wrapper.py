import threading

def get_thread_by_name(name):
    threads = threading.enumerate()
    for thread in threads:
        if thread.name == name:
            return thread