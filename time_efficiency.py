from timeit import default_timer as timer

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
        return time_taken
    return wrapper