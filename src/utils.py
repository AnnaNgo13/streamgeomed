import logging
import os
from time import time
  

def get_logger(topic):
    logpath = "./log"
    make_directory(logpath)
    logname = topic + ".log"
    delete_file(os.path.join(logpath, logname))
    logging.basicConfig(
        filename=os.path.join(logpath, logname),
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    logger = logging.getLogger()
    return logger


def delete_file(filePath):
    try:
        if os.path.exists(filePath):
            os.remove(filePath)
    except:
        print("Error while deleting file ", filePath)


def make_directory(directory):
    if not (os.path.exists(directory)):
        os.makedirs(directory)
        print(f"Created a directory: '{directory}'")

  
def timer_func(func):
    # This function shows the execution time of 
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        # kwargs["logger"].info(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func