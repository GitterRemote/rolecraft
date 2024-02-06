import argparse
import importlib
import logging
import os
import sys

import rolecraft

parser = argparse.ArgumentParser(
    prog="RoleCraft",
    description="Starting the task consumers and workers",
)

parser.add_argument("module")
parser.add_argument("-t", "--worker-threads", type=int)
parser.add_argument("--verbose", "-v", action="count", default=0)


def main():
    args = parser.parse_args()
    module: str = args.module
    worker_thread_num: int = args.worker_threads or 1
    verbose: int = args.verbose

    if verbose == 1:
        logging_level = logging.INFO
    elif verbose > 1:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.WARNING
    logging.basicConfig(
        level=logging_level, format=f"%(asctime)s:{logging.BASIC_FORMAT}"
    )

    # add CWD to the Python path for searching modules
    sys.path.insert(0, os.getcwd())
    importlib.import_module(module)

    service = rolecraft.ServiceFactory().create(prefetch_size=1)
    service.start(thread_num=worker_thread_num)
    service.join()

    return 0
