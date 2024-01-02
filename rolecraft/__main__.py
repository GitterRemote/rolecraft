import argparse
import importlib
import logging

from . import service_factory as _service_factory

parser = argparse.ArgumentParser(
    prog="RoleCraft",
    description="Starting the task consumers and workers",
)

parser.add_argument("module")
parser.add_argument("-w", "--worker-threads", type=int)

if __name__ == "__main__":
    # TODO: add -v (INFO) -vv (DEBUG) and -q
    logging.basicConfig(level=logging.INFO)

    args = parser.parse_args()
    module: str = args.module
    worker_thread_num: int = args.worker_threads or 1

    importlib.import_module(module)

    service = _service_factory.ServiceFactory().create(prefetch_size=1)
    service.start(thread_num=worker_thread_num)
    service.join()
