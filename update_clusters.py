from datetime import datetime
from tqdm import tqdm
from pathos.multiprocessing import freeze_support, ProcessPool
import sys
from multiprocessing import Process, Pool, RLock
from Utils.functions import Functions, Time
from Utils.sql import SqlFuncs
from Utils.es import Es
from clusters import Clusters
from prometheus_client import start_http_server, Summary, Counter, Gauge

RUN_TIME = Summary('run_time', 'Total run time')
TOTAL_TASKS = Summary('total_tasks', 'Total number of tasks to be processed')
TASKS_COMPLETED = Counter('tasks_completed', 'Count of tasks completed')

TOTAL_ITEMS = Summary('total_items', 'Total number of items to be processed')
TOTAL_ITEMS_COMPLETED = Counter('items_completed', 'Count of items completed')
@RUN_TIME.time()
def main():
    parallel = True
    num_processes = 16
    functions = Functions()
    connect = functions.get_config()

    c = Clusters(connect, num_processes, parallel)
    TOTAL_TASKS.observe(2) 
    c.process_clusters()

if __name__ == "__main__":
    start_http_server(8009)
    main()


