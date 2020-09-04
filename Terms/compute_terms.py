import pandas as pd
from tqdm import tqdm
import sqlalchemy
import time
from multiprocessing import Process, Pool
from pathos.multiprocessing import freeze_support, ProcessPool
import multiprocessing
from datetime import datetime
import re
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import ast
import sys
sys.path.append(r'C:\API_LIVE\API')
from Utils.functions import clean_text, getconf2, updateStatus, getTopKWS, single_process, testingKWT
from Utils.sql import SqlFuncs


if __name__ == "__main__":
    parallel_main = False
    num_processes_main = 6

    # Process updates function
    def process_updates(x):
        from Utils.functions import clean_text, getconf2, updateStatus, getTopKWS, single_process, testingKWT
        num_processes = 24
        update__status = True
        parallel = True
        tid = x['tid']
        # tid = 223
        testingKWT(tid, '144.167.35.89', parallel, update__status, num_processes)

    conf = getconf2()
    s = SqlFuncs(conf)

    # Get tracker id
    connection = s.get_connection(conf)
    with connection.cursor() as cursor:
        # cursor.execute("select tid from trackers where userid = 'cosmographers@gmail.com' or YEAR(date_created) in (2019,2020)")
        # cursor.execute("select tid from trackers")
        cursor.execute("SELECT tid FROM blogtrackers.tracker_keyword where DATE(last_modified_time) != CURDATE() or status != 1 or status_percentage != 100")
        # cursor.execute("select tid from tracker_keyword where tid in( select tid from trackers where YEAR(date_created) in (2019,2020) or userid = 'cosmographers@gmail.com') and DATE(last_modified_time) != CURDATE()")
        records = cursor.fetchall()
    connection.close()

    if parallel_main:
        pool = Pool(int(6))
        pool.map(process_updates, records)
    else:
        for x in tqdm(records, desc="Terms", ascii=True,  file=sys.stdout):
            print(x)
            process_updates(x)

