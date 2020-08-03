from datetime import datetime
import time
from tqdm import tqdm
from pathos.multiprocessing import freeze_support, ProcessPool
import sys
sys.path.append('C:\API_LIVE\API\Terms')
# print(sys.path)
from API_TTERMS  import getconf2, query, testingKWT
    
parallel = False
num_processes = 6
if __name__ == "__main__":
    def process_updates(x):
        from API_TTERMS  import getconf2, query, testingKWT
        # tid = x[0]
        tid = 428
        testingKWT(tid, '144.167.35.89')

    conf = getconf2()
    q_trackers = f"select tid from trackers where userid = 'cosmographers@gmail.com' or YEAR(date_created) in (2019,2020)"
    # q_trackers = f"select t.tid from trackers t left join tracker_keyword tk on  t.tid = tk.tid where t.tid is null or tk.tid is null or tk.status_percentage < 100 or tk.status != 1 or tk.status_percentage is null or tk.status is null"
    
    tracker_result = query(conf, q_trackers)
    if parallel:
        process_pool = ProcessPool(num_processes)
        for record in tqdm(process_pool.imap(process_updates, tracker_result), desc="Terms", ascii=True,  file=sys.stdout, total=len(tracker_result)):
            pass
        process_pool.close()
        print("Joining pool")
        process_pool.join()
        print("Clearing pool")
        process_pool.clear()
        print("Finished!")
    else:
        for x in tqdm(tracker_result, desc="Terms", ascii=True,  file=sys.stdout):
            start = time.time()
            process_updates(x)
            end = time.time()
            runtime_mins, runtime_secs = divmod(end - start, 60)
            print("Time to complete: {} Mins {} Secs".format(runtime_mins,runtime_secs), f'AT - {datetime.today().isoformat()}')