from Terms.API_TTERMS  import getconf2, query, testingKWT
from datetime import datetime
from tqdm import tqdm
import sys
if __name__ == '__main__':
    print(f'STARTED AT - {datetime.today().isoformat()}')
    conf = getconf2()
    q_trackers = f"select tid from trackers"
    # q_trackers = f"select t.tid from trackers t join tracker_keyword tk on  t.tid = tk.tid where t.tid is null or tk.tid is null or tk.status_percentage < 100 or tk.status != 1 or tk.status_percentage is null or tk.status is null limit 10"
    tracker_result = query(conf, q_trackers)
    
    for x in tqdm(tracker_result, desc="Terms", ascii=True,  file=sys.stdout):
        tid = x[0]
        testingKWT(tid, '144.167.35.89')
    print(f'ENDED AT - {datetime.today().isoformat()}')