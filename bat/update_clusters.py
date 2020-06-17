import Cluster.clusters as c
from datetime import datetime
from tqdm import tqdm
import sys
if __name__ == '__main__':
    print(f'STARTED AT - {datetime.today().isoformat()}')
    conf = c.getconf2()
    q_trackers = f'''select t.tid
                    from trackers t 
                    join clusters clst 
                    on  t.tid = clst.tid 
                    where t.tid is null 
                    or clst.tid is null 
                    or clst.status_percentage < 100 
                    or clst.status != 1 
                    or clst.status_percentage is null 
                    or clst.status is null 
                    or clst.svd is null '''

    tracker_result = c.query(conf, q_trackers)
    # for x in tracker_result:
    #     tid = x[0]
    #     all_ = c.getClusterforall(tid, conf)
    #     c.insert_to_cluster(conf,all_[0], tid)
    #     c.getStatus(conf, tid, 100)

    for x in tqdm(tracker_result, desc="Clusters", ascii=True,  file=sys.stdout):
        tid = x[0]
        all_ = c.getClusterforall(tid, conf)
        c.insert_to_cluster(conf,all_[0], tid)
        c.getStatus(conf, tid, 100)
    
    print(f'ENDED AT - {datetime.today().isoformat()}')
