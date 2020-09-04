if __name__ == "__main__":
    from datetime import datetime
    from tqdm import tqdm
    from pathos.multiprocessing import freeze_support, ProcessPool
    import sys
    sys.path.append('C:\API_LIVE\API\Cluster')
    import clusters as c

    parallel = True
    num_processes = 4
    # if __name__ == '__main__':
    # print(f'STARTED AT - {datetime.today().isoformat()}')
    conf = c.getconf2()
    # q_trackers = f'''select t.tid
    #                 from trackers t 
    #                 left join clusters clst 
    #                 on  t.tid = clst.tid 
    #                 where t.tid is null 
    #                 or clst.tid is null 
    #                 or clst.status_percentage < 100 
    #                 or clst.status != 1 
    #                 or clst.status_percentage is null 
    #                 or clst.status is null 
    #                 or clst.svd is null '''

    # q_trackers = """select tid 
    #                 from clusters 
    #                 where tid 
    #                 in(select tid 
    #                 from trackers 
    #                 where YEAR(date_created) in (2019,2020) 
    #                 and userid = 'cosmographers@gmail.com') 
    #                 and DATE(last_modified_time) != CURDATE() 
    #                 """

    q_trackers = """select tid from trackers"""

    tracker_result = c.query(conf, q_trackers)
    def process_updates(x):
        import clusters as c
        conf = c.getconf2()

        tid = x[0]
        all_ = c.getClusterforall(tid, conf)
        if all_:
            c.insert_to_cluster(conf,all_[0], tid)
            c.getStatus(conf, tid, 100)

    if parallel:
        process_pool = ProcessPool(num_processes)
        for record in tqdm(process_pool.uimap(process_updates, tracker_result), desc="Clusters", ascii=True,  file=sys.stdout, total=len(tracker_result)):
            pass
    else:
        for x in tqdm(tracker_result, desc="Clusters", ascii=True,  file=sys.stdout):
            process_updates(x)


