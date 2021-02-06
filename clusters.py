import sqlalchemy
from datetime import datetime
import re
import mysql.connector
import os
import pandas as pd
from multiprocessing import pool
import multiprocessing
from multiprocessing import Process, Pool
from sklearn.decomposition import PCA, IncrementalPCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
from sklearn import preprocessing
import numpy as np
import sys
from gensim.parsing.preprocessing import strip_tags, strip_punctuation, strip_short
from gensim.parsing.preprocessing import strip_multiple_whitespaces, strip_numeric, stem_text
from gensim.parsing.preprocessing import strip_non_alphanum, remove_stopwords, preprocess_string
from sklearn.decomposition import PCA, IncrementalPCA, TruncatedSVD
import json
from Utils.sql import SqlFuncs
from Utils.functions import Functions, Time
from Utils.es import Es
from tqdm import tqdm
from prometheus_client import start_http_server, Summary, Counter, Gauge

RUN_TIME = Summary('run_time', 'Total run time')
TOTAL_TASKS = Summary('total_tasks', 'Total number of tasks to be processed')
TASKS_COMPLETED = Counter('tasks_completed', 'Count of tasks completed')

TOTAL_ITEMS = Summary('total_items', 'Total number of items to be processed')
TOTAL_ITEMS_COMPLETED = Counter('items_completed', 'Count of items completed')

start = datetime.now()


# model_path = 'C:\\CLUSTERING_MODEL'
# conf = getconf2()
# engine = sqlalchemy.create_engine(f'mysql+pymysql://{conf[1]}:{conf[2]}@{conf[0]}:3306/{conf[3]}')

class Clusters(SqlFuncs, Functions, Es):
    def __init__(self, conf, num_process, parallel, update_stat):
        self.conf = conf
        self.num_process = num_process
        self.parallel = parallel
        self.update_stat = update_stat

        stop_words = []
        with open("C:\\API\\Utils\\stopwords.txt", "r", encoding="utf-8") as f:
            for line in f:
                stop_words.append(str(line.strip()))

        new_stp_wrds = ['00', 'abbã³l', 'acaba', 'acerca', 'aderton', 'ahimã', 'ain', 'akã', 'alapjã', 'alors', 'alã', 'alã³l', 'alã³la', 'alã³lad', 'alã³lam', 'alã³latok', 'alã³luk', 'alã³lunk', 'amã', 'annã', 'appendix', 'arrã³l', 'attã³l', 'azokbã³l', 'azokkã', 'azoknã', 'azokrã³l', 'azoktã³l', 'azokã', 'aztã', 'azzã', 'azã', 'ba', 'bahasa', 'bb', 'bban', 'bbi', 'bbszã', 'belã', 'belã¼l', 'belå', 'bennã¼k', 'bennã¼nk', 'bã', 'bãºcsãº', 'cioã', 'cittã', 'ciã²', 'conjunctions', 'cosã', 'couldn', 'csupã', 'daren', 'didn', 'dik', 'diket', 'doesn', 'don', 'dovrã', 'ebbå', 'effects', 'egyedã¼l', 'egyelå', 'egymã', 'egyã', 'egyã¼tt', 'egã', 'ek', 'ellenã', 'elså', 'elã', 'elå', 'ennã', 'enyã', 'ernst', 'errå', 'ettå', 'ezekbå', 'ezekkã', 'ezeknã', 'ezekrå', 'ezektå', 'ezekã', 'ezentãºl', 'ezutã', 'ezzã', 'ezã', 'felã', 'forsûke', 'fã', 'fûr', 'fûrst', 'ged', 'gen', 'gis', 'giã', 'gjûre', 'gre', 'gtã', 'gy', 'gyet', 'gã', 'gã³ta', 'gã¼l', 'gã¼le', 'gã¼led', 'gã¼lem', 'gã¼letek', 'gã¼lã¼k', 'gã¼lã¼nk', 'hadn', 'hallã³', 'hasn', 'haven', 'herse', 'himse', 'hiã', 'hozzã', 'hurrã', 'hã', 'hãºsz', 'idã', 'ig', 'igazã', 'immã', 'indonesia', 'inkã', 'insermi', 'ismã', 'isn', 'juk', 'jã', 'jã³', 'jã³l', 'jã³lesik', 'jã³val', 'jã¼k', 'kbe', 'kben', 'kbå', 'ket', 'kettå', 'kevã', 'khã', 'kibå', 'kikbå', 'kikkã', 'kiknã', 'kikrå', 'kiktå', 'kikã', 'kinã', 'kirå', 'kitå', 'kivã', 'kiã', 'kkel', 'knek', 'knã', 'korã', 'kre', 'krå', 'ktå', 'kã', 'kã¼lã', 'lad', 'lam', 'latok', 'ldã', 'led', 'leg', 'legalã', 'lehetå', 'lem', 'lennã', 'leszã¼nk', 'letek', 'lettã¼nk', 'ljen', 'lkã¼l', 'll', 'lnak', 'ltal', 'ltalã', 'luk', 'lunk', 'lã', 'lã¼k', 'lã¼nk', 'magã', 'manapsã', 'mayn', 'megcsinã', 'mellettã¼k', 'mellettã¼nk', 'mellã', 'mellå', 'mibå', 'mightn', 'mikbå', 'mikkã', 'miknã', 'mikrå', 'miktå', 'mikã', 'mindenã¼tt', 'minã', 'mirå', 'mitå', 'mivã', 'miã', 'modal',
                        'mostanã', 'mustn', 'myse', 'mã', 'mãºltkor', 'mãºlva', 'må', 'måte', 'nak', 'nbe', 'nben', 'nbã', 'nbå', 'needn', 'nek', 'nekã¼nk', 'nemrã', 'nhetå', 'nhã', 'nk', 'nnek', 'nnel', 'nnã', 'nre', 'nrå', 'nt', 'ntå', 'nyleg', 'nyszor', 'nã', 'nå', 'når', 'også', 'ordnung', 'oughtn', 'particles', 'pen', 'perchã', 'perciã²', 'perã²', 'pest', 'piã¹', 'puã²', 'pã', 'quelqu', 'qué', 'ra', 'rcsak', 'rem', 'retrieval', 'rlek', 'rmat', 'rmilyen', 'rom', 'rt', 'rte', 'rted', 'rtem', 'rtetek', 'rtã¼k', 'rtã¼nk', 'rã', 'rã³la', 'rã³lad', 'rã³lam', 'rã³latok', 'rã³luk', 'rã³lunk', 'rã¼l', 'sarã', 'schluss', 'semmisã', 'shan', 'shouldn', 'sik', 'sikat', 'snap', 'sodik', 'sodszor', 'sokat', 'sokã', 'sorban', 'sorã', 'sra', 'st', 'stb', 'stemming', 'study', 'sz', 'szen', 'szerintã¼k', 'szerintã¼nk', 'szã', 'sã', 'talã', 'ted', 'tegnapelå', 'tehã', 'tek', 'tessã', 'tha', 'tizenhã', 'tizenkettå', 'tizenkã', 'tizennã', 'tizenã', 'tok', 'tovã', 'tszer', 'tt', 'tte', 'tted', 'ttem', 'ttetek', 'ttã¼k', 'ttã¼nk', 'tulsã³', 'tven', 'tã', 'tãºl', 'tå', 'ul', 'utoljã', 'utolsã³', 'utã', 'vben', 'vek', 'velã¼k', 'velã¼nk', 'verbs', 'ves', 'vesen', 'veskedjã', 'viszlã', 'viszontlã', 'volnã', 'vvel', 'vã', 'vå', 'vöre', 'vört', 'wahr', 'wasn', 'weren', 'won', 'wouldn', 'zadik', 'zat', 'zben', 'zel', 'zepesen', 'zepã', 'zã', 'zã¼l', 'zå', 'ã³ta', 'ãºgy', 'ãºgyis', 'ãºgynevezett', 'ãºjra', 'ãºr', 'ð¾da', 'γα', 'البت', 'بالای', 'برابر', 'برای', 'بیرون', 'تول', 'توی', 'تی', 'جلوی', 'حدود', 'خارج', 'دنبال', 'روی', 'زیر', 'سری', 'سمت', 'سوی', 'طبق', 'عقب', 'عل', 'عنوان', 'قصد', 'لطفا', 'مد', 'نزد', 'نزدیک', 'وسط', 'پاعین', 'کنار', 'अपन', 'अभ', 'इत', 'इनक', 'इसक', 'इसम', 'उनक', 'उसक', 'एव', 'ऐस', 'करत', 'करन', 'कह', 'कहत', 'गय', 'जह', 'तन', 'तर', 'दब', 'दर', 'धर', 'नस', 'नह', 'पहल', 'बन', 'बह', 'यत', 'यद', 'रख', 'रह', 'लक', 'वर', 'वग़', 'सकत', 'सबस', 'सभ', 'सर', 'ἀλλ']
        final_stp_wrds = stop_words + new_stp_wrds
        self.stop_words = final_stp_wrds

    def getStatus(self, current):
        if self.update_stat:
            client = self.get_client("144.167.35.89")
            query = {
                "size": 1000,
                "query": {
                    "term": {
                        "tid.keyword": {
                            "value": self.tid,
                            "boost": 1.0
                        }
                    }
                },
                "_source": {
                    "includes": [
                        "C10xy",
                        "C1xy",
                        "C2xy",
                        "C3xy",
                        "C4xy",
                        "C5xy",
                        "C6xy",
                        "C7xy",
                        "C8xy",
                        "C9xy",
                        "cluster_1",
                        "cluster_10",
                        "cluster_2",
                        "cluster_3",
                        "cluster_4",
                        "cluster_5",
                        "cluster_6",
                        "cluster_7",
                        "cluster_8",
                        "cluster_9",
                        "cluster_id",
                        "last_modified_time",
                        "status",
                        "status_percentage",
                        "svd",
                        "tid",
                        "total"
                    ],
                    "excludes": []
                },
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }

            json_body = {}
            result = self.search_record(client, "clusters", query)
            if result['hits']['hits']:
                json_body = result['hits']['hits'][0]['_source']

            status = current

            if int(status) != 100:
                json_body["status"] = 0
                json_body["status_percentage"] = status
            else:
                json_body["status"] = 1
                json_body["status_percentage"] = status

            json_body["last_modified_time"] = str(datetime.now())

            insert = self.insert_record(
                client, "clusters", self.tid, "", json_body)
            if not insert:
                print(f"Data for tracker {self.tid} not inserted")
            client.transport.close()

        # status = current
        # config = self.conf
        # ip = config[0]
        # user_name = config[1]
        # password = config[2]
        # db = config[3]

        # mydb = mysql.connector.connect(
        #     host=ip, user=user_name, passwd=password, database=db)
        # mycursor = mydb.cursor()

        # if int(status) != 100:
        #     sql = "update clusters set status = %s, status_percentage = %s where tid = %s"
        #     mycursor.execute(sql, (str(0), str(status), str(self.tid)))
        # else:
        #     sql = "update clusters set status = %s, status_percentage = %s where tid = %s"
        #     mycursor.execute(sql, (str(1), str(status), str(self.tid)))

        # mydb.commit()
        # mycursor.close()
        # mydb.close()

    def remove(self, text):
        CUSTOM_FILTERS = [lambda x: x.lower(),  # lowercase
                          strip_multiple_whitespaces,
                          strip_non_alphanum,
                          strip_numeric,
                          remove_stopwords,
                          strip_short,
                          #                   stem_text
                          ]
        text = text.lower()
        example_sent = preprocess_string(text, CUSTOM_FILTERS)
        # print('done processing')
        filtered_sentence = [
            w for w in example_sent if not w in self.stop_words]

        return filtered_sentence

    def counter(self, text):
        from collections import Counter
        st = self.remove(text)
        counter_obj = Counter(st)
        return str(counter_obj.most_common(100)), str(counter_obj.most_common(1)[0][0])

    def get_topterms(self, post_ids):
        # q_topterms = f'select terms from blogpost_terms_api where blogpost_id in ({post_ids})'
        q_topterms = f"""select n.term, sum(n.occurr) occurrence 
                        from blogpost_terms_api, json_table(terms_test, '$[*]' columns( term varchar(128) path '$.term', occurr int(11) path '$.occurrence' ) ) as n 
                        where blogpost_id in ({post_ids}) 
                        group by n.term 
                        order by occurrence desc 
                        limit 100"""
        topterms_result = self.query(self.conf, q_topterms)
        # topterms_result

        term_dict = {}
        for rec in topterms_result:
            term_dict[rec['term']] = int(rec['occurrence'])
        # for i in range(len(topterms_result)):
        #     t = topterms_result[i]['terms'].replace('),', '----').replace('(', '').replace(
        #         ']', '').replace('[', '').replace(')', '').replace('\'', '').split('----')
        #     for elem in t:
        #         if elem != 'BLANK':
        #             key = elem.split(',')[0].replace('\'', '').strip()
        #             val = elem.split(',')[1].replace('\'', '').strip()
        #             if key in term_dict:
        #                 v = term_dict[key]
        #                 new_v = v + int(val)
        #                 term_dict[key] = new_v
        #             else:
        #                 term_dict[key] = int(val)

        # term_dict_sorted = {k: v for k, v in sorted(
        #     term_dict.items(), key=lambda item: item[1], reverse=True)}
        # final_terms = {}
        # top_100 = list(term_dict_sorted.keys())[0:100]

        # for key in top_100:
        #     final_terms[key] = term_dict_sorted[key]
        return term_dict

    def cleanbrackets(self, s):
        s_new = ''

        if s == '':
            return s_new
        if s[-1] == ',':
            s_new = s[0:-1]
        elif s[-2] == ',' and s[-1] == ')':
            s_new = s[0:-2] + ')'
        else:
            s_new = s

        return s_new

    def getClusterforall(self, tid):
        self.tid = tid["tid"]
        current_ = 5
        q_trackers = f"select * from trackers where tid = {self.tid}"
        tracker_result = self.query(self.conf, q_trackers)
        blogsite_ids = tracker_result[0]['query'].replace(
            'blogsite_id in', '').strip()
        blogsite_ids = self.cleanbrackets(
            blogsite_ids) if blogsite_ids else None

        if blogsite_ids != '()' and 'NaN' not in blogsite_ids:
            q_post = f"select blogpost_id, post from blogposts where blogsite_id in {blogsite_ids}"
            post_result = self.query(self.conf, q_post)

            current_ += 5
            self.getStatus(current_)

            post_ids_ = []
            posts = []

            for i in range(len(post_result)):
                post_ids_.append(post_result[i]['blogpost_id'])
                posts.append(post_result[i]['post'])

        # -------------------------------------------
            documents = list(map(str, posts))

            vectorizer = TfidfVectorizer(stop_words=self.stop_words)
            current_ += 10
            self.getStatus(current_)

            if documents and len(documents) >= 2:
                vectorizer.fit(documents)

                current_ += 10
                self.getStatus(current_)
                # print('done fitting data')
                X = vectorizer.transform(documents)

                data = TruncatedSVD(n_components=2, algorithm='arpack')
                new_data = data.fit_transform(X)

                current_ += 10
                self.getStatus(current_)

            # --------------------------------------------

                # data = []
                # for i in range(len(svd_result)):
                #     data.append(np.array(' '.join(svd_result[i][1].split()).split(' ')))
                # npa = np.asarray(data, dtype=np.float32)

            # ---------------------------------------------
                npa = np.asarray(new_data, dtype=np.float32)

                post_id_svd = {}
                for post_ids, svd in zip(post_ids_, new_data):
                    post_id_svd[f'{post_ids}'] = str(svd)
            # ---------------------------------------------

                model = KMeans(n_clusters=10)
                model.fit(npa)
                centroids = model.cluster_centers_
                labels = model.labels_

            # ----------------------------------------------
                # print(len(post_ids_), len(posts), len(npa))
                # update_svd(npa, post_ids_, conf)

            # ----------------------------------------------
                # print(len(post_ids_), len(posts), len(svd_result), len(data), len(npa))

                # print('model and necessary parameters are ready')

                current_ += 10
                self.getStatus(current_)

                def func(x): return str(x)
                new_list = list(map(func, post_ids_))

                df = pd.DataFrame()
                df['post_id_incluster'] = new_list
                df['cluster'] = labels

                test = df.groupby(['cluster'])[
                    'post_id_incluster'].apply(','.join)
                # test_df_transposed = pd.DataFrame(test).transpose()
                # test_df_transposed.insert(0, "cluster_id", self.tid, True)
                # test_df_transposed.insert(1, "tid", self.tid, True)
                # test_df_transposed.insert(2, "total", len(post_ids_), True)
                # shape = test_df_transposed.shape

                arr = []
                for i in range(10):
                    # idx = shape[1] + i
                    labll = f"C{i+1}xy"
                    arr.append(labll)
                    cent = str(list(centroids[i]))
                    # test_df_transposed.insert(idx, labll, cent, True)

                # arr2 = ["cluster_id", "tid", "total", "cluster_1", "cluster_2", "cluster_3", "cluster_4",
                #         "cluster_5", "cluster_6", "cluster_7", "cluster_8", "cluster_9", "cluster_10"]
                # newarrs = arr2 + arr
                # test_df_transposed.columns = newarrs

                elastic_dict = {
                    "_index": "clusters",
                    "_id": self.tid,
                    "_source": {}
                }
                for i in range(10):
                    dic = {}

                    pids = test[i]

                    dic['post_ids'] = pids
                    dic['topterms'] = self.get_topterms(pids)

                    # test_df_transposed[f'cluster_{i + 1}'] = str(dic)

                    current_ += 5
                    self.getStatus(current_)

                    elastic_dict["_source"][f"C{i+1}xy"] = str(
                        list(centroids[i]))
                    elastic_dict["_source"][f'cluster_{i + 1}'] = str(dic)
                    elastic_dict["_source"]["status_percentage"] = current_

                elastic_dict["_source"]["svd"] = str(post_id_svd)
                elastic_dict["_source"]["total"] = len(post_ids_)
                elastic_dict["_source"]["tid"] = self.tid
                elastic_dict["_source"]["status"] = 1
                elastic_dict["_source"]["cluster_id"] = self.tid
                elastic_dict["_source"]["last_modified_time"] = str(datetime.now())

                # test_df_transposed['svd'] = str(post_id_svd)

                # p = npa, post_ids_
                # return test_df_transposed, p
                return elastic_dict

    def insert_single_cluster(self, status, status_percentage):
        sql = "insert into clusters (cluster_id, tid, status, status_percentage) values (%s, %s, %s, %s)"
        data = (str(self.tid), str(self.tid), str(
            status), str(status_percentage))
        self.update_insert(sql, data, self.conf)

    def insert_to_cluster(self, data):
        error = False
        items = tuple([str(x) for x in data.iloc[0].tolist()])
        cols = "`,`".join([str(i) for i in data.columns.tolist()])

        insert_query = '''INSERT INTO clusters (`''' + cols + '''`) 
                            values (''' + '%s, ' * (len(items) - 1) + '''%s) '''

        cols2 = "=%s, ".join([str(i) for i in data.columns.tolist()[1:]])
        update_query = '''
            UPDATE clusters SET ''' + cols2 + '''=%s where cluster_id = %s 
        '''
        new_list = list(items[1:])
        new_list.append(items[0])
        update = tuple(new_list)

        if 'Duplicate entry' in self.update_insert(insert_query, items, self.conf):
            self.update_insert(update_query, update, self.conf)

    def func(self, x):
        a = str(x).replace('[', '').replace(']', '')
        b = ' '.join(a.split()).split()

        x1 = '{0:.20f}'.format(float(b[0]))
        y1 = '{0:.20f}'.format(float(b[1]))
        return f'{x1} {y1}'

    def process_clusters(self):
        q_trackers = """select tid from trackers  order by tid desc limit 15"""
        tracker_result = self.query(self.conf, q_trackers)
        TASKS_COMPLETED.inc()

        if self.parallel:
            TOTAL_ITEMS.observe(len(tracker_result))
            with Pool(processes=self.num_process) as process_pool:
                with tqdm(total=len(tracker_result), desc="Clusters", file=sys.stdout, postfix="\n") as pbar:
                    # with tqdm(total=len(tracker_result), desc="Clusters", file=sys.stdout) as pbar:
                    main_time = Time()
                    for i, d in enumerate(process_pool.imap_unordered(self.getClusterforall, tracker_result)):
                        client = self.get_client("144.167.35.89")

                        if d:
                            json_body = d['_source']
                            insert = self.insert_record(
                                client, "clusters", tracker_result[i]["tid"], "", json_body)
                            if not insert:
                                print(
                                    f"Data for tracker {tracker_result[i]['tid']} not inserted")
                            client.transport.close()
                            pbar.update()

                        TOTAL_ITEMS_COMPLETED.inc()

            print("Finished processing!")
            print("\n\nDatabase up to date!")
            main_time.finished()

            print("\nClosing pool")
            process_pool.close()
            print("Joining pool")
            process_pool.join()
            print("Clearing pool")
            # process_pool.clear()
            print("Finished!")
        else:
            for x in tqdm(tracker_result, desc="Clusters", ascii=True,  file=sys.stdout):
                d = self.getClusterforall(x)
                client = self.get_client("144.167.35.89")
                if d:
                    json_body = d['_source']
                    insert = self.insert_record(
                        client, "clusters", x["tid"], "", json_body)
                    if not insert:
                        print(f"Data for tracker {x['tid']} not inserted")
                    client.transport.close()

        TASKS_COMPLETED.inc()


@RUN_TIME.time()
def main():
    parallel = True
    num_processes = 6
    functions = Functions()
    connect = functions.get_config()
    update_stat = False

    c = Clusters(connect, num_processes, parallel, update_stat)
    TOTAL_TASKS.observe(2)
    c.process_clusters()


if __name__ == "__main__":
    start_http_server(8009)
    main()
