import sqlalchemy
    

from datetime import datetime


import matplotlib.pyplot as plt
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

start = datetime.now()

def getconf2():
    return ('cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers')


model_path = 'C:\\CLUSTERING_MODEL'
conf = getconf2()
engine = sqlalchemy.create_engine(f'mysql+pymysql://{conf[1]}:{conf[2]}@{conf[0]}:3306/{conf[3]}')


print('done getting config')
# print(ip,user_name,password,db)
def query(config,sql):
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    # sql = f"select * from trackers where tid = {tid}"
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    result = mycursor.fetchall()
    mydb.commit()

    mycursor.close()
    mydb.close()

    return result

def getStatus(conf, tid, current):
    # status = (current/total) * 100
    status = current
    # result = status, tid

    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()
    print('stat',status)
    if int(status) != 100:
        sql = "update clusters set status = %s, status_percentage = %s where tid = %s"
        mycursor.execute(sql,(str(0), str(status), str(tid)))
    else:
        sql = "update clusters set status = %s, status_percentage = %s where tid = %s"
        mycursor.execute(sql,(str(1), str(status), str(tid)))

    mydb.commit()
    mycursor.close()
    mydb.close()

def getClusterforall(tid, conf):
    stop_words = []
    with open("C:\\API\\stopwords.txt", "r", encoding="utf-8") as f:
        for line in f:
            stop_words.append(str(line.strip()))
            
    new_stp_wrds = ['00','abbã³l', 'acaba', 'acerca', 'aderton', 'ahimã', 'ain', 'akã', 'alapjã', 'alors', 'alã', 'alã³l', 'alã³la', 'alã³lad', 'alã³lam', 'alã³latok', 'alã³luk', 'alã³lunk', 'amã', 'annã', 'appendix', 'arrã³l', 'attã³l', 'azokbã³l', 'azokkã', 'azoknã', 'azokrã³l', 'azoktã³l', 'azokã', 'aztã', 'azzã', 'azã', 'ba', 'bahasa', 'bb', 'bban', 'bbi', 'bbszã', 'belã', 'belã¼l', 'belå', 'bennã¼k', 'bennã¼nk', 'bã', 'bãºcsãº', 'cioã', 'cittã', 'ciã²', 'conjunctions', 'cosã', 'couldn', 'csupã', 'daren', 'didn', 'dik', 'diket', 'doesn', 'don', 'dovrã', 'ebbå', 'effects', 'egyedã¼l', 'egyelå', 'egymã', 'egyã', 'egyã¼tt', 'egã', 'ek', 'ellenã', 'elså', 'elã', 'elå', 'ennã', 'enyã', 'ernst', 'errå', 'ettå', 'ezekbå', 'ezekkã', 'ezeknã', 'ezekrå', 'ezektå', 'ezekã', 'ezentãºl', 'ezutã', 'ezzã', 'ezã', 'felã', 'forsûke', 'fã', 'fûr', 'fûrst', 'ged', 'gen', 'gis', 'giã', 'gjûre', 'gre', 'gtã', 'gy', 'gyet', 'gã', 'gã³ta', 'gã¼l', 'gã¼le', 'gã¼led', 'gã¼lem', 'gã¼letek', 'gã¼lã¼k', 'gã¼lã¼nk', 'hadn', 'hallã³', 'hasn', 'haven', 'herse', 'himse', 'hiã', 'hozzã', 'hurrã', 'hã', 'hãºsz', 'idã', 'ig', 'igazã', 'immã', 'indonesia', 'inkã', 'insermi', 'ismã', 'isn', 'juk', 'jã', 'jã³', 'jã³l', 'jã³lesik', 'jã³val', 'jã¼k', 'kbe', 'kben', 'kbå', 'ket', 'kettå', 'kevã', 'khã', 'kibå', 'kikbå', 'kikkã', 'kiknã', 'kikrå', 'kiktå', 'kikã', 'kinã', 'kirå', 'kitå', 'kivã', 'kiã', 'kkel', 'knek', 'knã', 'korã', 'kre', 'krå', 'ktå', 'kã', 'kã¼lã', 'lad', 'lam', 'latok', 'ldã', 'led', 'leg', 'legalã', 'lehetå', 'lem', 'lennã', 'leszã¼nk', 'letek', 'lettã¼nk', 'ljen', 'lkã¼l', 'll', 'lnak', 'ltal', 'ltalã', 'luk', 'lunk', 'lã', 'lã¼k', 'lã¼nk', 'magã', 'manapsã', 'mayn', 'megcsinã', 'mellettã¼k', 'mellettã¼nk', 'mellã', 'mellå', 'mibå', 'mightn', 'mikbå', 'mikkã', 'miknã', 'mikrå', 'miktå', 'mikã', 'mindenã¼tt', 'minã', 'mirå', 'mitå', 'mivã', 'miã', 'modal', 'mostanã', 'mustn', 'myse', 'mã', 'mãºltkor', 'mãºlva', 'må', 'måte', 'nak', 'nbe', 'nben', 'nbã', 'nbå', 'needn', 'nek', 'nekã¼nk', 'nemrã', 'nhetå', 'nhã', 'nk', 'nnek', 'nnel', 'nnã', 'nre', 'nrå', 'nt', 'ntå', 'nyleg', 'nyszor', 'nã', 'nå', 'når', 'også', 'ordnung', 'oughtn', 'particles', 'pen', 'perchã', 'perciã²', 'perã²', 'pest', 'piã¹', 'puã²', 'pã', 'quelqu', 'qué', 'ra', 'rcsak', 'rem', 'retrieval', 'rlek', 'rmat', 'rmilyen', 'rom', 'rt', 'rte', 'rted', 'rtem', 'rtetek', 'rtã¼k', 'rtã¼nk', 'rã', 'rã³la', 'rã³lad', 'rã³lam', 'rã³latok', 'rã³luk', 'rã³lunk', 'rã¼l', 'sarã', 'schluss', 'semmisã', 'shan', 'shouldn', 'sik', 'sikat', 'snap', 'sodik', 'sodszor', 'sokat', 'sokã', 'sorban', 'sorã', 'sra', 'st', 'stb', 'stemming', 'study', 'sz', 'szen', 'szerintã¼k', 'szerintã¼nk', 'szã', 'sã', 'talã', 'ted', 'tegnapelå', 'tehã', 'tek', 'tessã', 'tha', 'tizenhã', 'tizenkettå', 'tizenkã', 'tizennã', 'tizenã', 'tok', 'tovã', 'tszer', 'tt', 'tte', 'tted', 'ttem', 'ttetek', 'ttã¼k', 'ttã¼nk', 'tulsã³', 'tven', 'tã', 'tãºl', 'tå', 'ul', 'utoljã', 'utolsã³', 'utã', 'vben', 'vek', 'velã¼k', 'velã¼nk', 'verbs', 'ves', 'vesen', 'veskedjã', 'viszlã', 'viszontlã', 'volnã', 'vvel', 'vã', 'vå', 'vöre', 'vört', 'wahr', 'wasn', 'weren', 'won', 'wouldn', 'zadik', 'zat', 'zben', 'zel', 'zepesen', 'zepã', 'zã', 'zã¼l', 'zå', 'ã³ta', 'ãºgy', 'ãºgyis', 'ãºgynevezett', 'ãºjra', 'ãºr', 'ð¾da', 'γα', 'البت', 'بالای', 'برابر', 'برای', 'بیرون', 'تول', 'توی', 'تی', 'جلوی', 'حدود', 'خارج', 'دنبال', 'روی', 'زیر', 'سری', 'سمت', 'سوی', 'طبق', 'عقب', 'عل', 'عنوان', 'قصد', 'لطفا', 'مد', 'نزد', 'نزدیک', 'وسط', 'پاعین', 'کنار', 'अपन', 'अभ', 'इत', 'इनक', 'इसक', 'इसम', 'उनक', 'उसक', 'एव', 'ऐस', 'करत', 'करन', 'कह', 'कहत', 'गय', 'जह', 'तन', 'तर', 'दब', 'दर', 'धर', 'नस', 'नह', 'पहल', 'बन', 'बह', 'यत', 'यद', 'रख', 'रह', 'लक', 'वर', 'वग़', 'सकत', 'सबस', 'सभ', 'सर', 'ἀλλ']       
    final_stp_wrds = stop_words + new_stp_wrds
    stop_words = final_stp_wrds

    def remove(text):
        from gensim.parsing.preprocessing import strip_tags, strip_punctuation, strip_short
        from gensim.parsing.preprocessing import strip_multiple_whitespaces, strip_numeric, stem_text
        from gensim.parsing.preprocessing import strip_non_alphanum, remove_stopwords, preprocess_string
        CUSTOM_FILTERS = [lambda x: x.lower(), # lowercase
                            strip_multiple_whitespaces,
                            strip_non_alphanum,
                            strip_numeric,
                            remove_stopwords,
                            strip_short,
        #                   stem_text
                            ]
        text = text.lower()
        example_sent = preprocess_string(text, CUSTOM_FILTERS)
        print('done processing')
        filtered_sentence = [w for w in example_sent if not w in stop_words]

        return filtered_sentence
        
    def counter(text):
        from collections import Counter
        st = remove(text)
        print('done removing')
        counter_obj = Counter(st)
        return str(counter_obj.most_common(100)),str(counter_obj.most_common(1)[0][0])
        
    def get_topterms(post_ids):
        q_topterms = f'select terms from blogpost_terms where blogpost_id in ({post_ids})'
        topterms_result = query(conf, q_topterms)
        topterms_result
        
        term_dict = {}
        for i in range(len(topterms_result)):
            t = topterms_result[i][0].replace('),', '----').replace('(','').replace(']','').replace('[','').replace(')','').replace('\'','').split('----')
            for elem in t:
                if elem != 'BLANK':
                    key = elem.split(',')[0].replace('\'','').strip()
                    val = elem.split(',')[1].replace('\'','').strip()
                    if key in term_dict:
                        v = term_dict[key]
                        new_v = v + int(val)
                        term_dict[key] = new_v
                    else:
                        term_dict[key] = int(val)

        term_dict_sorted = {k: v for k, v in sorted(term_dict.items(), key=lambda item: item[1], reverse = True)}
        final_terms = {}
        top_100 = list(term_dict_sorted.keys())[0:100]
    #     print(top_100)
        for key in top_100:
            final_terms[key] = term_dict_sorted[key]
    #     print(term_dict.items())
        # for k, v in  sorted(x.items()
        return final_terms

    # conf = (ip, user_name, password, db)
    current_ = 5
    q_trackers = f"select * from trackers where tid = {tid}"
    tracker_result = query(conf, q_trackers)
    blogsite_ids = tracker_result[0][5].replace('blogsite_id in', '').strip()
    print('done getting tracker info')

    q_post = f"select blogpost_id, post from blogposts where blogsite_id in {blogsite_ids}"
    post_result = query(conf, q_post)
    print('done getting posts', len(post_result))

    q_total = f"select count(*) from blogposts"
    total_result = query(conf,q_total)[0][0]
    print(total_result)

    current_+=5
    getStatus(conf, tid, current_)

    post_ids_ = []
    posts = []
    
    for i in range(len(post_result)):
        post_ids_.append(post_result[i][0])
        posts.append(post_result[i][1])

        
    
    # dat = {}  
    # print(post_ids_)  
    # q_svd = f"select post_id, svd from cluster_svd where post_id in ({str(post_ids_).replace('[','').replace(']','')})"
    # svd_result = query(conf, q_svd)

# -------------------------------------------
    print('done getting posts')
    documents = list(map(str,posts))


    vectorizer = TfidfVectorizer(stop_words=stop_words)
    print('done getting vectorizer')
    current_+=10
    getStatus(conf, tid, current_)
    # for d in documents:
    #     if type(d) == int:
    #         print('found',d)
    vectorizer.fit(documents)

    current_+=10
    getStatus(conf, tid, current_ )
    print('done fitting data')
    X = vectorizer.transform(documents)

    from sklearn.decomposition import PCA, IncrementalPCA, TruncatedSVD
    data = TruncatedSVD(n_components=2, algorithm='arpack')
    new_data = data.fit_transform(X)

    current_+=10
    getStatus(conf, tid, current_)

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
#     print(post_id_svd)
# ---------------------------------------------

    print('data for model is ready...')

    model = KMeans(n_clusters = 10)
    model.fit(npa)
    centroids = model.cluster_centers_
    labels = model.labels_


# ----------------------------------------------
    print(len(post_ids_), len(posts), len(npa))
    # update_svd(npa, post_ids_, conf)
    
# ----------------------------------------------
    # print(len(post_ids_), len(posts), len(svd_result), len(data), len(npa))

    print('model and necessary parameters are ready')

    current_+=10
    getStatus(conf, tid, current_)
    # print()
    func = lambda x: str(x)
    new_list = list(map(func,post_ids_))

    df = pd.DataFrame()     
    df['post_id_incluster'] = new_list
    df['cluster'] = labels
    
#     print(df)

    import json
    test = df.groupby(['cluster'])['post_id_incluster'].apply(','.join)
    test_df_transposed = pd.DataFrame(test).transpose()
    test_df_transposed.insert(0, "cluster_id", tid, True)
    test_df_transposed.insert(1, "tid", tid, True)
    test_df_transposed.insert(2, "total", len(post_ids_), True)
    shape = test_df_transposed.shape

    print('data frame created')

    arr = []
    for i in range(10):
        idx = shape[1] + i
        labll = f"C{i+1}xy"
        arr.append(labll)
        cent = str(list(centroids[i]))
        test_df_transposed.insert(idx, labll, cent, True)

    arr2 = ["cluster_id","tid","total","cluster_1","cluster_2","cluster_3","cluster_4","cluster_5","cluster_6","cluster_7","cluster_8","cluster_9","cluster_10"]
    newarrs = arr2 + arr
    test_df_transposed.columns = newarrs
    # print(test_df_transposed)
    print('getting topterms')

    for i in range(10):
        dic = {}
        pids = test[i]
    #     s = list(posts)
    #     string_post = ' '.join(map(str, s))
        print(f'done joining cluster {i + 0}')
    #     terms, topterms = counter(string_post)
        dic['post_ids'] = pids
        dic['topterms'] = get_topterms(pids)
        
        test_df_transposed[f'cluster_{i + 1}'] = str(dic)

        current_+=5
        getStatus(conf, tid, current_)

    test_df_transposed['svd'] = str(post_id_svd)
    print(test_df_transposed)


    p = npa, post_ids_
    return test_df_transposed,p

def insert_single_cluster(tid, status, status_percentage, conf):
    start = datetime.now()
    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = "insert into clusters (cluster_id, tid, status, status_percentage) values (%s, %s, %s, %s)"
    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()
    # print((str(tid), str(result), str(query)))
    mycursor.execute(sql,(str(tid), str(tid), str(status), str(status_percentage)))   
    # result = mycursor.fetchall()
    mydb.commit()

    mycursor.close()
    mydb.close()

    end = datetime.now()
    print(f'it took {end - start}')

def insert_to_cluster(conf, data, tid):
    error = False
    import pymysql
    ip = conf[0]
    user_name = conf[1]
    password = conf[2]
    db = conf[3]

    engine = sqlalchemy.create_engine(f'mysql+pymysql://{user_name}:{password}@{ip}:3306/{db}')
    # Connect to the database
    
    connection = pymysql.connect(host=ip,
                            user=user_name,
                            password=password,
                            db=db)
    
    cursor=connection.cursor()

    cols = "`,`".join([str(i) for i in data.columns.tolist()])

    # Insert DataFrame recrds one by one.
    print('insertion')
    for i,row in data.iterrows():
        sql = "INSERT INTO `clusters` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        print('done inserting to clusters')
        try:
            cursor.execute(sql, tuple(row))
            connection.commit()
            connection.close()
        except Exception as e:
            print('originall error', e)
            if 'Duplicate entry' in str(e):
                print(e,'seun')
                error = True
            
    
    if error:
        connection.commit()
        print('commited')
        cursor.close()
        connection.close()
        
        print('here')
        engine.execute(f"delete from clusters where cluster_id = {tid}")
        # trans.commit()
        print('deleted')
        df = data
        df.to_sql("clusters",engine,if_exists='append',index=False)
        print('inserted')

        # the connection is not autocommitted by default, so we must commit to save our changes
        
        

def update_svd(param,conf):
    start = datetime.now()

    npa, post_ids = param

    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = "update cluster_svd set svd = %s where post_id = %s"
    print(sql)
    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()

    for svd, post_id in zip(npa, post_ids):
        s = str(svd).replace('[','').replace(']','').strip()
        mycursor.execute(sql,(str(s), str(post_id)))

    mydb.commit()
    print('done')

    

    mycursor.close()
    mydb.close()

    end = datetime.now()
    print(f'it took {end - start}')

def func(x):

    a = str(x).replace('[','').replace(']','')
    b = ' '.join(a.split()).split()

    x1 = '{0:.20f}'.format(float(b[0]))
    y1 = '{0:.20f}'.format(float(b[1]))
    return f'{x1} {y1}'

def upd(all_):
    posts = all_[1][1]
    svd = all_[1][0]
    new_svd = list(map(func, svd))
    new_posts = list(map(str,posts))
    # print(posts)
    print('svdddd', len(new_svd), len(new_posts), new_svd[0])
    # print(','.join(new_posts))
    try:
        # delete those rows that we are going to "upsert"
        engine.execute(f"delete from cluster_svd where post_id in ({','.join(new_posts)})")
        # trans.commit()
        df = pd.DataFrame({'post_id' : posts,'svd' : new_svd})
        df.to_sql("cluster_svd",engine,if_exists='append',index=False)

        # insert changed rows
        # x.to_sql('test_upsert', engine, if_exists='append', index=True)
    except Exception as e:
        print(e)

# RUN FOR ALL TRACKERS
# conf = getconf()
# for tracker_id in query(conf, "select tid from trackers where userid = 'nihal1' "):
#     tid = tracker_id[0]
#     try:
#         all_ = getClusterforall(tid, conf)
#         insert_to_cluster(conf,all_[0],tid)
#         getStatus(conf, tid, 100)
#         # print('updating svd')y
#         # upd(all_)
#     except Exception as e:
#         print(tid, e)

# TEST FOR ONE TRACKER
# conf = getconf()
# tid = 7
# all_ = getClusterforall(tid, conf)
# insert_to_cluster(conf,all_[0], tid)
# getStatus(conf, tid, 100)