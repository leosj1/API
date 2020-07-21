import pandas as pd
import sqlalchemy
import time
from multiprocessing import Process, Pool
import multiprocessing
from elasticsearch import Elasticsearch
from datetime import datetime
import re
import json
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

# FUNCTION TO GET TOP 100 TERMS BY BLOG_IDS INDEXED ON ES
def getTopKWS(ids, ip):
    body = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "blogsite_id": ids.replace(' ','').replace('(','').replace(')','').split(','),
                            "boost": 1
                        }
                    }
                ]
            }
        },
        "aggs": {
            "frequent_words": {
                "terms": {
                    "field": "post",
                    "size": 50
                }
            }
        }
    }
    # print('body--',body)
    client = Elasticsearch([
        {'host': ip},

    ])
    response = client.search(
        index="blogposts",
    
        body= body

    )

    data = response['aggregations']['frequent_words']['buckets']
    func = lambda x:(x['key'],x['doc_count'])
    d = list(map(func, data))
    return d

# TO COUNT CONTENT 
def get_count(ids, ip):
    client = Elasticsearch([
        {'host': ip},

    ])
    response = client.count(
        index="blogposts",

        body={

            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "blogsite_id": ids.replace(' ','').split(','),
                                "boost": 1
                            }
                        }
                    ]
                }
            }
        }
    )
    return int(response['count'])

# QUERY DB AND RETURN DATAFRAME
def query_db(query, conf):
    # query,engine = en
    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    engine = sqlalchemy.create_engine(
        f'mysql+pymysql://{user_name}:{password}@{ip}:3306/{db}')
    df = pd.read_sql_query(query, engine)
    return df


# GROUPED BY YEAR
def countin(param):
    grouped_by_year, term,year = param
    tt = list(grouped_by_year.get_group(year)['terms'])
    from collections import Counter
    count = 0

    # c = sum([int(found[term]) for x[] in ])
    for t in tt:
        found = json.loads(t)
        if term in found:
            count += int(found[term])

    # print('done counting')
    return count

# RAW SQL QUERY FUNCTION
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

# GET CONFIG FROM CONFIG FILE
def getconf2():
    return ('cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers') 
    # return "144.167.35.73", "wale", "abcd1234!", "blogtrackers"

# def getconf2():
#     config_file = open('C:\\blogtrackers.config','r')
#     data = config_file.readlines()
#     # print(data)
#     # return "144.167.35.73", "wale", "abcd1234!", "blogtrackers"
#     ip = ''
#     user_name = ''
#     password = ''
#     db = 'blogtrackers'
#     for elem in data:
#         if 'dbConnection' in elem:
#             connection_url = elem.split('##')[1]
#             ip_and_port = re.search('mysql://(.*)/blogtrackers', connection_url)
#             ip_and_port_ = ip_and_port.group(1)
#             ip = ip_and_port_.split(':')[0].strip()
#         if 'dbUserName' in elem:
#             user_name = elem.split('##')[1].strip()
#         if 'dbPassword' in elem:
#             password = elem.split('##')[1].strip()

#     # host='cosmos-1.host.ualr.edu',user='ukraine_user', password='summer2014',db='blogtrackers',charset='utf8mb4',use_unicode=True,cursorclass=pymysql.cursors.DictCursor)
#     return ('cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers') 
    # return (ip, user_name, password, db)    

# UPDATE STATUS FIELD IN TRACKER_KEYWORD TABLE
def getStatus(conf, tid, total, current):
    # status = (current/total) * 100
    status = current + 2
    # result = status, tid

    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()
    # print('stat',status)
    if int(status) != 100:
        sql = "update tracker_keyword set status = %s, status_percentage = %s where tid = %s"
        mycursor.execute(sql,(str(0), str(status), str(tid)))
    else:
        sql = "update tracker_keyword set status = %s, status_percentage = %s where tid = %s"
        mycursor.execute(sql,(str(1), str(status), str(tid)))

    mydb.commit()
    mycursor.close()
    mydb.close()

# UPDATE tracker_keyword TABLE FUNCTION
def update_terms(tid, result, query, keyword_trend, conf):
    # start = datetime.now()
    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = "update tracker_keyword set terms = %s, query = %s, keyword_trend = %s where tid = %s"
    # print(sql)
    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()
    mycursor.execute(sql,(str(result), str(query), str(keyword_trend), str(tid)))
    # result = mycursor.fetchall()
    mydb.commit()

    mycursor.close()
    mydb.close()

# SORT DICTIONARY
def sort_dict(dic, limit):
    term_dict_sorted = {k: int(v) for k, v in sorted(dic.items(), key=lambda item: item[1], reverse = True)}
    final_terms = {}
    top_n = list(term_dict_sorted.keys())[0:limit]
    for key in top_n:
        occur = term_dict_sorted[key]
        if int(occur) != 0:
            final_terms[key] =  occur

    return final_terms


def loop(PARAMS):
    param_initial,tid, conf, blog_ids = PARAMS

    years, param = param_initial
    year_data={}
    final_data = {}
    total_data = {}

    for y in years:
        new_param = param[0], param[1], y
        # grouped_by_year, term,year = new_param
        year_data[f'{y}'] = countin(new_param)

    final_data[param[1]] = year_data
    total_data[param[1]] = sum(list(year_data.values()))

    # check if key already exists in DB
    q_checked = f'select terms, keyword_trend from tracker_keyword where tid = {tid}'
    checked_result = query(conf, q_checked)

    if checked_result[0][0]:
        replaced_terms = checked_result[0][0].replace("\'", "\"")
    else:
        replaced_terms = "{}"

    if checked_result[0][1]:
        replaced_kwt = checked_result[0][1].replace("\'", "\"")
    else:
        replaced_kwt = "{}"
    
    terms_checked = json.loads(replaced_terms)
    kwt_checked = json.loads(replaced_kwt)

    term = param[1]
    terms_checked[term] = total_data[term]
    kwt_checked[term] = final_data[term]



    update_terms(tid, terms_checked, f'blogsite_id in {blog_ids}',kwt_checked , conf)

    q_count = f'select status_percentage from tracker_keyword where tid = {tid}'
    q_stat_res = query(conf, q_count)
    q_stat = q_stat_res[0][0]
    count = int(q_stat) 

    # print('count', q_stat)
    # print('add', int(q_stat) + 2)


    getStatus(conf, tid, 0, count)

def contains_only_integers(tup):
    try:
        tup = tuple(map(int, tup[1:-1].split(',')))
        if tup:
            return True
        else:
            return False
        # for item in tup[1:]:
        #     if not isinstance(item, int):
        #         return False
        
    except ValueError:
        return False
    
    

def testingKWT(tid, ip):
    # print('here1')
    conf = getconf2()
    q_trackers = f"select * from trackers where tid = {tid}"
    tracker_result = query(conf, q_trackers)
    if len(tracker_result) > 0:
        blogsite_ids = tracker_result[0][5].replace('blogsite_id in', '').strip()
        s_new = ''
        s = blogsite_ids
        if s[-1] == ')':
            if s[-2] == ',':
                s_new = s[0:-2] + ')'
                blogsite_ids = s_new
    else:
        blogsite_ids = '()'
    # print('here2')
    blog_ids = blogsite_ids
    if not contains_only_integers(blog_ids):
        blog_ids = '()'
        return None
    # blog_ids = "153,148,259,114,32,123,37,155,46,3,170,154,72,38,224,247,157,128,61,112,140,144,116,125,193,9,173,89,68,87,249,250,263,98,69,152,62,78,117,83,73,264,135,184,120,138,133,100,93,143,77,233,139,132,146,147,149,150,43,242,47,111,101,86,81,118,194,45,106,121,129,49,237,66,179,91,176,124,167,84,174,215,141,119,236,252,185,20,162,130,22,76,235,178,232,85,79,26,109,80,131,253,105,151,142,137,115,52,53,65,94,92,96,136,191,27,29,107,63,99,57,190,169,216,122,126,36,127,134,108,54"
    q = f'select terms,date from blogpost_terms_api where blogsiteid in {blog_ids}'
    # print('here3')
    total =  51
    count = 0
    # print('here4')
    getStatus(conf, tid, total, 0)

    start1 = time.perf_counter()
    result = query_db(q, conf)
    end1 = time.perf_counter()
    # print('sql took ', end1 - start1)
    # count =5

    getStatus(conf, tid, total, count)

    # print(result.shape)
    # print('here5')
    df = pd.DataFrame()
    df = result
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df[df['date'].notna()]
    grouped_by_year = df.groupby(df.date.dt.year)
    years = sorted(df.date.dt.year.unique())

    # print('done grouping')

    terms_result = getTopKWS(blog_ids, ip)

    # print('KWS DONE')

    data_ = []

    for i in range(len(terms_result)):
        term = terms_result[i][0]
        # terms_data.append(term)

        param = grouped_by_year, term.replace(' ','')
        param_initial = years, param

        PARAMS = param_initial,tid, conf, blog_ids

        data_.append(PARAMS)

    # if __name__ == '__main__':
    cores = (multiprocessing.cpu_count()) 
    pool = Pool(int(12))
    # print('cores', cores)
    # pool = Pool(len(data_))
    pool.map(loop, data_)

    q_checked = f'select terms,query,keyword_trend from tracker_keyword where tid = {tid}'
    checked_result = query(conf, q_checked)
    replaced_terms = checked_result[0][0].replace("\'", "\"")
    kwt_terms = checked_result[0][2].replace("\'", "\"")

    terms_checked = sort_dict(json.loads(replaced_terms),50)
    kwt_checked = json.loads(kwt_terms)
    blog_ids = checked_result[0][1].replace("\'", "\"")

    update_terms(tid, terms_checked,  blog_ids ,kwt_checked , conf)
    getStatus(conf, tid, 0, 98)


      

