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
def getTopKWS(ids):
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
        {'host': '144.167.35.50'},

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
def get_count(ids):
    client = Elasticsearch([
        {'host': '144.167.35.50'},

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
def query_db(query):
    # query,engine = en
    engine = sqlalchemy.create_engine(
        'mysql+pymysql://wale:abcd1234!@144.167.35.73:3306/blogtrackers')
    df = pd.read_sql_query(query, engine)
    return df


# GROUPED BY YEAR
def countin(param):
    grouped_by_year, term,year = param
    tt = list(grouped_by_year.get_group(year)['terms'])
    from collections import Counter
    count = 0
    for t in tt:
        found = json.loads(t)
        if term in found:
            count += int(found[term])
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
def getconf():
    config_file = open('C:\\blogtrackers.config','r')
    data = config_file.readlines()
    # print(data)
    # return "144.167.35.73", "wale", "abcd1234!", "blogtrackers"
    ip = ''
    user_name = ''
    password = ''
    db = 'blogtrackers'
    for elem in data:
        if 'dbConnection' in elem:
            connection_url = elem.split('##')[1]
            ip_and_port = re.search('mysql://(.*)/blogtrackers', connection_url)
            ip_and_port_ = ip_and_port.group(1)
            ip = ip_and_port_.split(':')[0].strip()
        if 'dbUserName' in elem:
            user_name = elem.split('##')[1].strip()
        if 'dbPassword' in elem:
            password = elem.split('##')[1].strip()
    
    return (ip, user_name, password, db)    

# UPDATE STATUS FIELD IN TRACKER_KEYWORD TABLE
def getStatus(conf, tid, total, current):
    status = (current/total) * 100
    # result = status, tid

    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()

    if int(status) != 100:
        sql = "update tracker_keyword set status = %s, status_percentage = %s where tid = %s"
        mycursor.execute(sql,(str(0), str(status), str(tid)))
    else:
        sql = "update tracker_keyword set status = %s, status_percentage = %s where tid = %s"
        mycursor.execute(sql,(str(1), str(status), str(tid)))

    mydb.commit()
    mycursor.close()
    mydb.close()


def loop(param_initial):
    years, param = param_initial
    year_data={}

    for y in years:
        new_param = param[0], param[1], y
        year_data[f'{y}'] = countin(new_param)

    return year_data

    

def testingKWT(tid):
    data = []

    conf = getconf()
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

    blog_ids = blogsite_ids
    # blog_ids = "153,148,259,114,32,123,37,155,46,3,170,154,72,38,224,247,157,128,61,112,140,144,116,125,193,9,173,89,68,87,249,250,263,98,69,152,62,78,117,83,73,264,135,184,120,138,133,100,93,143,77,233,139,132,146,147,149,150,43,242,47,111,101,86,81,118,194,45,106,121,129,49,237,66,179,91,176,124,167,84,174,215,141,119,236,252,185,20,162,130,22,76,235,178,232,85,79,26,109,80,131,253,105,151,142,137,115,52,53,65,94,92,96,136,191,27,29,107,63,99,57,190,169,216,122,126,36,127,134,108,54"
    q = f'select terms,date from blogpost_terms_api where blogsiteid in {blog_ids}'
    q_count = f'select count(*) as total from blogpost_terms_api where blogsiteid in {blog_ids}'
    c = int(query_db(q_count)['total'][0])
    total =  51
    count = 0

    getStatus(conf, tid, total, count)

    result = query_db(q)
    count +=1

    getStatus(conf, tid, total, count)

    print(result.shape)

    df = result
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    grouped_by_year = df.groupby(df.date.dt.year)
    years = sorted(df.date.dt.year.unique())

    terms_result = getTopKWS(blog_ids)

    final_data = {}
    total_data = {}
    
    data_ = []
    terms_data = []
    
    for i in range(len(terms_result)):
        term = terms_result[i][0]
        terms_data.append(term)

        param = grouped_by_year, term.replace(' ','')
        param_initial = years, param

        res = loop(param_initial)
        final_data[term] = res
        total_data[term] = sum(list(res.values()))
        # data_.append(param_initial)

    # if __name__ == '__main__':
        # cores = (multiprocessing.cpu_count()) 
        # pool = Pool(int(cores/3))
        # print('cores', cores)
        # pool = Pool(len(data_))
        # response = pool.map(loop, data_)

        # final_data = []
        # for x,term in zip(response,terms_data):
            # final_data[term] = x

    # print(len(data_), len(terms_data))

    # with ThreadPoolExecutor() as executor:
    #     results = executor.map(loop, data_)
    
    # for result,term in zip(results,terms_data):
    #     final_data[term] = result

        # final_data[term] = year_data
        # print(result)
        # final_data[term] = loop(param_initial)

        count+=1

        getStatus(conf, tid, total, count)
        # print(count)
        # break
    
    # print('total_data',total_data)
    # print('final_data',final_data)
    # stop = time.perf_counter()
    # print(stop - start)    

    return final_data,total_data


start = time.perf_counter()
# f = testingKWT(453)
# print(f[0],f[1])
# import terms as ttttt

# ttttt.update_terms(tid, t.sort_dict(f[1],50), blod_ids,f[0] , conf)
# # print("sorted-----",ttttt.sort_dict(f[1],100))
# # print(years_dict)
# stop = time.perf_counter()
# print('here--',stop - start)

# conf = getconf()
# for tracker_id in query(conf, "select tid,query from trackers where userid = 'nihal1'"):
#     tid = tracker_id[0]
#     blod_ids = tracker_id[1]
#     # print(tid,query)
#     try:
#         f = testingKWT(tid)
#         ttttt.update_terms(tid, ttttt.sort_dict(f[1],50), blod_ids,f[0] , conf)
#     except Exception as e:
#         print(tid, e)

# print(get_count(blog_ids))

# if __name__ == '__main__':
#     step = 24618

#     start = time.perf_counter()

#     data = []
#     # blog_ids = "808,62,88,239,641,182,148,109,750,193,1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399"
#     blog_ids = "153,148,259,114,32,123,37,155,46,3,170,154,72,38,224,247,157,128,61,112,140,144,116,125,193,9,173,89,68,87,249,250,263,98,69,152,62,78,117,83,73,264,135,184,120,138,133,100,93,143,77,233,139,132,146,147,149,150,43,242,47,111,101,86,81,118,194,45,106,121,129,49,237,66,179,91,176,124,167,84,174,215,141,119,236,252,185,20,162,130,22,76,235,178,232,85,79,26,109,80,131,253,105,151,142,137,115,52,53,65,94,92,96,136,191,27,29,107,63,99,57,190,169,216,122,126,36,127,134,108,54"
    
#     max_ = get_count(blog_ids)

#     for i in range(0, max_, step):
#         q = f'select * from blogpost_terms_api where blogsiteid in ({blog_ids}) order by blogpost_id LIMIT {i} , {step}'
#         data.append(q)

#     print('started processes ',len(data))
#     processes = []
#     for d in data:
#         # print(query_db(d))
#         # res = multiprocessing.Queue()
#         proc = Process(target=query_db, args=(d,))
#         processes.append(proc)
#         proc.start()

#     for p_ in processes:
#         p_.join()

#     result = [pp for pp in processes]
#     print(result)

    # pool = Pool(len(data))
    # response = pool.map(query_db, data)

    # final_data = []
    # for x in response:
    #     final_data.append(x)

    # print('done with pool')
    # df_concat = pd.concat(final_data)
    # print(df_concat.shape)

    # print(df_concat[df_concat['blogpost_id'].duplicated(keep=False)])

    # stop = time.perf_counter()
    # print(stop - start)

