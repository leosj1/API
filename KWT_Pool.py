from elasticsearch import Elasticsearch
from datetime import datetime
import re
# import terms as t
import mysql.connector

# 
# start = datetime.now()
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

def cleanbrackets(s):
    s_new = ''
    
    if s == '':
        return s_new
    if s[-1] == ',':
        s_new = s[0:-1]
    else:
        s_new = s
    
    return s_new

def getblog_ids(tid,conf):
    query_ = query(conf, f'select query from trackers where tid = {tid}')
    return cleanbrackets(query_[0][0].replace('blogsite_id in (', '').replace(')', ''))




def getTermQuery(ids, terms):
    client = Elasticsearch()
    response = client.search(
        index="blogposts",
        scroll='2m',
        body={
            "size":1000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "post":terms.split(',')
                            }
                        },
                        {
                            "terms": {
                                "blogsite_id": ids.split(','),
                               
                            }
                        }
                        ,
                        # {
                        #     "range": {
                        #         "date": {
                        #             "from": "2000-01-01",
                        #             "to": "2020-01-01",
                        #         }
                        #     }
                        # }
                    ]
                }
            }
        }
        
    )
  
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']

    all_data = []
    for data in hits:
        all_data.append(data['_source'])
#         print('done', len(data['_source']))

    
    while len(response['hits']['hits']):
        response = client.scroll(
            scroll_id=scroll_id,
            scroll='2s'
        )

        scroll_id = response['_scroll_id']

        for data in response['hits']['hits']:
            all_data.append(data['_source'])
#             print('done', len(data['_source']))

    return all_data



def trendforall(tid, conf):
    ids = getblog_ids(tid,conf).replace(' ','')
    stored_terms = query(conf, f'select terms from tracker_keyword where tid = {tid}')
#     print('done getting terms stored with ids ', ids)
    # print('\"nato\", \"security\"')
    # print('nato, security'.split(','))
    # print(dict(stored_terms[0][0])['nato'])
    terms_dic = {}
    for d in stored_terms[0][0].replace('{','').replace('}','').replace('\'', '').replace('\"', '').strip().split(','):
        split = d.split(':')
        terms_dic[split[0].replace('\'','').strip()] = split[1]

#     print('done cleaning',list(terms_dic.keys()))
    terms = ','
#     print('joined',terms.join(list(terms_dic.keys())))

    all_data = getTermQuery(ids, terms.join(list(terms_dic.keys())).replace(' ',''))
    
    print('done getting data of length', len(all_data))
    return all_data,terms_dic
    

import time

conf = getconf()
all_ = trendforall(410, conf)
all_data = all_[0]


import pandas as pd
df = pd.DataFrame(all_data)
df['date'] = pd.to_datetime(df['date'], errors='coerce')


def KWT(param):
    df,term = param
    grouped_by_year = df.groupby(df.date.dt.year)
    years = sorted(df.date.dt.year.unique())
    year_start = years[0]
    year_end = years[-1]

    dict_year = {}
    for year in years:
        df_year = grouped_by_year.get_group(year)
        posts = ' '.join(df_year['post'].tolist())
#         print('done getting post')
        count = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape(term), posts.lower()))
#         print('done getting count')
        dict_year[f'{year}'] = count
    
    return dict_year

terms_dic = all_[1]
# start = time.perf_counter()
# for key in terms_dic.keys():
#     p = df,key
#     print(KWT(p))

# stop = time.perf_counter()
# print(f'it took {stop - start}')


# term_test = list(terms_dic.keys())[0]
if __name__ == "__main__":
    start = time.perf_counter()
    data = []
    for key in terms_dic.keys():
        data.append((df,key))
    from multiprocessing import Pool
    # 
    pool = Pool(len(data))
    # p = (df, term_test)
    response = pool.map(KWT, data)

    # final_data = []
    for x in response:
        print(x)


#     # print(KWT(p))
#     stop = time.perf_counter()
#     print(f'it took {stop - start}')