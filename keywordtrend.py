from elasticsearch import Elasticsearch
from datetime import datetime
import re
import terms as t
import mysql.connector

# 
# start = datetime.now()

def getblog_ids(tid,conf):
    query = t.query(conf, f'select query from trackers where tid = {tid}')
    return t.cleanbrackets(query[0][0].replace('blogsite_id in (', '').replace(')', ''))




def getTermQuery(ids, terms):
    client = Elasticsearch()
    response = client.search(
        index="blogposts",
        scroll='1m',
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
        all_data.append(data)

    
    while len(response['hits']['hits']):
        response = client.scroll(
            scroll_id=scroll_id,
            scroll='2s'
        )

        scroll_id = response['_scroll_id']

        for data in response['hits']['hits']:
            all_data.append(data)

    return all_data



def trendforall(tid, conf):
    ids = getblog_ids(tid,conf).replace(' ','')
    stored_terms = t.query(conf, f'select terms from tracker_keyword where tid = {tid}')
    print('done getting terms stored with ids ', ids)
    # print('\"nato\", \"security\"')
    # print('nato, security'.split(','))
    # print(dict(stored_terms[0][0])['nato'])
    terms_dic = {}
    for d in stored_terms[0][0].replace('{','').replace('}','').replace('\'', '').replace('\"', '').strip().split(','):
        split = d.split(':')
        terms_dic[split[0].replace('\'','').strip()] = split[1]

    print('done cleaning',list(terms_dic.keys()))
    terms = ','
    print('joined',terms.join(list(terms_dic.keys())))

    all_data = getTermQuery(ids, terms.join(list(terms_dic.keys())).replace(' ',''))
    print('done getting data of length', len(all_data))

    def generateTrend(all_data, term):
        dic = {}
        for i in range(len(all_data)):
            count = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape(term), all_data[i]['_source']['post'].lower()))
            date = all_data[i]['_source']['date'].split('-')[0]

            if date not in dic:
                dic[date] = count
            else:
                new_v = count
                old_v = dic[date]
                dic[date] = new_v + old_v
    # count 
        return dic

    print('computing....')
    final_dic = {}
    trend_dic = {}

    for key in terms_dic:
        trend_data = generateTrend(all_data,key)
        trend_dic[key] = trend_data
        final_dic[key] = sum(trend_data.values())


    def update_terms(tid, terms, result, conf):
        start = datetime.now()
        config = conf 
        ip = config[0]
        user_name = config[1]
        password = config[2]
        db = config[3]

        sql = "update tracker_keyword set terms = %s, keyword_trend = %s where tid = %s"
        print(sql)
        mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
        mycursor = mydb.cursor()
        mycursor.execute(sql,(terms, result, tid))
        # result = mycursor.fetchall()
        mydb.commit()

        mycursor.close()
        mydb.close()

        end = datetime.now()
        print(f'it took {end - start}')

    # update_terms(tid, str(final_dic), str(trend_dic), conf)

# print('final_dic',final_dic)
# print('trend_dic',trend_dic)

# tracker_ids = t.query(conf, f'select tid from tracker_keyword where tid > 106')
# for t__ in tracker_ids:
#     print(t__[0])
#     trendforall(t__[0], conf)
# print(tracker_ids[0][0])
# conf = t.getconf()
# trendforall(410, conf)

# end  = datetime.now()
# dur = end - start

# print(f'it took {dur}')
# print(f'it took {dur.days, dur.seconds//3600, (dur.seconds//60)%60}')