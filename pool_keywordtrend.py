from pathos.multiprocessing import ProcessPool
import multiprocessing
import concurrent.futures
from tqdm import tqdm 
import re
from elasticsearch import Elasticsearch
from datetime import datetime
import mysql.connector
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

# import terms as t
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



# print('final_dic',final_dic)
# print('trend_dic',trend_dic)

# for t__ in tracker_ids:
#     print(t__[0])
#     trendforall(t__[0], conf)
# print(tracker_ids[0][0])

conf = getconf()
tracker_ids = query(conf, f'select tid from tracker_keyword where tid > 360')
terms_extract = []
for t__ in tracker_ids:
    terms_extract.append(t__[0])

if __name__ == '__main__':
    
    def processterms(tid):
        import re
        from pathos.multiprocessing import ProcessPool
        import concurrent.futures
        from tqdm import tqdm 
        
        from elasticsearch import Elasticsearch
        from datetime import datetime
        import mysql.connector 
    
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
        
        # import terms as t
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
        
        # import mysql.connector

        # 
        # start = datetime.now()

        def getblog_ids(tid,conf):
            q__ = query(conf, f'select query from trackers where tid = {tid}')
            return cleanbrackets(q__[0][0].replace('blogsite_id in (', '').replace(')', ''))

        def cleanbrackets(s):
            s_new = ''

            if s == '':
                return s_new
            if s[-1] == ',':
                s_new = s[0:-1]
            else:
                s_new = s

            return s_new



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
            # import re
            ids = getblog_ids(tid,conf).replace(' ','')
            stored_terms = query(conf, f'select terms from tracker_keyword where tid = {tid}')
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

            # all_data = []
            try:
                all_data = getTermQuery(ids, terms.join(list(terms_dic.keys())).replace(' ',''))
            except:
                pass

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
                # start = datetime.now()
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

                # end = datetime.now()
                # print(f'it took {end - start}')

            update_terms(tid, str(final_dic), str(trend_dic), conf)
        
        conf = getconf()
        trendforall(tid, conf)
    parallel = True

    if parallel:
        # cores = (multiprocessing.cpu_count()*2) 
        # pool = ProcessPool(cores)
        pool = ProcessPool(5)
        # for _ in pool.uimap(process_blogs, blogposts):
        #     pass
        
        for _ in tqdm(pool.uimap(processterms, terms_extract), ascii=True, total=len(terms_extract)):
            # break
            pass
    else:
        for line in tqdm(terms_extract):
            processterms(line)
            # break

