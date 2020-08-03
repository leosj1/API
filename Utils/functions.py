from gensim.parsing.preprocessing import strip_tags, strip_punctuation, strip_short
from gensim.parsing.preprocessing import strip_multiple_whitespaces, strip_numeric, stem_text
from gensim.parsing.preprocessing import strip_non_alphanum, remove_stopwords, preprocess_string
from elasticsearch import Elasticsearch
import json
from pathos.multiprocessing import freeze_support, ProcessPool
from tqdm import tqdm
import sys

from Utils.sql import SqlFuncs

def getconf2():
    return ('cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers')
    # return "144.167.35.73", "wale", "abcd1234!", "blogtrackers"

def get_stop_words():
    stop_words = []
    with open("C:\\API\\stopwords.txt", "r", encoding="utf-8") as f:
        for line in f:
            stop_words.append(str(line.strip()))
            
    new_stp_wrds = ['00','abbã³l', 'acaba', 'acerca', 'aderton', 'ahimã', 'ain', 'akã', 'alapjã', 'alors', 'alã', 'alã³l', 'alã³la', 'alã³lad', 'alã³lam', 'alã³latok', 'alã³luk', 'alã³lunk', 'amã', 'annã', 'appendix', 'arrã³l', 'attã³l', 'azokbã³l', 'azokkã', 'azoknã', 'azokrã³l', 'azoktã³l', 'azokã', 'aztã', 'azzã', 'azã', 'ba', 'bahasa', 'bb', 'bban', 'bbi', 'bbszã', 'belã', 'belã¼l', 'belå', 'bennã¼k', 'bennã¼nk', 'bã', 'bãºcsãº', 'cioã', 'cittã', 'ciã²', 'conjunctions', 'cosã', 'couldn', 'csupã', 'daren', 'didn', 'dik', 'diket', 'doesn', 'don', 'dovrã', 'ebbå', 'effects', 'egyedã¼l', 'egyelå', 'egymã', 'egyã', 'egyã¼tt', 'egã', 'ek', 'ellenã', 'elså', 'elã', 'elå', 'ennã', 'enyã', 'ernst', 'errå', 'ettå', 'ezekbå', 'ezekkã', 'ezeknã', 'ezekrå', 'ezektå', 'ezekã', 'ezentãºl', 'ezutã', 'ezzã', 'ezã', 'felã', 'forsûke', 'fã', 'fûr', 'fûrst', 'ged', 'gen', 'gis', 'giã', 'gjûre', 'gre', 'gtã', 'gy', 'gyet', 'gã', 'gã³ta', 'gã¼l', 'gã¼le', 'gã¼led', 'gã¼lem', 'gã¼letek', 'gã¼lã¼k', 'gã¼lã¼nk', 'hadn', 'hallã³', 'hasn', 'haven', 'herse', 'himse', 'hiã', 'hozzã', 'hurrã', 'hã', 'hãºsz', 'idã', 'ig', 'igazã', 'immã', 'indonesia', 'inkã', 'insermi', 'ismã', 'isn', 'juk', 'jã', 'jã³', 'jã³l', 'jã³lesik', 'jã³val', 'jã¼k', 'kbe', 'kben', 'kbå', 'ket', 'kettå', 'kevã', 'khã', 'kibå', 'kikbå', 'kikkã', 'kiknã', 'kikrå', 'kiktå', 'kikã', 'kinã', 'kirå', 'kitå', 'kivã', 'kiã', 'kkel', 'knek', 'knã', 'korã', 'kre', 'krå', 'ktå', 'kã', 'kã¼lã', 'lad', 'lam', 'latok', 'ldã', 'led', 'leg', 'legalã', 'lehetå', 'lem', 'lennã', 'leszã¼nk', 'letek', 'lettã¼nk', 'ljen', 'lkã¼l', 'll', 'lnak', 'ltal', 'ltalã', 'luk', 'lunk', 'lã', 'lã¼k', 'lã¼nk', 'magã', 'manapsã', 'mayn', 'megcsinã', 'mellettã¼k', 'mellettã¼nk', 'mellã', 'mellå', 'mibå', 'mightn', 'mikbå', 'mikkã', 'miknã', 'mikrå', 'miktå', 'mikã', 'mindenã¼tt', 'minã', 'mirå', 'mitå', 'mivã', 'miã', 'modal', 'mostanã', 'mustn', 'myse', 'mã', 'mãºltkor', 'mãºlva', 'må', 'måte', 'nak', 'nbe', 'nben', 'nbã', 'nbå', 'needn', 'nek', 'nekã¼nk', 'nemrã', 'nhetå', 'nhã', 'nk', 'nnek', 'nnel', 'nnã', 'nre', 'nrå', 'nt', 'ntå', 'nyleg', 'nyszor', 'nã', 'nå', 'når', 'også', 'ordnung', 'oughtn', 'particles', 'pen', 'perchã', 'perciã²', 'perã²', 'pest', 'piã¹', 'puã²', 'pã', 'quelqu', 'qué', 'ra', 'rcsak', 'rem', 'retrieval', 'rlek', 'rmat', 'rmilyen', 'rom', 'rt', 'rte', 'rted', 'rtem', 'rtetek', 'rtã¼k', 'rtã¼nk', 'rã', 'rã³la', 'rã³lad', 'rã³lam', 'rã³latok', 'rã³luk', 'rã³lunk', 'rã¼l', 'sarã', 'schluss', 'semmisã', 'shan', 'shouldn', 'sik', 'sikat', 'snap', 'sodik', 'sodszor', 'sokat', 'sokã', 'sorban', 'sorã', 'sra', 'st', 'stb', 'stemming', 'study', 'sz', 'szen', 'szerintã¼k', 'szerintã¼nk', 'szã', 'sã', 'talã', 'ted', 'tegnapelå', 'tehã', 'tek', 'tessã', 'tha', 'tizenhã', 'tizenkettå', 'tizenkã', 'tizennã', 'tizenã', 'tok', 'tovã', 'tszer', 'tt', 'tte', 'tted', 'ttem', 'ttetek', 'ttã¼k', 'ttã¼nk', 'tulsã³', 'tven', 'tã', 'tãºl', 'tå', 'ul', 'utoljã', 'utolsã³', 'utã', 'vben', 'vek', 'velã¼k', 'velã¼nk', 'verbs', 'ves', 'vesen', 'veskedjã', 'viszlã', 'viszontlã', 'volnã', 'vvel', 'vã', 'vå', 'vöre', 'vört', 'wahr', 'wasn', 'weren', 'won', 'wouldn', 'zadik', 'zat', 'zben', 'zel', 'zepesen', 'zepã', 'zã', 'zã¼l', 'zå', 'ã³ta', 'ãºgy', 'ãºgyis', 'ãºgynevezett', 'ãºjra', 'ãºr', 'ð¾da', 'γα', 'البت', 'بالای', 'برابر', 'برای', 'بیرون', 'تول', 'توی', 'تی', 'جلوی', 'حدود', 'خارج', 'دنبال', 'روی', 'زیر', 'سری', 'سمت', 'سوی', 'طبق', 'عقب', 'عل', 'عنوان', 'قصد', 'لطفا', 'مد', 'نزد', 'نزدیک', 'وسط', 'پاعین', 'کنار', 'अपन', 'अभ', 'इत', 'इनक', 'इसक', 'इसम', 'उनक', 'उसक', 'एव', 'ऐस', 'करत', 'करन', 'कह', 'कहत', 'गय', 'जह', 'तन', 'तर', 'दब', 'दर', 'धर', 'नस', 'नह', 'पहल', 'बन', 'बह', 'यत', 'यद', 'रख', 'रह', 'लक', 'वर', 'वग़', 'सकत', 'सबस', 'सभ', 'सर', 'ἀλλ']       
    final_stp_wrds = stop_words + new_stp_wrds
    stop_words = final_stp_wrds
    return stop_words

def clean_text(text):
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
    stop_words = get_stop_words()
    filtered_sentence = [w for w in example_sent if not w in stop_words]

    return filtered_sentence


# FUNCTION TO GET TOP 100 TERMS BY BLOG_IDS INDEXED ON ES
def getTopKWS(ids, ip):
    body = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "blogsite_id": ids.replace(' ', '').replace('(', '').replace(')', '').split(','),
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
                    "size": 100
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
        body=body,
        request_timeout=30

    )

    data = response['aggregations']['frequent_words']['buckets']
    def func(x): return (x['key'], x['doc_count'])
    d = list(map(func, data))
    return d

def updateStatus(status, tid):
    conn = getconf2()
    s = SqlFuncs(conn)

    if int(status) != 100:
        data = 0, status, tid
        s.update_insert("update tracker_keyword set status = %s, status_percentage = %s where tid = %s", data, conn)
    else:
        data = status, 1, tid
        s.update_insert("update tracker_keyword set status = %s, status_percentage = %s where tid = %s", data, conn)

def single_process(parameters):
    from Utils.functions import clean_text, getconf2
    from Utils.sql import SqlFuncs
    import pandas as pd
    import ast

    term, blog_ids, tid = parameters
    # start = time.time()
    # print('here-2')
    
    conf = getconf2()
    s = SqlFuncs(conf)

    # Get blogsites in tracker
    connection = s.get_connection(conf)
    with connection.cursor() as cursor:
        cursor.execute(f"""
        select * from 
        (SELECT date, post,title, blogsite_id, blogpost_id,
        ROUND ((LENGTH(lower(post)) - LENGTH(REPLACE (lower(post), "{term}", ""))) / LENGTH("{term}")) AS count 
        from (select * from blogposts where match (title, post) against ('{term}' IN BOOLEAN MODE)) bp) a where blogsite_id in ({blog_ids})
        """)

        final_data = {}
        total_data = {}

        records = cursor.fetchall()

        if records:
            df = pd.DataFrame(records)
            df['count'] = df['count'].map(int)
            year_count = df.groupby(df.date.dt.year)['count'].sum().to_dict()

            final_data[term] = year_count
            total_data[term] = sum(list(year_count.values()))

            # check if key already exists in DB
            cursor.execute(f'select terms, keyword_trend from tracker_keyword where tid = {tid}')
            checked_result = cursor.fetchall()

            if checked_result:
                terms_checked = ast.literal_eval(checked_result[0]['terms']) if 'terms' in checked_result[0] else None
                kwt_checked = ast.literal_eval(checked_result[0]['keyword_trend']) if 'keyword_trend' in checked_result[0] else None
                
            else:
                terms_checked = json.loads("{}")
                kwt_checked = json.loads("{}")

            terms_checked[term] = total_data[term]
            kwt_checked[term] = final_data[term]

            # Insert if not yet created
            conn = getconf2()
            data = tid, str(terms_checked), f'blogsite_id in ({blog_ids})', str(kwt_checked)
            s.update_insert("insert into tracker_keyword (tid, terms, query, keyword_trend) values (%s, %s, %s, %s)", data, conn)
            

            # Update if already created
            data = str(terms_checked), f'blogsite_id in ({blog_ids})',str(kwt_checked), tid
            s.update_insert("update tracker_keyword set terms = %s, query = %s, keyword_trend = %s where tid = %s", data, conn)

    connection.close()

def testingKWT(tid, ip, parallel, update__status, num_processes):
    conf = getconf2()
    s = SqlFuncs(conf)

    # Get blogsites in tracker
    connection = s.get_connection(conf)
    with connection.cursor() as cursor:
        cursor.execute(f"""select * from trackers where tid = {tid}""")
        records = cursor.fetchall()
        if records:
            query = records[0]['query']
            if 'blogsite_id in (' in query:
                blog_ids = query[query.find("(")+1:query.find(")")]
                if blog_ids:
                    blog_ids = blog_ids[:-1] if ',' == blog_ids[-1] else blog_ids

                    cursor.execute(
                        f"""select post from blogposts where blogsite_id in ({blog_ids})""")
                    records = cursor.fetchall()

                    # Get top terms from blog_ids
                    try:
                        terms_result = getTopKWS(blog_ids, ip)
                    except Exception as e:
                        print(e)
                        try:
                            print('Retrying...')
                            terms_result = getTopKWS(blog_ids, ip)
                            print('success')
                        except Exception as e:
                            print(e)

                    # Count terms and group by year
                    if terms_result:
                        data_ = []
                        for term, occurrence in terms_result:
                            PARAMS = term, blog_ids, tid
                            data_.append(PARAMS)

                        if parallel:
                            print("starting multi-process")
                            process_pool = ProcessPool(num_processes)
                            pbar = tqdm(process_pool.imap(single_process, data_), desc="Terms", ascii=True,  file=sys.stdout, total=len(data_))
                            for x in pbar:
                                pbar.update(1)
                                
                                # Update status on DB
                                if update__status:
                                    status = round((pbar.last_print_n/len(data_)) * 100)
                                    updateStatus(status, tid)

                            process_pool.close()
                            print("Joining pool")
                            process_pool.join()
                            print("Clearing pool")
                            process_pool.clear()
                            print("Finished!")

                        else:
                            for x in tqdm(data_, desc="Terms", ascii=True,  file=sys.stdout):
                                single_process(x)
