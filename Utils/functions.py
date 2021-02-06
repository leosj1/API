from gensim.parsing.preprocessing import strip_tags, strip_punctuation, strip_short
from gensim.parsing.preprocessing import strip_multiple_whitespaces, strip_numeric, stem_text
from gensim.parsing.preprocessing import strip_non_alphanum, remove_stopwords, preprocess_string
from elasticsearch import Elasticsearch
import json
from pathos.multiprocessing import freeze_support, ProcessPool
from tqdm import tqdm
import sys
import configparser
from datetime import datetime
import time

from Utils.sql import SqlFuncs

class Functions:
    def get_config(self):
        config = configparser.ConfigParser()
        config.read(r"C:\config.ini")

        DB_MOVER=config["DB_MOVER"]
        ip = DB_MOVER["HOST"]
        user_name = DB_MOVER["USER"] 
        password =  DB_MOVER["PASS"]
        db = DB_MOVER["DB"]

        return ip, user_name, password, db

    def get_stop_words(self):
        stop_words = []
        with open("C:\\API\\stopwords.txt", "r", encoding="utf-8") as f:
            for line in f:
                stop_words.append(str(line.strip()))
                
        new_stp_wrds = ['00','abbã³l', 'acaba', 'acerca', 'aderton', 'ahimã', 'ain', 'akã', 'alapjã', 'alors', 'alã', 'alã³l', 'alã³la', 'alã³lad', 'alã³lam', 'alã³latok', 'alã³luk', 'alã³lunk', 'amã', 'annã', 'appendix', 'arrã³l', 'attã³l', 'azokbã³l', 'azokkã', 'azoknã', 'azokrã³l', 'azoktã³l', 'azokã', 'aztã', 'azzã', 'azã', 'ba', 'bahasa', 'bb', 'bban', 'bbi', 'bbszã', 'belã', 'belã¼l', 'belå', 'bennã¼k', 'bennã¼nk', 'bã', 'bãºcsãº', 'cioã', 'cittã', 'ciã²', 'conjunctions', 'cosã', 'couldn', 'csupã', 'daren', 'didn', 'dik', 'diket', 'doesn', 'don', 'dovrã', 'ebbå', 'effects', 'egyedã¼l', 'egyelå', 'egymã', 'egyã', 'egyã¼tt', 'egã', 'ek', 'ellenã', 'elså', 'elã', 'elå', 'ennã', 'enyã', 'ernst', 'errå', 'ettå', 'ezekbå', 'ezekkã', 'ezeknã', 'ezekrå', 'ezektå', 'ezekã', 'ezentãºl', 'ezutã', 'ezzã', 'ezã', 'felã', 'forsûke', 'fã', 'fûr', 'fûrst', 'ged', 'gen', 'gis', 'giã', 'gjûre', 'gre', 'gtã', 'gy', 'gyet', 'gã', 'gã³ta', 'gã¼l', 'gã¼le', 'gã¼led', 'gã¼lem', 'gã¼letek', 'gã¼lã¼k', 'gã¼lã¼nk', 'hadn', 'hallã³', 'hasn', 'haven', 'herse', 'himse', 'hiã', 'hozzã', 'hurrã', 'hã', 'hãºsz', 'idã', 'ig', 'igazã', 'immã', 'indonesia', 'inkã', 'insermi', 'ismã', 'isn', 'juk', 'jã', 'jã³', 'jã³l', 'jã³lesik', 'jã³val', 'jã¼k', 'kbe', 'kben', 'kbå', 'ket', 'kettå', 'kevã', 'khã', 'kibå', 'kikbå', 'kikkã', 'kiknã', 'kikrå', 'kiktå', 'kikã', 'kinã', 'kirå', 'kitå', 'kivã', 'kiã', 'kkel', 'knek', 'knã', 'korã', 'kre', 'krå', 'ktå', 'kã', 'kã¼lã', 'lad', 'lam', 'latok', 'ldã', 'led', 'leg', 'legalã', 'lehetå', 'lem', 'lennã', 'leszã¼nk', 'letek', 'lettã¼nk', 'ljen', 'lkã¼l', 'll', 'lnak', 'ltal', 'ltalã', 'luk', 'lunk', 'lã', 'lã¼k', 'lã¼nk', 'magã', 'manapsã', 'mayn', 'megcsinã', 'mellettã¼k', 'mellettã¼nk', 'mellã', 'mellå', 'mibå', 'mightn', 'mikbå', 'mikkã', 'miknã', 'mikrå', 'miktå', 'mikã', 'mindenã¼tt', 'minã', 'mirå', 'mitå', 'mivã', 'miã', 'modal', 'mostanã', 'mustn', 'myse', 'mã', 'mãºltkor', 'mãºlva', 'må', 'måte', 'nak', 'nbe', 'nben', 'nbã', 'nbå', 'needn', 'nek', 'nekã¼nk', 'nemrã', 'nhetå', 'nhã', 'nk', 'nnek', 'nnel', 'nnã', 'nre', 'nrå', 'nt', 'ntå', 'nyleg', 'nyszor', 'nã', 'nå', 'når', 'også', 'ordnung', 'oughtn', 'particles', 'pen', 'perchã', 'perciã²', 'perã²', 'pest', 'piã¹', 'puã²', 'pã', 'quelqu', 'qué', 'ra', 'rcsak', 'rem', 'retrieval', 'rlek', 'rmat', 'rmilyen', 'rom', 'rt', 'rte', 'rted', 'rtem', 'rtetek', 'rtã¼k', 'rtã¼nk', 'rã', 'rã³la', 'rã³lad', 'rã³lam', 'rã³latok', 'rã³luk', 'rã³lunk', 'rã¼l', 'sarã', 'schluss', 'semmisã', 'shan', 'shouldn', 'sik', 'sikat', 'snap', 'sodik', 'sodszor', 'sokat', 'sokã', 'sorban', 'sorã', 'sra', 'st', 'stb', 'stemming', 'study', 'sz', 'szen', 'szerintã¼k', 'szerintã¼nk', 'szã', 'sã', 'talã', 'ted', 'tegnapelå', 'tehã', 'tek', 'tessã', 'tha', 'tizenhã', 'tizenkettå', 'tizenkã', 'tizennã', 'tizenã', 'tok', 'tovã', 'tszer', 'tt', 'tte', 'tted', 'ttem', 'ttetek', 'ttã¼k', 'ttã¼nk', 'tulsã³', 'tven', 'tã', 'tãºl', 'tå', 'ul', 'utoljã', 'utolsã³', 'utã', 'vben', 'vek', 'velã¼k', 'velã¼nk', 'verbs', 'ves', 'vesen', 'veskedjã', 'viszlã', 'viszontlã', 'volnã', 'vvel', 'vã', 'vå', 'vöre', 'vört', 'wahr', 'wasn', 'weren', 'won', 'wouldn', 'zadik', 'zat', 'zben', 'zel', 'zepesen', 'zepã', 'zã', 'zã¼l', 'zå', 'ã³ta', 'ãºgy', 'ãºgyis', 'ãºgynevezett', 'ãºjra', 'ãºr', 'ð¾da', 'γα', 'البت', 'بالای', 'برابر', 'برای', 'بیرون', 'تول', 'توی', 'تی', 'جلوی', 'حدود', 'خارج', 'دنبال', 'روی', 'زیر', 'سری', 'سمت', 'سوی', 'طبق', 'عقب', 'عل', 'عنوان', 'قصد', 'لطفا', 'مد', 'نزد', 'نزدیک', 'وسط', 'پاعین', 'کنار', 'अपन', 'अभ', 'इत', 'इनक', 'इसक', 'इसम', 'उनक', 'उसक', 'एव', 'ऐस', 'करत', 'करन', 'कह', 'कहत', 'गय', 'जह', 'तन', 'तर', 'दब', 'दर', 'धर', 'नस', 'नह', 'पहल', 'बन', 'बह', 'यत', 'यद', 'रख', 'रह', 'लक', 'वर', 'वग़', 'सकत', 'सबस', 'सभ', 'सर', 'ἀλλ']       
        final_stp_wrds = stop_words + new_stp_wrds
        stop_words = final_stp_wrds
        return stop_words

    def clean_text(self, text):
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
        stop_words = self.get_stop_words()
        filtered_sentence = [w for w in example_sent if not w in stop_words]

        return filtered_sentence


    # FUNCTION TO GET TOP 100 TERMS BY BLOG_IDS FROM MYSQL
    def getTopKWS(self, ids):
        conf = getconf2()
        s = SqlFuncs(conf)

        # Get blogsites in tracker
        connection = s.get_connection(conf)
        with connection.cursor() as cursor:
            cursor.execute(f"""
                select n.term, sum(n.occurr) occurrence
                from blogpost_terms_api, 
                json_table(terms_test,
                '$[*]' columns(
                    term varchar(255) path '$.term',
                    occurr int(11) path '$.occurrence'
                    )
                ) 
                as n
                where blogsiteid in ({ids})
                group by n.term
                order by occurrence desc
                limit 100;
            """)

            records = cursor.fetchall()

            if records:
                data = [x['term'] for x in records]
            else:
                data = None
            
        connection.close()
        cursor.close()

        return data


    def updateStatus(self, status, tid):
        conn = getconf2()
        s = SqlFuncs(conn)

        if int(status) < 100:
            data = 0, status, tid
            s.update_insert("update tracker_keyword set status = %s, status_percentage = %s where tid = %s", data, conn)
        else:
            data = 1, status, tid
            s.update_insert("update tracker_keyword set status = %s, status_percentage = %s where tid = %s", data, conn)

    def single_process(self, parameters):
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
                df['date'] = pd.to_datetime(df['date'], errors = 'coerce')
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

    def testingKWT(self, tid, ip, parallel, update__status, num_processes):
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
                    if blog_ids and 'NaN' not in blog_ids:
                        blog_ids = blog_ids[:-1] if ',' == blog_ids[-1] else blog_ids

                        cursor.execute(
                            f"""select post from blogposts where blogsite_id in ({blog_ids})""")
                        records = cursor.fetchall()

                        # Get top terms from blog_ids
                        try:
                            terms_result = getTopKWS(blog_ids)
                        except Exception as e:
                            print(e)
                            try:
                                print('Retrying...')
                                terms_result = getTopKWS(blog_ids)
                                print('success')
                            except Exception as e:
                                terms_result = []
                                print(e)

                        # Count terms and group by year
                        if terms_result:
                            data_ = []
                            for term in terms_result:
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
                                        if status <= 99 and status >= 90:
                                            status = 100
                                        updateStatus(status, tid)

                                process_pool.close()
                                print("Joining pool")
                                process_pool.join()
                                print("Clearing pool")
                                process_pool.clear()
                                print("Finished!")

                            else:
                                for x in data_:
                                    single_process(x)

class Time():
    def __init__(self):
        self.start = time.time()
        self.end = None
        self.runtime_mins = None
        self.runtime_secs = None

    def finished(self):
        self.end = time.time()
        self.runtime_mins, self.runtime_secs = divmod(
            self.end - self.start, 60)
        self.runtime_mins = round(self.runtime_mins, 0)
        self.runtime_secs = round(self.runtime_secs, 0)
        print("Time to complete: {} Mins {} Secs".format(self.runtime_mins,
                                                         self.runtime_secs), f'AT - {datetime.today().isoformat()}')
        return "Time to complete: {} Mins {} Secs".format(self.runtime_mins,
                                                         self.runtime_secs), f'AT - {datetime.today().isoformat()}'
