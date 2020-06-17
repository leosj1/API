import re
import mysql.connector
from datetime import datetime
import multiprocessing


def getconf2():
    return ('cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers')


def query(config, sql):
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    mydb = mysql.connector.connect(
        host=ip, user=user_name, passwd=password, database=db)
    # sql = f"select * from trackers where tid = {tid}"
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    result = mycursor.fetchall()
    mydb.commit()

    mycursor.close()
    mydb.close()

    return result


def sort_dict(dic, limit):
    term_dict_sorted = {k: int(v) for k, v in sorted(
        dic.items(), key=lambda item: item[1], reverse=True)}
    final_terms = {}
    top_n = list(term_dict_sorted.keys())[0:limit]
    for key in top_n:
        occur = term_dict_sorted[key]
        if int(occur) != 0:
            final_terms[key] = occur

    return final_terms


def get_topterms(q_topterms, conf):
    try:
        topterms_result = query(conf, q_topterms)
        term_dict = {}
        posts = []
        date = []
        for i in range(len(topterms_result)):
            t = topterms_result[i][2].replace('),', '----').replace('(', '').replace(
                ']', '').replace('[', '').replace(')', '').replace('\'', '').split('----')
            for elem in t:
                if elem != 'BLANK':
                    key = elem.split(',')[0].replace('\'', '').strip()
                    val = elem.split(',')[1].replace('\'', '').strip()
                    if key in term_dict:
                        v = int(term_dict[key])
                        new_v = v + int(val)
                        term_dict[key] = new_v
                    else:
                        term_dict[key] = int(val)

            posts.append(topterms_result[i][6])
            date.append(topterms_result[i][4])

        final_terms = sort_dict(term_dict, 100)

        return final_terms, posts, date

    except:
        return {}, [], []


def getTerms(tid, conf):
    q_trackers = f"select * from trackers where tid = {tid}"
    tracker_result = query(conf, q_trackers)
    if len(tracker_result) > 0:
        blogsite_ids = tracker_result[0][5].replace(
            'blogsite_id in', '').strip()
        s_new = ''
        s = blogsite_ids
        if s[-1] == ')':
            if s[-2] == ',':
                s_new = s[0:-2] + ')'
                blogsite_ids = s_new
    else:
        blogsite_ids = '()'

    q_topterms = f'select * from blogpost_terms where blogsiteid in {blogsite_ids}'

    return get_topterms(q_topterms, conf), blogsite_ids


def cleanbrackets(s):
    s_new = ''

    if s == '':
        return s_new
    if s[-1] == ',':
        s_new = s[0:-1]
    else:
        s_new = s

    return s_new


def getandUpdateTerms(blogsite_ids, conf):
    q_topterms = f'select * from blogpost_terms where blogsiteid in ({blogsite_ids})'
    return get_topterms(q_topterms, conf)[0]


def insert_terms(tid, result, query, keyword_trend, conf):
    config = conf
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = "insert into tracker_keyword (tid, terms, query, keyword_trend) values (%s, %s, %s, %s)"
    mydb = mysql.connector.connect(
        host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()

    mycursor.execute(sql, (str(tid), str(result),
                           str(query), str(keyword_trend)))

    mydb.commit()

    mycursor.close()
    mydb.close()




def update_terms(tid, result, query, keyword_trend, conf):
    # start = datetime.now()
    config = conf
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = "update tracker_keyword set terms = %s, query = %s, keyword_trend = %s where tid = %s"

    mydb = mysql.connector.connect(
        host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()
    mycursor.execute(sql, (str(result), str(query),
                           str(keyword_trend), str(tid)))

    mydb.commit()

    mycursor.close()
    mydb.close()


def delete_terms_and_cluster(tid, conf):
    start = datetime.now()
    config = conf
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = f"delete from tracker_keyword where tid = {tid}"
    sql2 = f"delete from clusters where cluster_id = {tid}"

    mydb = mysql.connector.connect(
        host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()

    mycursor.execute(sql)
    mycursor.execute(sql2)
    mydb.commit()

    mycursor.close()
    mydb.close()

    end = datetime.now()
    print(f'it took {end - start}')


def counOccurence(term, data):
    result = []
    for p, d in data:
        dic = {}
        year = str(d).split('-')[0].strip()
        if year not in dic:
            dic[year] = p.count(term)
        else:
            new_v = p.count(term)
            old_v = dic[year]
            dic[year] = new_v + old_v

        result.append(dic)
    return result


def keywordTrend(tid, conf):
    res = getTerms(tid, conf)[0]
    terms = res[0]
    posts = res[1]
    date = res[2]


    for key in terms:
        # dic[key] = []
        # temp = {}
        print(counOccurence(key, zip(posts, date)))
    # return getTerms(tid, conf)[0]


def filestat(q):
    term_dict = {}

    t = q.replace('),', '----').replace('(', '').replace(']',
                                                         '').replace('[', '').replace(')', '').replace('\'', '').split('----')

    for elem in t:
        if elem != 'BLANK':
            key = elem.split(',')[0].replace('\'', '').strip()
            val = elem.split(',')[1].replace('\'', '').strip()
            if key in term_dict:
                v = term_dict[key]
                new_v = v + int(val)
                term_dict[key] = new_v
            else:
                term_dict[key] = int(val)

    return term_dict
