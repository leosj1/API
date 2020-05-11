import re
import mysql.connector
from datetime import datetime
import multiprocessing
from collections import Counter
import collections, functools, operator 
import json
start = datetime.now()
# tid = input("PLEASE INPUT TRACKER ID")


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

def sort_dict(dic, limit):
    term_dict_sorted = {k: int(v) for k, v in sorted(dic.items(), key=lambda item: item[1], reverse = True)}
    final_terms = {}
    top_n = list(term_dict_sorted.keys())[0:limit]
#     print(top_100)
    for key in top_n:
        final_terms[key] = term_dict_sorted[key]

    return final_terms

def get_topterms(q_topterms, conf):
#     q_topterms = f'select terms from blogpost_terms where blogpost_id in ({post_ids})'
    try:
        topterms_result = query(conf, q_topterms)
        print('query_result',len(topterms_result))
        # print('topterms_result',topterms_result)

        # term_dict = {}
        posts = []
        date = []
        term_dict = {}
        dict_array = []
        # counter = collections.Counter()
        print('counting....')
        for i in range(len(topterms_result)):
            t = topterms_result[i][2]
            # for elem in t:
            # print('t--',json.loads(t))
            l = json.loads(t)
            it = list(l.items())
            dict_array.extend(it)

            # term_dict.update(json.loads(t))
            # print('term_dict--',term_dict)

            # date.append(topterms_result[i][4])
            # break
        print('done appending')

        # result = dict(functools.reduce(operator.add,map(collections.Counter,term_dict)))
        print('done with counter')
        # result = dict(counter)
        # final_terms = sort_dict(dict(term_dict), 100)
        print('done sorting')
        
    #     print(term_dict.items())
        # for k, v in  sorted(x.items()
        # print('term_dict', term_dict)
        
        return dict_array,posts,date

    except Exception as e:
        print(e)
        return {},[],[]

def getTerms(tid, conf):
#     conf = (ip, user_name, password, db)

    q_trackers = f"select * from trackers where tid = {tid}"
    tracker_result = query(conf, q_trackers)
    # print(tracker_result)
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


    print(blogsite_ids)
    q_topterms = f'select * from blogpost_terms_api where blogsiteid in {blogsite_ids}'
    
    # print('terms----------------------------------',q_topterms)
    
    return get_topterms(q_topterms, conf),blogsite_ids

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
    print(q_topterms)
    return get_topterms(q_topterms, conf)[0]

def insert_terms(tid, result, query, conf):
    start = datetime.now()
    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = "insert into tracker_keyword (tid, terms, query) values (%s,%s,%s)"
    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()
    print((str(tid), str(result), str(query)))
    mycursor.execute(sql,(str(tid), str(result), str(query)))   
    # result = mycursor.fetchall()
    mydb.commit()

    mycursor.close()
    mydb.close()

    end = datetime.now()
    print(f'it took {end - start}')

def update_terms(tid, result, query, conf):
    start = datetime.now()
    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = "update tracker_keyword set terms = %s, query = %s where tid = %s"
    print(sql)
    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()
    mycursor.execute(sql,(str(result), str(query), str(tid)))
    # result = mycursor.fetchall()
    mydb.commit()

    mycursor.close()
    mydb.close()

    end = datetime.now()
    print(f'it took {end - start}')

def delete_terms(tid, conf):
    start = datetime.now()
    config = conf 
    ip = config[0]
    user_name = config[1]
    password = config[2]
    db = config[3]

    sql = f"delete from tracker_keyword where tid = {tid}"
    mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    # result = mycursor.fetchall()
    mydb.commit()

    mycursor.close()
    mydb.close()

    end = datetime.now()
    print(f'it took {end - start}')

def counOccurence(term,data):
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

    dic = {}
    v = []
    for key in terms:
        # dic[key] = []
        # temp = {}
        print(counOccurence(key, zip(posts, date)))
    # return getTerms(tid, conf)[0]

def filestat(q):
    term_dict ={}
    # for i in q:
    # print(q)
    t = q.replace('),', '----').replace('(','').replace(']','').replace('[','').replace(')','').replace('\'','').split('----')
    # print(t)
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

    return term_dict
#     term_dict_sorted = {k: v for k, v in sorted(term_dict.items(), key=lambda item: item[1], reverse = True)}
#     final_terms = {}
#     top_100 = list(term_dict_sorted.keys())[0:100]
# #     print(top_100)
#     for key in top_100:
#         final_terms[key] = term_dict_sorted[key]

#     return final_terms
    # return t[0]
    # for i in q:
    #     return i
# if __name__ == '__main__': 
#     start = datetime.now() 
#     conf = getconf()
#     # q_topterms = f'select * from blogpost_terms where blogsiteid in (109,750,193,808,1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399)'
#     q_topterms = f'select * from blogpost_terms where blogsiteid in (109)'

#     cores = (multiprocessing.cpu_count()*2)
#     topterms_result = query(conf, q_topterms)
#     pool_get_attributes = multiprocessing.Pool(8)
#     # print(cores)
    
#     terms = [topterms_result[i][2] for i in range(len(topterms_result))]
#     # print(terms)

#     result_map = pool_get_attributes.map(filestat, terms)
#     print(result_map)
#     end = datetime.now()

#     print(end - start)
#     pool_get_attributes.close()
#     pool_get_attributes.join()


# if __name__ == '__main__':
    # import time
    # conf = getconf()
    # start = time.perf_counter()
    # q_count = f'select count(*) from blogpost_terms where blogsiteid in (813,815,809,811,812,806,808,817,644,652,616,641,732,761,709,128) order by blogpost_id'
    # result_count = query(conf, q_count)
    # count = result_count[0][0]
    # print(count)
    # first = 0
    # second = 0
    # limit = 1000
    # # data = []
    # # for i in range(0,count,limit):
    # #     first = i
    # #     second = limit
    # #     # if second > count:
    # #     #     second = count
        
    # #     # print(first , second)
    # #     data.append(f'select * from blogpost_terms where blogsiteid in (813,815,809,811,812,806,808,817,644,652,616,641,732,761,709,128) order by blogpost_id limit {first}, {second}')

    # q_topterms = f'select * from blogpost_terms where blogsiteid in (813,815,809,811,812,806,808,817,644,652,616,641,732,761,709,128) order by blogpost_id'
    # result = query(conf, q_topterms)
    # # import concurrent.futures
    # # with concurrent.futures.ThreadPoolExecutor() as executor:
    # #     result = executor.map(query, data)
    # #     for x in result:
    # #         print(len)

    # print(len(result))

    # stop = time.perf_counter()
    # print(stop - start)


# import time
# start = time.perf_counter()
# conf = getconf()

# answer = getTerms(7, conf)[0][0]

# print(answer,'-------',len(answer))
# stop = time.perf_counter()
# print(stop - start)


# config = getconf()
# ip = config[0]
# user_name = config[1]
# password = config[2]
# db = config[3]

# print(sql)
# mydb = mysql.connector.connect(host=ip, user=user_name, passwd=password, database=db)
# mycursor = mydb.cursor()
# # mycursor.execute(sql)
# for row in mycursor.execute('SELECT * FROM blogpost_terms where blogsiteid in (813,815,809,811,812,806,808,817,644,652,616,641,732,761,709,128) order by blogpost_id'): 
#     print (row) 
#     break

# from mysql.connector import MySQLConnection, Error
# # from python_mysql_dbconfig import read_db_config

# def iter_row(cursor, size=10):
#     while True:
#         rows = cursor.fetchmany(size)
#         if not rows:
#             break
#         for row in rows:
#             yield row

# def query_with_fetchmany():
#     try:
#         dbconfig = getconf()
#         conn = MySQLConnection(dbconfig)
#         cursor = conn.cursor()

#         cursor.execute("SELECT * FROM books")

#         for row in iter_row(cursor, 10):
#             print(row)

#         cursor.close()
#         conn.close()
#     except Error as e:
#         print(e)

#     # finally:
        

# query_with_fetchmany()

