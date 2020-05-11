
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType
import pyspark
import gc
import time
import terms_API as tt



def countt(data):
    # sc = None
    # sc = SparkContext("local[4]", "test")

    # spark = SparkSession.builder.appName(
    #     'testing').master("local[*]").getOrCreate()
    # sc = spark.sparkContext
    # try:
    
    conf = SparkConf().setAppName('testing').setMaster('spark://144.167.35.138:7077').set('spark.cores.max',20).set('spark.executor.memory', '100g').set('spark.driver.memory', '15g')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # spark.sparkContext.setSystemProperty('spark.executor.memory', '15g')
    # spark.sparkContext.setSystemProperty('spark.driver.memory', '15g')
    # text_file  = sc.textFile(pathin)

    spark.sparkContext.setLogLevel("ERROR")
    # spark.sparkContext.setSystemProperty('spark.executor.memory', '15g')
    # lines_rdd = spark.sparkContext.textFile(pathin)
    # lines_list = lines_rdd.collect()
    # counts1 = lines_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).takeOrdered(100, key = lambda x: -x[1])
    # counts = lines_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).max(key = lambda x: x[1])
    # data = {"aid": 1, "bbc": 1, "buy": 1, "mao": 1, "oil": 2, "pew": 1, "red": 1, "ally": 2, "anti": 1, "army": 1, "bloc": 1, "book": 1, "care": 1, "case": 1, "east": 1, "fell": 1, "flee": 1, "game": 1, "good": 1, "gulf": 1, "guys": 1, "host": 1, "huge": 1, "iran": 4, "isnt": 1, "jews": 1, "join": 1, "july": 1, "land": 1, "left": 1, "life": 2, "lots": 1, "meet": 1, "nuke": 1, "open": 1, "paid": 2, "read": 1, "reza": 1, "role": 1, "send": 1, "shah": 5, "sold": 1, "ties": 1, "weak": 1, "week": 3, "west": 1, "adolf": 1, "adopt": 1, "assad": 1, "build": 1, "burke": 1, "cared": 1, "chose": 7, "close":
    #         1, "didnt": 1, "egypt": 4, "enemy": 2, "exile": 1, "expel": 1, "fatal": 2, "funds": 2, "hands": 1, "human": 3, "irans": 1, "kings": 1, "media": 1, "model": 2, "mouth": 1, "nabil": 1, "nobel": 2, "north":
    #         1, "obama": 2, "paint": 1, "paris": 1, "peace": 2, "power": 5, "price": 1, "prize": 2, "reach": 2, "refer": 2, "sadat": 1, "sanad": 1, "shahs": 1, "small": 1, "times": 1, "today": 4, "tools": 1, "union":
    #         2, "wasnt": 2, "write": 1, "wrong": 2, "actors": 1, "actual": 1, "allies": 5, "berlin": 1, "called": 1, "center": 1, "change": 1, "choice": 4, "crimes": 1, "crisis": 1, "defend": 1, "doomed": 1, "edmund": 1, "europe": 3, "family": 2, "famous": 1, "funded": 1, "hanged": 1, "heikal": 1, "hitler": 1, "hosted": 1, "impact": 1, "inline": 1, "israel": 8, "korean": 1, "labels": 1, "leader": 2, "maikel": 1, "middle": 4, "moment": 2, "months": 1, "moving": 1, "normal": 1, "palace": 1, "paying": 2, "people": 1, "posted": 1, "prizes": 1, "public": 1, "reagan": 1, "remain": 1, "repeat": 1, "return": 1, "rights": 3, "simply": 1, "soviet": 2, "speech": 1, "stalin": 1, "strong": 1, "threat": 1, "traded": 1, "troops": 1, "voters": 1, "writer": 1, "admired": 1, "bombing": 1, "british": 1, "chinese": 1, "choices": 1, "corrupt": 2, "country": 4, "culture": 1, "decided": 1, "deliver": 1, "denying": 1, "destroy": 1, "display": 1, "eastern": 4, "enemies": 2, "execute": 1, "extreme": 1, "history": 3, "honored": 2, "hussein": 1, "include": 1, "iranian": 1, "knowing": 3, "mistake": 1, "muslims": 1, "nuclear": 1, "opening": 1, "options": 1, "pahlavi": 9, "persian": 1, "politic": 1, "qaddafi": 1, "radical": 1, "realize": 2, "running": 1, "semitic": 1, "silence": 1, "society": 1, "started": 3, "suicide": 1, "support": 4, "thought": 1, "weapons": 2, "western": 4, "american": 3, "annoying": 1, "articles": 1, "citizens": 1, "complain": 1, "contacts": 1, "credible": 1, "decision": 1, "distance": 1, "distract": 1, "egyptian": 6, "european": 3, "executed": 2, "exported": 1, "fashions": 1, "fighting": 1, "forgiven": 1, "fullpost": 1, "greatest": 2, "happened": 3, "historic": 1, "hostages": 1, "incident": 1, "invented": 1, "khomeini": 5, "laureate": 1, "learning": 1, "liberals": 3, "michelle": 1, "mohammed": 2, "pahlavis": 1, "politics": 1, "pressure": 1, "problems": 1, "prophecy": 1, "realizes": 1, "received": 1, "relation": 1, "russians": 1, "security": 1, "solution": 1, "supports": 1, "thinking": 1, "weakness": 1, "attention": 1, "boycotted": 1, "committed": 2, "companies": 1, "countries": 4, "diplomacy": 1, "diplomats": 1, "egyptians": 1, "elbaradei": 1, "execution": 1, "incidents": 1, "interests": 1, "islamists": 7, "justified": 1, "literally": 1, "nominated": 1, "pragmatic": 1, "published": 1, "receiving": 2, "relations": 1, "religious": 1, "resources": 1, "socialism": 1, "terrorism": 1, "thousands": 1, "ceremonies": 1, "characters":
    #         1, "christians": 1, "complained": 1, "conclusion": 1, "considered": 3, "fulfilling": 1, "government": 1, "historians": 1, "ideologies": 1, "opposition": 1, "parliament": 1, "practicing": 1, "recognized":
    #         1, "resistance": 1, "retirement": 1, "revolution": 4, "surrounded": 1, "violations": 2, "westernize": 1, "consciously": 1, "friendships": 1, "interesting": 2, "palestinian": 2, "responsible": 1, "sovereignty": 1, "traditional": 1, "civilization": 1, "dictatorship": 2, "nationalists": 1, "instrumenting": 1, "international": 4, "opportunities": 1, "organizations": 1}


    my_list_rdd=sc.parallelize(data,10)
    # my_list_rdd2 = my_list_rdd.repartition( 10 ) 
        # result = data.map(lambda x: x).collect()
    print(dict(my_list_rdd.reduceByKey(lambda a, b: a + b).takeOrdered(10, key = lambda x: -x[1])))
    # print(my_list_rdd.map(lambda x:x.items()[0]).reduceByKey(lambda x, y: x + y).collectAsMap())

    # reduce_rdd = sc.parallelize(data)
    # print(reduce_rdd.map(lambda x:x.items()[0]).reduceByKey(lambda x, y: x + y).collectAsMap())


        # counts.saveAsTextFile(pathout)

        # print(counts, counts1)

    # sc.stop()
        # return counts1
        # except Exception as e:
        #     gc.collect()
        #     sc.stop()

start = time.perf_counter()
conf = tt.getconf()
# print(countt('C:\\Users\\bt_admin\\SOCIAL_COMPUTING_PROJECT\\BT\\SUMMARIES.rtf'))
answer = tt.getTerms(233, conf)[0][0]
print('executed sql')
stop = time.perf_counter()
print('it took ', stop - start)
print(countt(answer))
# print(answer,'-------',len(answer))

stop = time.perf_counter()
print(stop - start)
