import findspark
findspark.init()

from pyspark.sql import Row
from collections import OrderedDict
from pyspark import SparkConf, SparkContext

def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))

conf = SparkConf().setAppName('testing').setMaster('spark://144.167.35.138:7077').set('spark.cores.max',20).set('spark.executor.memory', '100g').set('spark.driver.memory', '15g')
sc = SparkContext(conf=conf)
rdd = sc.parallelize([{"arg1": "", "arg2": ""},{"arg1": "", "arg2": ""},{"arg1": "", "arg2": ""}]).map(convert_to_row)
print('here---',hasattr(rdd, "toDF"))
# sc.parallelize([{"arg1": "", "arg2": ""},{"arg1": "", "arg2": ""},{"arg1": "", "arg2": ""}]).toDF()