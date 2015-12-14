# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from streaming_app_conf import *
from pprint import pprint

# For spark
sys.path.append('/data01/app/bigdata/spark/python')
sys.path.append('/data01/app/bigdata/spark/python/lib/py4j-0.8.2.1-src.zip')
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/data01/app/bigdata/spark'
SPARK_HOME = os.environ['SPARK_HOME']

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def fun_map_print(x):
    print('= = ' * 10, '[myapp fun_map_print] x = ', x)
    return x


def fun_union_in_rdd_map(line, i):
    list1 = [x.strip()+str(i) for x in line.strip().split(',')]
    ret = ','.join(list1)
    # print('= = ' * 10, '[myapp fun_union_in_rdd_map] ret = ', ret)
    return ret


def fun_union_in_dstream_foreachRDD(rdd):
    rdd_list_outer = []
    for i in range(0, 3):
        rdd2 = rdd.map(lambda line: fun_union_in_rdd_map(line, i))
        rdd_list_outer.append(rdd2)

    rdd_union_outer = reduce(lambda rddx, rddy: rddx.union(rddy), rdd_list_outer)
    result = rdd_union_outer.map(fun_map_print)
    print('result.isEmpty() = ', result.isEmpty())
    if result.isEmpty():
        result
    else:
        result.count()


def main():

    master = 'local[2]'
    app_name = 'reduce_demo1'

    # print(range(0, 3))

    sc = SparkContext(master, app_name)
    ssc = StreamingContext(sc, 15)

    host = 'localhost'
    port = 9999
    stream = ssc.socketTextStream(host, port)
    stream.foreachRDD(fun_union_in_dstream_foreachRDD)

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
