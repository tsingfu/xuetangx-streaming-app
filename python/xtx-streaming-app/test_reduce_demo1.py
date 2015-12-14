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


def fun_map_print(x):
    print('= = ' * 10, '[myapp fun_map_print] x = ', x)
    return x


def main():

    master = 'local[1]'
    app_name = 'reduce_demo1'

    # print(range(0, 3))

    sc = SparkContext(master, app_name)

    # 测试1：正常
    # rdd_list = [sc.parallelize(range(i * 3, (i+1) * 3)) for i in range(0,3)]
    # rdd_union = sc.union(rdd_list)
    # print(rdd_union.getNumPartitions())
    # result = rdd_union.map(fun_map_print)
    # result.count()

    # 测试2：两次 union
    rdd_list_outer = []
    for x in ['a', 'b', 'c']:
        rdd_list_inner = [sc.parallelize(map(lambda j: x + str(j),range(i * 3, (i+1) * 3))) for i in range(0,3)]
        rdd_union_inner = sc.union(rdd_list_inner)
        rdd_list_outer.append(rdd_union_inner)

    rdd_union_outer = reduce(lambda rddx, rddy: rddx.union(rddy), rdd_list_outer)
    result = rdd_union_outer.map(fun_map_print)
    result.count()

    sc.stop()


if __name__ == '__main__':
    main()
