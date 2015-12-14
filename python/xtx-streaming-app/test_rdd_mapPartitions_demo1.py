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


def fun_batch_iter(p_iter, batch_limit=1):

    import time
    batch_list = []
    batch_size = 0

    def process_item(item):
        print('- - ' * 8, '[debug myapp fun_batch_iter.process_item] item = ', item)
        time.sleep(2)

    def process_batch(p_list):
        print('= = ' * 8, '[debug myapp fun_batch_iter.process_batch] len = ', len(p_list), p_list)

    if batch_limit == 1:
        for x in p_iter:
            process_item(x)
            yield x
    else:
        for x in p_iter:
            current = x
            batch_list.append(current)
            batch_size += 1
            if batch_size == batch_limit:
                print('= = ' * 4, '[debug myapp fun_batch_iter]', type(batch_list))
                process_batch(batch_list)

                for y in batch_list:
                    yield y
                else:
                    print('= = ' * 10, '[debug myapp fun_batch_iter] clean up 1')
                    batch_list = []
                    batch_size = 0

        if batch_size > 0:
            process_batch(batch_list)

            for i in batch_list:
                batch_size -= 1
                yield i
            else:
                print('= = ' * 10, '[debug myapp fun_batch_iter] clean up 2')


def main():
    json_file1 = 'file:///data01/data/datadir_github/xuetangx-projects/xuetangx-streaming-app/test/tracking-regiser-test.log'

    master = 'local[2]'
    app_name = 'json2df_demo1'

    sc = SparkContext(master, app_name)
    rdd1 = sc.textFile(json_file1)
    from format_track import TrackingLogFormater

    # 测试1：格式化json日志，转换为df
    rdd2 = rdd1.mapPartitions(
        lambda iter_x: fun_batch_iter(iter_x, 2)
    )

    rdd2.count()

    sc.stop()


if __name__ == '__main__':
    main()
