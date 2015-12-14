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

sys.path.append('/data01/data/datadir_python/projects/sparkTest/xtx-streaming-app/tracking_log')

from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.serializers import PickleSerializer, MarshalSerializer, AutoSerializer, CompressedSerializer

from streaming_app_platform import StreamingReader
from streaming_app_platform import StreamingApp

from streaming_app_utils import *
from test_utils import *

def main():
    json_file1 = 'file:///data01/data/datadir_github/xuetangx-projects/xuetangx-streaming-app/test/tracking-regiser-test.log'
    schema_conf_string = 'user_id, username, uuid, event_type, host, platform, origin_referer, spam, time, course_id'
    struct_type = generate_df_schmea(schema_conf_string, add_map_flag=True, reserved_field_for_add_map=['labels'])

    # platform_idx = struct_type.field2
    # print('platform_idx = ',  platform_idx)


    master = 'local[2]'
    app_name = 'json2df_demo1'

    sc = SparkContext(master, app_name)
    rdd1 = sc.textFile(json_file1)
    from format_track import TrackingLogFormater

    # 测试1：格式化json日志，转换为df
    rdd2 = rdd1.map(
        lambda line: TrackingLogFormater.fun_rdd_map(line)
        # lambda line: TrackingLogFormater.fun_rdd_map2(line)
    ).filter(lambda row: row is not None)

    sqlc = SQLContext(sc)
    df2 = sqlc.createDataFrame(rdd2, struct_type)
    # df2 = sqlc.jsonRDD(rdd2, struct_type)

    df2.printSchema()
    df2.show()


    # 测试2： 更新 df 属性.
    #  Note:1 dict 转换为 row 的方式 Row(**dict) ，转换后的 row 字段是根据字段名排序
    time_key = 'time'
    time_interval_minute_list = [1]
    time_key_prefix = 'time_minute_'
    # rdd2_row = df2.map(
    #     lambda row: fun_map_row(row, time_key, time_interval_minute_list, time_key_prefix)
    # ).filter(lambda row: row is not None)
    # df3 = sqlc.createDataFrame(rdd2_row, struct_type)
    #
    # df3.printSchema()
    # df3.show()

    # 测试3 ： 获取部分字段，然后再更新属性
    selectClause = 'user_id, username, uuid, time, platform, labels, origin_referer'
    select_list = [col.strip() for col in selectClause.split(',') if col.strip() != '']
    df4 = df2.selectExpr(select_list)
    print('= = ' * 20, '[myapp ] df4.schema = ')
    df4.printSchema()
    struct_type4 = df4.schema  # struct_type4 和 fun_map_row 后的 Row 字段顺序不对，原因：spark-sql 对 dict 转换为 row 的方式 Row(**dict) ，转换后的 row 字段是根据字段名排序
    struct_type4_2 = StructType(sorted([field for field in df4.schema.fields], key=lambda x: x.name))
    # struct_type4_2 = generate_df_schmea(selectClause)  # 指定字段名中有 map字段

    rdd4_row = df4.map(
        lambda row: fun_map_row(row, time_key, time_interval_minute_list, time_key_prefix)
    ).filter(lambda row: row is not None)

    # df5 = sqlc.createDataFrame(rdd4_row, struct_type4)
    df5 = sqlc.createDataFrame(rdd4_row, struct_type4_2)  # TypeError: StructField(labels,MapType(StringType,StringType,true),true) is not JSON serializable
    df5.printSchema()
    df5.show()


    # 测试5 jsonRDD 过程中内部是否对 StructType 的 StructField 有排序操作
    import json
    rdd4_row2 = df4.map(lambda row: json.dumps(row.asDict())).map(
        lambda json_string: fun_map_json(json_string, time_key, time_interval_minute_list, time_key_prefix)
    ).filter(lambda row: row is not None)

    df6 = sqlc.createDataFrame(rdd4_row2, struct_type4_2)  # TypeError: StructField(labels,MapType(StringType,StringType,true),true) is not JSON serializable
    df6.printSchema()
    df6.show()

    sc.stop()


if __name__ == '__main__':
    main()