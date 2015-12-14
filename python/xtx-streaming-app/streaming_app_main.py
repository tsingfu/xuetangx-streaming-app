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

# For elasticsearch, mongo
sys.path.append('/data01/data/datadir_python_3rd_pacakge/usr/local/lib/python2.7/site-packages')
sys.path.append('/usr/local/lib/python2.7/site-packages')


from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# from pyspark.serializers import PickleSerializer, MarshalSerializer, AutoSerializer, CompressedSerializer

from streaming_app_platform import StreamingReader
from streaming_app_platform import StreamingApp
from streaming_app_platform import CacheManager

from streaming_app_utils import *


def main():
    # 解析配置
    app_id = int(sys.argv[1])
    master = sys.argv[2]
    app_name = sys.argv[3]

    # 应用配置
    assert APP_CONFIG.get(app_id) is not None, \
        '[myapp streaming_app_main.main()] configuration error invalid APP_CONFIG with app.id = ' + str(app_id)
    app_conf = map_conf_properties(APP_CONFIG.get(app_id), 'app.id')[app_id]
    spark_home = app_conf['sparkHome']
    pyFiles = app_conf['pyFiles.list']
    di_id = app_conf.get('app.interfaceId')

    # 数据接口配置
    di_in_conf_with_ds_conf = get_di_conf_with_ds_conf(
        di_id, DATAINTERFACE_CONFIG, DATASOURCE_CONFIG,
        di_key='interface.id', di_ds_key='interface.sourceId', ds_key='source.id', merge_key_name='interface.id'
    )[di_id]
    print('= = ' * 20, type(di_in_conf_with_ds_conf), 'di_in_conf_with_ds_conf = ')
    pprint(di_in_conf_with_ds_conf)

    schema_conf_string = di_in_conf_with_ds_conf['schema']
    struct_type = generate_df_schmea(schema_conf_string)
    # schema_field_list = [x.name for x in struct_type.fields]
    di_in_conf_with_ds_conf['struct.type'] = struct_type
    # di_in_conf_with_ds_conf['struct.field.list'] = schema_field_list

    di_out_confs = [kv for kv in DATAINTERFACE_CONFIG.iteritems() if kv[1].get('interface.type', '') == 'output']
    print('= = ' * 20, type(di_out_confs), 'di_out_confs = ')
    pprint(di_out_confs)

    di_out_confs_with_ds_conf = list_dict_merge(
        [get_di_conf_with_ds_conf(
            kv[0], DATAINTERFACE_CONFIG, DATASOURCE_CONFIG,
            di_key='interface.id', di_ds_key='interface.sourceId', ds_key='source.id', merge_key_name='interface.id')
         for kv in DATAINTERFACE_CONFIG.iteritems() if kv[1].get('interface.type', '') == 'output']
    )

    print('= = ' * 20, type(di_out_confs_with_ds_conf), 'di_out_confs_with_ds_conf = ')
    pprint(di_out_confs_with_ds_conf)

    # 外部缓存配置
    cache_confs_with_ds_conf = list_dict_merge(
        [get_di_conf_with_ds_conf(
            kv[0], CACHE_CONFIG, DATASOURCE_CONFIG,
            di_key='cache.id', di_ds_key='cache.sourceId', ds_key='source.id', merge_key_name='cache.id')
         for kv in CACHE_CONFIG.iteritems()]
    )
    print('= = ' * 20, type(cache_confs_with_ds_conf), 'cache_confs_with_ds_conf = ')
    pprint(cache_confs_with_ds_conf)

    # 指定输入接口准备阶段的配置
    # 准备阶段配置中有效步骤的配置
    # Note: 对 dict 进行 filter，传给function的参数是 dict 的 key
    prepares_config_active = PREPARES_CONFIG[di_id] \
        if PREPARES_CONFIG.get(di_id, {}).get('prepares.enabled', False) else {}
    # print('= = ' * 20, type(prepares_config_active), 'prepares_config_active = ')
    # pprint(prepares_config_active)

    # TODO: 2中方法的结果==测试False， 删除注释
    # prepares_config_active_steps = filter(
    # lambda step_conf: step_conf[1].get('step.enabled', False),
    #     map(lambda step_conf: (step_conf[0], map_conf_properties(step_conf[1])),
    #         prepares_config_active.get('steps', {}).iteritems()
    #     )
    # )
    prepares_config_active_steps = \
        [(k, map_conf_properties(v)) for k, v in prepares_config_active.get('steps', {}).iteritems()
         if v.get('step.enabled', False)]

    print('= = ' * 20, type(prepares_config_active_steps), 'prepares_config_active_steps = ')
    pprint(prepares_config_active_steps)

    # 指定输入接口计算阶段的配置
    # filter 之后变成 list，list 的每个元素是 tuple(computeStatistics.id, computeStatistics.conf_dict)
    computes_config_active = COMPUTES_CONFIG[di_id] \
        if COMPUTES_CONFIG.get(di_id, {}).get('computeStatistics.enabled', False) else {}

    # list[{computeStatistic.id: {conf}}, ...]
    # # TODO: 2中方法的结果==测试False， 删除注释
    # compute_computeStatistics_config_active = filter(
    #     lambda computeStatistic_conf: computeStatistic_conf[1].get('computeStatistic.enabled', False),
    #     computes_config_active.get('computeStatistics', {}).iteritems())

    compute_computeStatistics_config_active = [
        kv for kv in computes_config_active.get('computeStatistics', {}).iteritems()
        if kv[1].get('computeStatistic.enabled', False)]
    print('= = ' * 20, type(compute_computeStatistics_config_active), 'compute_computeStatistics_config_active = ')
    pprint(compute_computeStatistics_config_active)

    # {computeStatistic.id -> list[step_conf_tuple]}, 其中 step_conf_tuple = (step_id, step_conf_dict)
    compute_prepares_config_active = dict(map(
        lambda computeStatistic_conf: (computeStatistic_conf[0],
                                       sorted(list_dict_merge(
                                           map(lambda step_conf: map_conf_properties(step_conf[1], 'step.id'),
                                               filter(
                                                   lambda step_conf: step_conf[1].get('step.enabled', False),
                                                   computeStatistic_conf[1].get('prepares.steps', {}).iteritems())
                                           )).iteritems())
        ), compute_computeStatistics_config_active))
    # print('= = ' * 30, compute_prepares_config_active2 == compute_prepares_config_active)

    print('= = ' * 20, type(compute_prepares_config_active), 'compute_prepares_config_active = ')
    pprint(compute_prepares_config_active)

    compute_computes_config_active = dict(map(
        lambda computeStatistic_conf: (computeStatistic_conf[0],
                                       sorted(list_dict_merge(
                                           map(lambda step_conf: map_conf_properties(step_conf[1], 'step.id'),
                                               filter(lambda step_conf: step_conf[1].get('step.enabled', False),
                                                      computeStatistic_conf[1].get('computes.steps', {}).iteritems())
                                           )).iteritems())
        ), compute_computeStatistics_config_active))
    print('= = ' * 20, type(compute_computes_config_active), 'compute_computes_config_active = ')
    pprint(compute_computes_config_active)

    test_flag = False
    if not test_flag:
        # 初始化
        # 测试 serializer
        # serializer 默认取值 PickleSerializer()  #UnpicklingError: invalid load key, '{'.
        # serializer=MarshalSerializer()  # ValueError: bad marshal data
        # serializer=AutoSerializer()  # ValueError: invalid sevialization type: {
        # serializer=CompressedSerializer(PickleSerializer())  # error: Error -3 while decompressing data: incorrect header check

        # sc = SparkContext(master, app_name, sparkHome = spark_home, pyFiles=pyFiles)
        # sc = SparkContext(master, app_name, sparkHome = sparkHome, pyFiles=pyFiles, serializer=MarshalSerializer())
        # sc = SparkContext(master, app_name, sparkHome = sparkHome, pyFiles=pyFiles, serializer=AutoSerializer())
        # sc = SparkContext(master, app_name, sparkHome = sparkHome, pyFiles=pyFiles, serializer=CompressedSerializer(PickleSerializer()))

        spark_conf = SparkConf()
        spark_conf.setMaster(master).setAppName(app_name).setSparkHome(spark_home)

        # spark streaming 调优配置
        spark_streaming_blockInterval = str(app_conf.get('spark.streaming.blockInterval', '')).strip()
        if spark_streaming_blockInterval:
            spark_conf.set('spark.streaming.blockInterval', spark_streaming_blockInterval)

        spark_streaming_kafka_maxRatePerPartition = str(
            app_conf.get('spark.streaming.kafka.maxRatePerPartition', '')).strip()
        if spark_streaming_kafka_maxRatePerPartition:
            spark_conf.set('spark.streaming.kafka.maxRatePerPartition', spark_streaming_kafka_maxRatePerPartition)

        spark_streaming_receiver_maxRate = str(app_conf.get('spark.streaming.receiver.maxRate', '')).strip()
        if spark_streaming_receiver_maxRate:
            spark_conf.set('spark.streaming.receiver.maxRate', spark_streaming_receiver_maxRate)

        spark_streaming_concurrentJobs = str(app_conf.get('spark.streaming.concurrentJobs', '')).strip()
        if spark_streaming_concurrentJobs:
            spark_conf.set('spark.streaming.concurrentJobs', spark_streaming_concurrentJobs)

        # spark sql 调优配置
        spark_sql_shuffle_partitions = str(app_conf.get('spark.sql.shuffle.partitions', '')).strip()
        if spark_sql_shuffle_partitions:
            spark_conf.set('spark.sql.shuffle.partitions', spark_sql_shuffle_partitions)

        sc = SparkContext(conf=spark_conf)
        for path in (pyFiles or []):
            sc.addPyFile(path)

        # 外部缓存优化，broadcast 分发
        cache_manager = CacheManager()
        cache_broadcast_list = \
            [(cache_id, cache_manager.cache_dataset(sc, cache_conf))
             for cache_id, cache_conf in cache_confs_with_ds_conf.iteritems()
             if cache_conf.get('broadcast.enabled', False)]

        for cache_id, cache_broadcast in cache_broadcast_list:
            cache_confs_with_ds_conf[cache_id]['broadcast'] = cache_broadcast

        batchDruationSeconds = app_conf['batchDuration.seconds']
        ssc = StreamingContext(sc, batchDruationSeconds)
        sqlc = SQLContext(sc)

        # 读取数据源
        stream = StreamingReader.readSource(ssc, di_in_conf_with_ds_conf, app_conf)
        # 流处理： 1 根据配置初始化处理指定数据接口的类的实例， 2 调用指定处理类实例的流数据处理方法
        # 测试 kafka_wordcount
        # counts = stream.flatMap(lambda line: line.split(" ")) \
        # .map(lambda word: (word, 1)) \
        # .reduceByKey(lambda a, b: a+b)
        # counts.pprint()
        StreamingApp.process(
            stream, sc, sqlc,
            di_in_conf_with_ds_conf, di_out_confs_with_ds_conf, cache_confs_with_ds_conf,
            prepares_config_active_steps, compute_prepares_config_active, compute_computes_config_active)

        ssc.start()
        ssc.awaitTermination()


if __name__ == '__main__':
    main()
