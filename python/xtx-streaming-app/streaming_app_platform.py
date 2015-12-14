# coding: utf-8

import os
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

# For spark
sys.path.append('/data01/app/bigdata/spark/python')
sys.path.append('/data01/app/bigdata/spark/python/lib/py4j-0.8.2.1-src.zip')
# if 'SPARK_HOME' not in os.environ:
#     os.environ['SPARK_HOME'] = '/data01/app/bigdata/spark'
# SPARK_HOME = os.environ['SPARK_HOME']

# For elasticsearch, mongo
# sys.path.append('/usr/local/lib/python2.7/site-packages')
sys.path.append('/data01/data/datadir_python_3rd_pacakge/usr/local/lib/python2.7/site-packages')


from pyspark.streaming.kafka import KafkaUtils
from pyspark import RDD

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *   # SyntaxError: import * is not allowed in function 'process_step' because it is contains a nested function with free variables
from pprint import pprint

from streaming_app_utils import *
from es_utils import *
from mysql_utils import MySQLUtils


class StreamingRDDRule(object):
    def __init__(self):
        self.rule_id = None
        self.conf = {}
        self.rule_type = {}
        self.rule_method = {}
        self.enabled = False
        self.cache_conf = {}
        self.output_conf = {}

        self.record_rules = []

    def init(self, rule_id, conf, cache_conf, output_conf):
        self.rule_id = rule_id
        self.conf = conf
        if isinstance(conf, dict) and len(dict) != 0:
            self.rule_type = conf['step.type']
            self.rule_method = conf['step.method']
            self.enabled = conf.get('enabled', False)

            record_rule_clzes = [get_class_obj(clz)()
                                 for clz in conf.get('batchProcessor.class.list', '').split(",") if clz.strip()]

        if isinstance(cache_conf, dict):
            self.cache_conf = cache_conf




    pass


class StreamingApp(object):
    def __init__(self):
        pass

    @staticmethod
    def fun_prepares_in_rdd_transform(
            dataset, sc, sqlc,
            di_in_conf_with_ds_conf, di_out_confs_with_ds_conf, cache_confs_with_ds_conf,
            prepares_config_active_steps):
        """
        输入： dataset 是 rdd[Row], StreamingReader.readSource 后，格式化后的 DStream 类型 DStream[Row], 其中 RDD类型 RDD[Row]
        输出: RDD[Row]
        """
        struct_type = di_in_conf_with_ds_conf.get('struct.type')

        # Note 不能判断数据是否为空, 另外python api 中 sqlc.createDataFrame(rdd, schema) 如果没有指定 schema，context 调用 _inferSchema 会调用 rdd.first() 触发job
        # df = sqlc.createDataFrame(dataset, struct_type)

        # 方式1,  使用 json 生成 df 的目的，可以用于判断数据是否为空
        # import json
        # df = sqlc.jsonRDD(dataset)
        # df = sqlc.jsonRDD(dataset.map(lambda row: json.dumps(row.asDict())))

        # 方式2
        # if len(prepares_config_active_steps) == 0 or len(df.schema.fields) == 0:
        if len(prepares_config_active_steps) == 0 or dataset.isEmpty():
            ret = dataset
        else:
            result_data_step = dataset
            result_schema_step = struct_type
            for step_id, step_conf in prepares_config_active_steps:
                phase_step = 'di.id[{0}]-prepares.step[{1}]'.format(
                    str(di_in_conf_with_ds_conf['interface.id']), str(step_id))
                # Note: schema 经过 rdd transform操作后，可能会变，
                # spark-sql 类型处理规则，返回的对象是 df，df.schema 和 df 对应
                # plugin 类型处理规则，返回的对象是 RDD[Row], schema 应该有配置中的 schema 指定

                (ret_data, ret_schema) = StreamingApp.process_step(
                    result_data_step, result_schema_step, sqlc,
                    step_conf, cache_confs_with_ds_conf, di_out_confs_with_ds_conf,
                    phase_step=phase_step)

                result_data_step = ret_data
                result_schema_step = ret_schema

            print('= = ' * 20 + '[myapp StreamingApp.process.fun_prepares_in_rdd_transform] '
                                'type(result_data) = ', type(result_data_step))
            if issubclass(type(dataset), RDD):
                ret = result_data_step
            elif isinstance(result_data_step, DataFrame):
                ret = result_data_step.rdd
            else:
                raise Exception('= = ' * 20 + '[myapp StreamingApp.process.fun_prepares_in_rdd_transform] '
                                              'found invalid ret_data from StreamingApp.process_step() '
                                              'with phase_step = ' + 'di.id[{0}]-prepares]'.format(str(di_in_conf_with_ds_conf['interface.id'])))

        return ret

    @staticmethod
    def fun_compute_prepares_in_dstream_transform(
            dataset, sc, sqlc,
            di_in_conf_with_ds_conf, di_out_confs_with_ds_conf, cache_confs_with_ds_conf,
            computeStatisticId, compute_prepare_step_conf_list):
        """
        输入: RDD[Row]
        输出: RDD[Row]
        """
        import json
        # 方式1
        # 解决：Python Api 如果不判断数据是否为空 schmea 为空, df = sqlc.createDataFrame(dataset, None) 会报错的问题： ValueError: RDD is empty
        # import json
        # df = sqlc.jsonRDD(dataset.map(lambda row: json.dumps(row.asDict())))
        # schema = df.schema

        # 方式2：
        # if len(schema.fields) == 0 or len(compute_prepare_step_conf_list) == 0:
        if dataset.isEmpty() or len(compute_prepare_step_conf_list) == 0:
            ret = dataset
        else:
            # df = sqlc.createDataFrame(dataset, None)  #raise ValueError("Some of types cannot be determined by the "; ValueError: Some of types cannot be determined by the first 100 rows, please try again with sampling
            # df = sqlc.createDataFrame(dataset)   # 方式1 #raise ValueError("Some of types cannot be determined by the "; ValueError: Some of types cannot be determined by the first 100 rows, please try again with sampling
            df = sqlc.jsonRDD(dataset.map(lambda row: json.dumps(row.asDict())))  # 方式1： ok
            schema = df.schema

            result_data_step = dataset
            result_schema_step = schema

            ret = None
            for step_id, step_conf in compute_prepare_step_conf_list:
                phase_step = 'di.id[{0}]-computeStatistic.id[{1}]-prepares-step[{2}]'.format(
                    str(di_in_conf_with_ds_conf['interface.id']), str(computeStatisticId), str(step_id))
                print('= = ' * 10, '[myapp StreamingApp.process] start processing phase_step = ' + phase_step)

                # TODO: 问题： Python Api 如果不判断数据是否为空 schmea 为空, df = sqlc.createDataFrame(dataset, p_schema) 会报错： ValueError: RDD is empty
                (ret_data, ret_schema) = StreamingApp.process_step(
                    result_data_step, result_schema_step, sqlc,
                    step_conf, cache_confs_with_ds_conf, di_out_confs_with_ds_conf,
                    phase_step=phase_step)

                result_data_step = ret_data
                result_schema_step = ret_schema

                if issubclass(type(result_data_step), RDD):
                    ret = result_data_step
                elif isinstance(result_data_step, DataFrame):
                    # res = result_data.toJSON()
                    ret = result_data_step.rdd
                else:
                    raise Exception('= = ' * 20 + '[myapp StreamingApp.process.fun_compute_prepares_in_dstream_transform] '
                                                  'found invalid ret_data from StreamingApp.process_step() '
                                                  'with phase_step = ' + phase_step)
                print('= = ' * 10, '[myapp StreamingApp.process] finish process phase_step = ' + phase_step)

        return ret

    @staticmethod
    def fun_compute_computes_in_dstream_foreachRDD(
            rdd, sc, sqlc,
            di_in_conf_with_ds_conf, di_out_confs_with_ds_conf, cache_confs_with_ds_conf,
            computeStatisticId, compute_computes_step_conf_list):
        """
        输入: RDD[Row]
        输出: RDD[obj]
        Note: dstream.foreachRDD: driver 端每个批次都会执行
        """
        # 方式1
        # 解决：Python Api 如果不判断数据是否为空 schmea 为空, df = sqlc.createDataFrame(rdd, p_schema) 会报错的问题： ValueError: RDD is empty
        # try:
        #     import json
        #     df = sqlc.jsonRDD(rdd.map(lambda row: json.dumps(row.asDict())))
        # except Exception, ex:
        #     rdd.foreach(fun_print_in_rdd_foreach)
        #     print('= = ' * 4, '[myapp StreamingApp.process.fun_compute_computes_in_dstream_foreachRDD] '
        #                       'error while df = sqlc.jsonRDD')
        #     raise ex
        # rdd_schema = df.schema

        # 方式2
        # if len(rdd_schema.fields) == 0:
        if rdd.isEmpty():
            print '= = ' * 4, '[myapp StreamingApp.process.fun_compute_computes_in_dstream_foreachRDD] ' \
                              'found empty dataframe after compute_prepare in computeStatisticId = ', computeStatisticId
        else:
            # df = sqlc.createDataFrame(rdd, None)  # 问题：发现有时会报错：raise ValueError("Some of types cannot be determined by the "， ValueError: Some of types cannot be determined by the first 100 rows, please try again with sampling
            import json
            df = sqlc.jsonRDD(rdd.map(lambda row: json.dumps(row.asDict())))
            rdd_schema = df.schema

            compute_step_rdd_list = []
            for step_id, step_conf in compute_computes_step_conf_list:
                phase_step = 'di.id[{0}]-computeStatistic.id[{1}]-computes.step.id[{2}]'.format(
                    str(di_in_conf_with_ds_conf['interface.id']), str(computeStatisticId), str(step_id))
                (ret_data, ret_schema) = StreamingApp.process_step(
                    df, rdd_schema, sqlc,
                    step_conf, cache_confs_with_ds_conf, di_out_confs_with_ds_conf,
                    phase_step=phase_step)
                compute_step_rdd_list.append(ret_data)

            # result = reduce(lambda rddx, rddy: rddx.union(rddy), compute_step_rdd_list)
            result = sc.union(compute_step_rdd_list)

            # TODO:调试内存
            if not result.is_cached:
                result.persist()

            # 调整es写入方式
            # fun_output_in_rdd_mapPartitions： executor 写ES
            # result.count()  # 触发job

            # fun_output_in_rdd_mapPartitions_new： driver 写ES
            output_list = result.collect()
            ESWriter.driver_output(output_list)

            # TODO:调试内存
            if result.is_cached:
                result.unpersist()

    # 处理流程入口
    @staticmethod
    def process(dstream, sc, sqlc,
                di_in_conf_with_ds_conf, di_out_confs_with_ds_conf, cache_confs_with_ds_conf,
                prepares_config_active_steps, compute_prepares_config_active, compute_computes_config_active):

        # TODO: 内存批次间排重
        if di_in_conf_with_ds_conf.get('type') == 'json':
            dstream2 = dstream
        else:
            raise Exception("Error: format.class of configuration in dataInterfaces is subclass of StreamingFormater, "
                            "should return format of json string!!!")

        # 准备阶段有效配置为空，取原始数据流；不为空时，根据配置执行相关步骤
        if len(prepares_config_active_steps) == 0:
            dstream_after_prepares = dstream2
        else:
            dstream_after_prepares = dstream2.transform(
                lambda rdd: StreamingApp.fun_prepares_in_rdd_transform(
                    rdd, sc, sqlc,
                    di_in_conf_with_ds_conf, di_out_confs_with_ds_conf, cache_confs_with_ds_conf,
                    prepares_config_active_steps)
            )

        dstream_after_prepares.cache()

        # 没有配置有效的计算，不处罚job
        # 指定接口的计算配置，又分2个阶段，准备阶段、计算阶段
        # compute_computes_config_active: {computeStatistic.id -> list[step_conf_tuple]}, 其中 step_conf_tuple = (step_id, step_conf_dict)
        # compute_computes_config_active: {computeStatistic.id -> list[step_conf_tuple]}, 其中 step_conf_tuple = (step_id, step_conf_dict)
        if len(compute_computes_config_active) == 0:
            dstream_after_prepares.foreachRDD(lambda rdd: rdd)
        else:  # 存在有效的计算配置
            # 此处没有选择直接使用 DSteam.foreachRDD，是之前考虑实现批次间排重需要 Dstream 的 join
            id2streams = {}
            for computeStatisticId in compute_computes_config_active:
                compute_prepares_step_conf_list = compute_prepares_config_active.get(computeStatisticId)
                if compute_prepares_step_conf_list is not None:
                    if len(compute_prepares_step_conf_list) == 0:
                        id2streams[computeStatisticId] = dstream_after_prepares
                    else:
                        dstream_after_compute_prepares = dstream_after_prepares.transform(
                            lambda rdd: StreamingApp.fun_compute_prepares_in_dstream_transform(
                                rdd, sc, sqlc,
                                di_in_conf_with_ds_conf, di_out_confs_with_ds_conf, cache_confs_with_ds_conf,
                                computeStatisticId, compute_prepares_step_conf_list))
                        id2streams[computeStatisticId] = dstream_after_compute_prepares
                else:
                    id2streams[computeStatisticId] = dstream_after_prepares

            for computeStatisticId, stream in id2streams.iteritems():
                compute_computes_step_conf_list = compute_computes_config_active[computeStatisticId]
                if len(compute_computes_step_conf_list) == 0:  # 没有有效的计算配置，不执行 rdd 的 action 操作，不触发 job
                    stream.foreachRDD(lambda rdd: rdd)
                else:
                    if not stream.is_cached:
                        stream.cache()
                    stream.foreachRDD(
                        # TODO: 删除测试
                        # lambda rdd: rdd.foreach(fun_print_in_rdd_foreach)
                        lambda rdd: StreamingApp.fun_compute_computes_in_dstream_foreachRDD(
                            rdd, sc, sqlc,
                            di_in_conf_with_ds_conf, di_out_confs_with_ds_conf, cache_confs_with_ds_conf,
                            computeStatisticId, compute_computes_step_conf_list)
                    )


    @staticmethod
    def process_step(dataset, p_schema, sqlc,
                     step_conf, cache_confs_with_ds_conf, di_out_confs_with_ds_conf, phase_step=''):
        from pyspark.sql.types import StructType
        print('= = ' * 10, '[myapp StreamingApp.process_step] start processing phase_step = ' + phase_step)

        step_type = step_conf['step.type']
        step_method = step_conf['step.method']

        step_cache_id = step_conf.get('cacheId', -1)
        assert isinstance(step_cache_id, int), 'step_cache_id should be int in phase_step = ' + phase_step
        print('= = ' * 20, '[myapp StreamingApp.process_step] step_cache_id = ', step_cache_id,
              ', cache_confs_with_ds_conf.keys = ', cache_confs_with_ds_conf.keys())
        cache_conf = cache_confs_with_ds_conf.get(step_cache_id, {})

        if step_type == 'enhance' or step_type == 'filter':
            if step_method == 'spark-sql':
                if issubclass(type(dataset), RDD):
                    # df = sqlc.jsonRDD(dataset, p_schema)
                    df = sqlc.createDataFrame(dataset, p_schema)
                elif isinstance(dataset, DataFrame):
                    df = dataset
                else:
                    raise Exception('[myapp StreamingApp.process_step.enhance.spark-sql] Error: '
                                    'found invalid type dataset in phase_step = ' + phase_step)

                print('= = ' * 8, '[myapp StreamingApp.process_step.enhance.spark-sql] schema = ')
                df.printSchema()

                select_expr_clause = [col.strip() for col in step_conf.get('selectExprClause', '').split(',')
                                      if col.strip()]
                where_clause = step_conf.get('whereClause', '')

                if select_expr_clause and where_clause:
                    df2 = df.selectExpr(select_expr_clause).filter(where_clause)
                elif select_expr_clause and (not where_clause):
                    df2 = df.selectExpr(select_expr_clause)
                elif (not select_expr_clause) and where_clause:
                    df2 = df.filter(where_clause)
                else:
                    df2 = df

                print('= = ' * 10, '[myapp myapp StreamingApp.process_step] finish process phase_step = ' + phase_step)

                # note，此处返回的 schema 与 df.rdd 的顺序不同，需要排序
                # return df2, df2.schema
                schema_new = StructType(sorted([x for x in df2.schema.fields], key=lambda x: x.name))
                return df2, schema_new

            elif step_method == 'plugin':
                # plugin 类型处理规则，可能增减字段，返回的对象是 RDD[Row], schema 应该使用配置中的 schema 指定

                class_path = step_conf.get('class', '')
                assert class_path.strip() != '', \
                    '[myapp StreamingApp.process_step] configuration Error: found invalid class in phase_step = ' \
                    + phase_step
                output_plugin_class = get_class_obj(class_path)

                # TODO: 是用实例方法还是类方法比较好？都可以
                # output_plugin_instance = output_plugin_class()
                print('= = ' * 10, '[myapp StreamingApp.process_step.enhance.plugin] type(dataset) = ', type(dataset),
                      'class of dataset is subClass of RDD = ', issubclass(type(dataset), RDD))
                if issubclass(type(dataset), RDD):
                    # result_plugin = output_plugin_instance.process(dataset, step_conf, cache_conf)
                    (result_plugin, schema_new) = output_plugin_class.process(dataset, p_schema, step_conf, cache_conf)

                elif isinstance(dataset, DataFrame):
                    # result_plugin = output_plugin_instance.process(dataset.toJSON(), step_conf, cache_conf)
                    # result_plugin = output_plugin_class.process(dataset.toJSON(), step_conf, cache_conf)
                    (result_plugin, schema_new) = output_plugin_class.process(
                        dataset.rdd, p_schema, step_conf, cache_conf)
                else:
                    raise Exception('[myapp StreamingApp.process_step.enhance.plugin] error found invalid dataset with '
                                    ' type = ' + dataset.__class__.__name__)

                print('= = ' * 10, '[myapp myapp StreamingApp.process_step] finish process phase_step = '
                      + phase_step + ', schema_new = ')
                pprint(schema_new.fields)

                return result_plugin, schema_new
            else:
                raise Exception('[myapp StreamingApp.process_step] configuration Error: found invalid step_method = '
                                + step_method)

        elif step_type == 'compute':
            # Note: dstream.forearchRDD 的一部分，每个批次生成 job 的过程都会执行一次
            # Note: 执行前能保证数据不为空, 目前输入的是 sqlc.jsonRDD() 生成的 df,
            if step_method == 'spark-sql':
                if issubclass(type(dataset), RDD):
                    import json
                    # df = sqlc.jsonRDD(dataset.map(lambda row: json.dumps(row.asDict())))
                    df = sqlc.createDataFrame(dataset, p_schema)
                    schema = df.schema
                    rdd = dataset
                elif isinstance(dataset, DataFrame):
                    df = dataset
                    schema = p_schema
                    rdd = dataset.rdd
                else:
                    raise Exception('[myapp StreamingApp.process_step.compute.spark-sql] Error: '
                                    'found invalid type dataset with phase_step = ' + phase_step)

                print('= = ' * 8, '[myapp StreamingApp.process_step.compute.spark-sql] df.schema = ')
                df.printSchema()
                # TODO: 删除测试
                df.show()

                if rdd.isEmpty():
                    print('= = ' * 8, '[myapp StreamingApp.process_step] found empty dataframe in phase_step = '
                          + phase_step)
                    # TODO: yangfuqing@xuetangx.com, spark-1.4.0 python api 中没有 context.emptyRDD()，不能创建0个分区的RDD

                    print('= = ' * 8, '[myapp myapp StreamingApp.process_step] '
                                       'finish process phase_step = ' + phase_step)
                    return rdd, schema  # Note: rdd 的分区数大于0，数据为空
                    # return sc.parallelize([], 1), schema
                    # TODO: yangfuqing@xuetangx.com, 更新到 spark-1.4.1，推荐使用
                    # return sc.emptyRDD(), schema
                else:
                    select_expr_clause = [col for col in step_conf.get('selectExprClause', '').split(',')
                                          if col.strip()]
                    where_clause = step_conf.get('whereClause', '')

                    if select_expr_clause and where_clause:
                        df2 = df.selectExpr(select_expr_clause).filter(where_clause)
                    elif select_expr_clause and (not where_clause):
                        df2 = df.selectExpr(select_expr_clause)
                    elif (not select_expr_clause) and where_clause:
                        df2 = df.filter(where_clause)
                    else:
                        df2 = df
                    print('= = ' * 8, '[myapp StreamingApp.process_step.compute.spark-sql] df2.schema = ')
                    df2.printSchema()
                    df2.show()

                    if df2.rdd.isEmpty():
                        result = df2.rdd
                    else:
                        # TODO: 优化点，如果能判断没有数据，可以不用处理后面的数据
                        statisticKeyMap = step_conf.get('statisticKeyMap', {})
                        targetKeysList = step_conf.get('targetKeysList', '')
                        aggregateKeyMethodList = [tuple(key_method.strip().split(':')[0:2])
                                                  for key_method in step_conf['aggegate.key.method.list']
                                                  if key_method.strip() != '' and (':' in key_method)]

                        DEFAULT_GROUPBY_NONE_VALUE = 'NONE'
                        GROUPBY_NONE_VALUE = step_conf.get('groupby.none.key', DEFAULT_GROUPBY_NONE_VALUE)
                        if GROUPBY_NONE_VALUE.strip() == '':
                            GROUPBY_NONE_VALUE = DEFAULT_GROUPBY_NONE_VALUE

                        output_class_path = step_conf.get('output.class', 'streaming_app_platform.ConsolePrinter')
                        output_plugin_class = get_class_obj(output_class_path)
                        # output_plugin_instance = output_plugin_class()

                        di_output_id = step_conf.get('output.dataInterfaceId')
                        assert di_output_id is not None and isinstance(di_output_id, int), \
                            '[myapp StreamingApp.process_step] configuration error invalid output.dataInterfaceId' \
                            ' in phase_step = ' + phase_step

                        di_output_conf = di_out_confs_with_ds_conf.get(di_output_id, {})

                        default_data_type = step_conf.get('data.type', '')
                        default_value_type = step_conf.get('value.type', '')
                        default_value_cycle = step_conf.get('value.cycle', '')
                        exclude_key_set = set([x for x in (step_conf.get('statistic.keyLabel.excludes', '').split(','))
                                               if x.strip() != ''])

                        # targetKeys_output_rdd_list = []
                        rdd_stat_output_list = []
                        for targetKeys in targetKeysList.split(','):
                            targetKeys_dt_vt_list = targetKeys.split("#")
                            keyLevelListStr = targetKeys_dt_vt_list[0]
                            data_type = default_data_type if len(targetKeys_dt_vt_list) == 1 else targetKeys_dt_vt_list[1]
                            value_type = targetKeys_dt_vt_list[2] if len(targetKeys_dt_vt_list) > 2 else default_value_type
                            value_cycle = targetKeys_dt_vt_list[3] if len(targetKeys_dt_vt_list) > 3 else default_value_cycle

                            keyLevelList = [x.strip() for x in keyLevelListStr.split(':')]
                            groupByKeyArr = [GROUPBY_NONE_VALUE if x == DEFAULT_GROUPBY_NONE_VALUE else statisticKeyMap[x]
                                             for x in keyLevelList]

                            groupByKeyArr_without_none_value = [x for x in groupByKeyArr if x != GROUPBY_NONE_VALUE]

                            if len(exclude_key_set) == 0:
                                keyLevelList2 = keyLevelList
                            else:
                                keyLevelList2 = [x for x in keyLevelList if not (x in exclude_key_set)]

                            groupByKeyArr2 = [GROUPBY_NONE_VALUE if x == DEFAULT_GROUPBY_NONE_VALUE else statisticKeyMap[x]
                                              for x in keyLevelList2]
                            dict_target = {
                                'origin.statistic.key': ','.join(groupByKeyArr),
                                'origin.statistic.keyLevel': keyLevelListStr,
                                'statistic.key': ','.join(keyLevelList2) if len(keyLevelList2) > 0 else 'L0',
                                'statistic.keyName': ','.join(groupByKeyArr2) if len(groupByKeyArr2) > 0 else 'NONE',
                                'statistic.data_type': data_type,
                                'statistic.value_type': value_type,
                                'statistic.value_cycle': value_cycle
                            }
                            pre_output_conf = dict_merge(di_output_conf, dict_target, update_flag=True)  # Note: dict_merge 默认并不覆盖

                            for agg_key, agg_method in aggregateKeyMethodList:
                                if agg_method == 'count':
                                    if len(groupByKeyArr_without_none_value) == 0:
                                        # rdd_stat = df2.agg(count(agg_key).alias('value')).toJSON()
                                        df_stat = df2.agg(count(agg_key).alias('value'))
                                    else:
                                        # rdd_stat = df2.groupBy(groupByKeyArr_without_none_value).agg(count(agg_key).alias('value')).toJSON()
                                        df_stat = df2.groupBy(groupByKeyArr_without_none_value).agg(count(agg_key).alias('value'))
                                elif agg_method == 'countDistinct':
                                    if len(groupByKeyArr_without_none_value) == 0:
                                        # rdd_stat = df2.agg(countDistinct(agg_key).alias('value')).toJSON()
                                        df_stat = df2.agg(countDistinct(agg_key).alias('value'))
                                    else:
                                        # rdd_stat = df2.groupBy(groupByKeyArr_without_none_value).agg(countDistinct(agg_key).alias('value')).toJSON()
                                        df_stat = df2.groupBy(groupByKeyArr_without_none_value).agg(countDistinct(agg_key).alias('value'))
                                else:
                                    raise Exception('[myapp StreamingApp.process_step] configuration error: '
                                                    'unsupport agg_method = ' + agg_method)
                                print('= = ' * 10, '[myapp myapp StreamingApp.process_step.df_stat.show]  phase_step = '
                                      + phase_step, 'with keyLevelListStr = ' + keyLevelListStr)
                                df_stat.show()
                                rdd_stat = df_stat.rdd
                                rdd_stat_output = output_plugin_class.output(rdd_stat, step_conf, pre_output_conf)
                                rdd_stat_output_list.append(rdd_stat_output)

                        result = reduce(lambda rddx, rddy: rddx.union(rddy), rdd_stat_output_list)

                    print('= = ' * 10, '[myapp myapp StreamingApp.process_step] finish process phase_step = '
                          + phase_step)

                    return result, None
            elif step_method == 'plugin':
                # TODO: 支持插件方式实现统计指标计算，优先级低
                raise Exception('[myapp StreamingApp.process_step.compute] configuration Warning: '
                                'does not support compute statistics using plugin class')
            else:
                raise Exception('[myapp configuration] Error: found invalid step_method = ' + step_method)
        else:
            raise Exception('[myapp configuration] Error: found invalid step_type = ' + step_type)


class StreamingReader(object):
    def __init__(self):
        pass

    @staticmethod
    def readSource(ssc, di_in_conf_with_ds_conf, app_conf):
        sourceType = di_in_conf_with_ds_conf['source.type']

        if sourceType == 'kafka':
            kafkaSimpleConsumerApiUsed = app_conf.get('kafka.simple.consumer.api.used', True)
            if kafkaSimpleConsumerApiUsed:
                topics = di_in_conf_with_ds_conf['topics']
                if not isinstance(topics, list):
                    raise TypeError("topic should be list")

                brokers = di_in_conf_with_ds_conf['metadata.broker.list']
                kafkaParams = {"metadata.broker.list": brokers}
                stream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams).map(lambda x: x[1])
            else:
                zkConnect = di_in_conf_with_ds_conf['zookeeper.connect']
                groupId = app_conf['group.id']
                numReceivers = app_conf.get('num.receivers', 1)
                numConsumerFetchers = app_conf.get('num.consumer.fetchers')
                topics = di_in_conf_with_ds_conf['topics']
                topic_map = dict(zip(topics, numConsumerFetchers))
                # streams = reduce(lambda x, y: x.union(y),
                #                  map(KafkaUtils.createStream(ssc, zkConnect, groupId, topic_map),
                #                      range(0, numReceivers)))
                streams = [KafkaUtils.createStream(ssc, zkConnect, groupId, topic_map) for i in range(0, numReceivers)]
                stream = ssc.union(streams).map(lambda x: x[1])
        elif sourceType == 'hdfs':
            path = di_in_conf_with_ds_conf['fs.defaultFS'] + '/' + di_in_conf_with_ds_conf['path']
            stream = ssc.textFilesStream(path)
        else:
            raise Exception('Error: unsupported source.type = ' + sourceType)

        num_repartition = app_conf.get('dataInterface.stream.repatition.partitions')
        if num_repartition is None or not isinstance(num_repartition, int):
            stream2 = stream
        else:
            stream2 = stream.repartition(num_repartition)

        # 是否使用格式化插件类格式化
        format_class_path = di_in_conf_with_ds_conf.get('format.class', '')
        if format_class_path.strip() == '':
            stream3 = stream2
        else:
            format_class_obj = get_class_obj(format_class_path)
            stream3 = format_class_obj.format(stream2)

        return stream3


# class StreamingFormater():
#     def __init__(self):
#         pass
#
#     @staticmethod
#     def format(dstream):
#         return dstream


class StreamingProcessor(object):
    def __init__(self):
        pass

    @staticmethod
    def process(rdd, step_conf, cache_conf=None):
        return rdd

    @staticmethod
    def compute(rdd, step_conf, cache_conf=None):
        return rdd

    @staticmethod
    def output(rdd, step_conf):
        return rdd

    @staticmethod
    def pre_output(rdd, step_conf):
        return rdd


class ConsolePrinter(object):
    @staticmethod
    def fun_print_in_rdd_mapPartitions(iter_x):
        for row in iter_x:
            print('= = ' * 10 + '[myapp ConsolePrinter.output.fun_print_in_rdd_mapPartitions] row = ', row)
            yield row

    @staticmethod
    def output(rdd, step_conf, di_output_conf):
        return rdd.mapPartitions(ConsolePrinter.fun_print_in_rdd_mapPartitions)

    @staticmethod
    def pre_output(rdd, step_conf):
        return rdd


class CacheManager(object):
    def __init__(self):
        self.cache_pools = {}

    def cache_dataset(self, sc, cache_conf):
        broadcast_enabled = cache_conf.get('broadcast.enabled', False)

        if isinstance(broadcast_enabled, bool) and broadcast_enabled:
            # cache_id = 'ds_id_' + cache_conf['source.id'] + '#cache_id_' + cache_conf['cache.id']
            cache_id = 'ds_id_' + str(cache_conf['source.id']) + "#cache_id_" + str(cache_conf['cache.id'])

            if cache_id in self.cache_pools:
                print('= = ' * 10, '[myapp CacheManager.cache_dataset] found dataset has been cached')
                return self.cache_pools[cache_id]
            else:
                host = cache_conf['host']
                port = cache_conf.get('port', 3306)
                db = cache_conf['db']
                user = cache_conf['user']
                password = cache_conf.get('password', '')

                table_name = cache_conf['tableName']
                key_name = cache_conf['keyName']
                cache_key_name_list = cache_conf['cache.keyName.list']

                cache_sql = 'select ' + key_name + ', ' + cache_key_name_list + ' from ' + db + '.' + table_name
                conn = MySQLUtils.get_connection(host=host, db=db, user=user, password=password, port=port)
                external_cache = MySQLUtils.query(conn, sql_text=cache_sql, key_name=key_name)

                cache_broadcast = sc.broadcast(external_cache)

                self.cache_pools[cache_id] = cache_broadcast
                return cache_broadcast
        else:
            print('= = ' * 10, '[myapp CacheManager.cache_dataset] '
                               'configuration warning: found cache is not enabled, with broadcast.enabled = ',
                  broadcast_enabled)
            return None
