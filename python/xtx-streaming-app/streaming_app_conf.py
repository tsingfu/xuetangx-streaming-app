# -*- coding: utf-8 -*-

APP_CONFIG = {
    # key = app.id
    1: {
        'app.id': 1,
        # 'app.name': 'app1',
        'app.interfaceId': 1,
        'properties': {
            'batchDuration.seconds': 15,
            'kafka.simple.consumer.api.used': True,
            # 'kafka.consumer.group.id': '',
            # 'spark.streaming.kafka.maxRatePerPartition': '',
            # 'spark.streaming.kafka.maxRetries': '',
            # 'dataInterface.stream.repatition.partitions': '',
            # 'checkpointDir': '',
            # 'spark.streaming.blockInterval': '',
            # 'spark.streaming.receiver.maxRate': '',
            # 'spark.streaming.concurrentJobs': '',
            'spark.sql.shuffle.partitions': 5,
            'sparkHome':'/data01/app/bigdata/spark',
            'pyFiles.list': [
                'streaming_app_main.py',
                'streaming_app_conf.py',
                'streaming_app_platform.py',
                'streaming_app_main.py',
                'streaming_app_utils.py',
                'format_track.py',
                'es_utils.py',
                'mysql_utils.py',
                'mongo_utils.py'
            ]
        # ['file:///data01/data/datadir_python/projects/sparkTest/xtx-streaming-app/core/streaming_app_main.py']
        # : org.apache.spark.SparkException: Added file file:/Volumes/2/data/datadir_python/projects/sparkTest/xtx-streaming-app/core is a directory and recursive is not turned on.
        }
    }
}


DATASOURCE_CONFIG = {
    # key = source.id
    1: {
        'source.id': 1,
        'source.type': 'kafka',
        'source.name': 'ds_kafka1',
        'properties': {
            # 'zookeeper.connect': '10.0.0.193:2181,10.0.0.194:2181,10.0.0.195:2181',
            'zookeeper.connect': 'localhost:2181',
            # 'metadata.broker.list': '10.0.0.193:9092,10.0.0.194:9092,10.0.0.195:9092'
            'metadata.broker.list': 'localhost:9092'
        }
    },
    2: {
        'source.id': 2,
        'source.type': 'jdbc',
        'source.name': 'ds_mysqldb1',
        'properties': {
            # 'driver': 'com.mysql.jdbc.Driver',
            # # 'url': 'jdbc:mysql://10.0.0.116:3306/edxapp',
            # # 'user': 'mysql_ro',
            # # 'password': 'xuetangx.com168mysql'
            # 'url': 'jdbc:mysql://192.168.9.228:3306/edxapp',
            'host': '10.0.0.168',
            'port': 3306,
            'db': 'edxapp',
            'user': 'mysql_ro',
            'password': 'xuetangx.com168mysql'
        }
    },
    3: {
        'source.id': 3,
        'source.type': 'mongo',
        'source.name': 'ds_mongodb1',
        'properties': {
            'mongo.connection.url': 'mongodb://10.0.0.198:27017,10.0.0.233:27017,10.0.0.234:27017'
        }
    },
    4: {
        'source.id': 4,
        'source.type': 'elasticsearch',
        'source.name': 'ds_es1',
        'properties': {
            # 'serverPort.list': '10.0.0.227:9300,10.0.0.228:9300,10.0.0.236:9300',
            'serverPort.list': '10.0.0.227:9200,10.0.0.228:9200,10.0.0.236:9200',
            'cluster.name': 'xuetang_escluster'
        }
    },
    # 以下是测试环境
    21: {
        'source.id': 21,
        'source.type': 'jdbc',
        'source.name': 'ds_mysqldb21',
        'properties': {
            # 'driver': 'com.mysql.jdbc.Driver',
            # 'url': 'jdbc:mysql://192.168.9.228:3306/edxapp',
            'host': '192.168.9.228',
            'port': 3306,
            'db': 'edxapp',
            'user': 'root',
            'password': ''
        }
    },
    31: {
        'source.id': 31,
        'source.type': 'mongo',
        'source.name': 'ds_mongodb31',
        'properties': {
            'mongo.connection.url': 'mongodb://192.168.9.164:27017'
        }
    },
    41: {
        'source.id': 41,
        'source.type': 'elasticsearch',
        'source.name': 'ds_es41',
        'properties': {
            # 'serverPort.list': '192.168.9.164:9300',
            'serverPort.list': '192.168.9.164:9200',
            'cluster.name': 'elasticsearch'
        }
    },
    42: {
        'source.id': 42,
        'source.type': 'elasticsearch',
        'source.name': 'ds_es42',
        'properties': {
            # 'serverPort.list': 'monitor1:9300',
            'serverPort.list': 'monitor1:9200',
            'cluster.name': 'ES_Cluster1'
        }
    }
}

DATAINTERFACE_CONFIG = {
    # key = interface.id
    1: {
        'interface.id': 1,
        'interface.type': 'input',
        'interface.sourceId': 1,
        'interface.name': 'di_kafka1',
        'properties': {
            # 'topics': ['platformlog','vpclog'],
            'topics': ['topic1-platformlog', 'topic2-vpclog'],
            'type': 'json',
            'format.class': 'format_track.TrackingLogFormater',
            'schema': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id'
        }
    },
    41: {
        'interface.id': 41,
        'interface.type': 'output',
        'interface.sourceId': 41,
        'interface.name': 'di_es41',
        'properties': {
            'index': 'opkpi',
            'type': 'realstats_minute_create_account',
            # 'id.key.delimiter':'#',
            'value.key': 'value',
            'value.type': 'long',
            'add.update.method': 'getAndUpdate',
            'script.enabled': False,
            'script.param': 'p1'
        }
    },
    42: {
        'interface.id': 42,
        'interface.type': 'output',
        'interface.sourceId': 41,
        'interface.name': 'di_es42',
        'properties': {
            'index': 'opkpi',
            'type': 'realstats_minute_login_account',
            # 'id.key.delimiter':'#',
            'value.key': 'value',
            'value.type': 'long',
            'add.update.method': 'getAndUpdate',
            'script.enabled': False,
            'script.name': 'update_realstat_value',
            'script.param': 'p1'
        }
    },
    43: {
        'interface.id': 43,
        'interface.type': 'output',
        'interface.sourceId': 41,
        'interface.name': 'di_es43',
        'properties': {
            'index': 'opkpi',
            'type': 'realstats_minute_active_account',
            # 'id.key.delimiter':'#',
            'value.key': 'value',
            'value.type': 'long',
            'add.update.method': 'getAndUpdate',
            'script.enabled': False,
            'script.name': 'update_realstat_value',
            'script.param': 'p1'
        }
    },
    44: {
        'interface.id': 44,
        'interface.type': 'output',
        'interface.sourceId': 41,
        'interface.name': 'di_es44',
        'properties': {
            'index': 'opkpi',
            'type': 'realstats_minute_enrollment_state',
            # 'id.key.delimiter':'#',
            'value.key': 'value',
            'value.type': 'long',
            'add.update.method': 'getAndUpdate',
            'script.enabled': False,
            'script.name': 'update_realstat_value',
            'script.param': 'p1'
        }
    },
    45: {
        'interface.id': 45,
        'interface.type': 'output',
        'interface.sourceId': 41,
        'interface.name': 'di_es45',
        'properties': {
            'index': 'opkpi',
            'type': 'realstats_minute_access_courseware',
            # 'id.key.delimiter':'#',
            'value.key': 'value',
            'value.type': 'long',
            'add.update.method': 'getAndUpdate',
            'script.enabled': False,
            'script.name': 'update_realstat_value',
            'script.param': 'p1'
        }
    }
}

CACHE_CONFIG = {
    # key = cache.id
    21: {
        'cache.id': 21,
        'cache.sourceId': 2,
        'properties': {
            'tableName': 'api_deviceinfo',
            'keyName': 'uuid',
            'cache.keyName.list': 'channel, event, uid',
            'broadcast.enabled': False
            # ,
            # 'batchLimit': 100,
            # 'cache.query.condition.enabled': False,
            # 'maxActive': 100,
            # 'initialSize': 10,
            # 'minIdle': 5,
            # 'maxWait': 10000
        }
    },
    22: {
        'cache.id': 22,
        'cache.sourceId': 2,
        'properties': {
            'tableName': 'course_meta_course',
            'keyName': 'course_id',
            # 'cache.keyName.list': "course_type, owner, status, date_format(date_add(start, interval +8 hour), '%Y-%m-%d %H:%i:%s') as start, date_format(date_add(end, interval +8 hour), '%Y-%m-%d %H:%i:%s') as end",
            'cache.keyName.list': "course_type, owner, status, date_format(date_add(start, interval +8 hour), '%%Y-%%m-%%d %%H:%%i:%%s') as start, date_format(date_add(end, interval +8 hour), '%%Y-%%m-%%d %%H:%%i:%%s') as end",
            'broadcast.enabled': False
            # ,
            # 'batchLimit': 100,
            # 'cache.query.condition.enabled': False,
            # 'maxActive': 100,
            # 'initialSize': 10,
            # 'minIdle': 5,
            # 'maxWait': 10000
        }
    },
    23: {
        'cache.id': 23,
        'cache.sourceId': 2,
        'properties': {
            'tableName': 'auth_user',
            'keyName': 'id',
            # 'cache.keyName.list': "date_format(date_add(date_joined, interval +8 hour), '%Y-%m-%d %H:%i:%s') as date_joined",
            'cache.keyName.list': "date_format(date_add(date_joined, interval +8 hour), '%%Y-%%m-%%d %%H:%%i:%%s') as date_joined",
            'broadcast.enabled': False
            # ,
            # 'batchLimit': 100,
            # 'cache.query.condition.enabled': False,
            # 'maxActive': 100,
            # 'initialSize': 10,
            # 'minIdle': 5,
            # 'maxWait': 10000
        }
    },
    # 外部排重缓存
    31: {
        'cache.id': 31,
        'cache.sourceId': 31,
        'properties': {
            'mongo.db': 'realstats_deduplicate_db'
        }
    }
}

PREPARES_CONFIG = {
    # key = prepares.interfaceId, 配置约定：每个 interface 有一个准备阶段，可以不配置
    1: {
        'prepares.id': 1,
        'prepares.interfaceId': 1,
        'prepares.enabled': True,
        'steps': {
            # 增强时间属性
            1: {
                'step.id': 1,
                'step.type': 'enhance',
                'step.method': 'plugin',
                'step.enabled': True,
                'properties': {
                    'class': 'format_track.EnhanceTimeProcessor',
                    'timeKeyName': 'time',
                    'add.timeKeyInterval.minutes.list': [1],
                    'add.timeKey.prefix': 'time_minute_',
                    'add.key.list': ['time_minute_1_start', 'time_minute_1_end'],
                    'remove.key.list': []
                }
            },
            2: {
                'step.id': 2,
                'step.type': 'enhance',
                'step.method': 'plugin',
                'step.enabled': True,
                'properties': {
                    'class': 'format_track.EnhanceApiDeviceInfoProcessor',
                    'cacheId': 21,
                    'add.key.list': [],
                    'remove.key.list': []
                }
            },
            3: {
                'step.id': 3,
                'step.type': 'enhance',
                'step.method': 'plugin',
                'step.enabled': True,
                'properties': {
                    'class': 'format_track.EnhanceCourseInfoProcessor',
                    'cacheId': 22,
                    'add.key.list': ['course_type', 'owner', 'status', 'start, end'],
                    'remove.key.list': []
                }
            },
            4: {
                'step.id': 4,
                'step.type': 'enhance',
                'step.method': 'plugin',
                'step.enabled': True,
                'properties': {
                    'class': 'format_track.EnhanceUserInfoProcessor',
                    'cacheId': 23,
                    'add.key.list': ['data_joined'],
                    'remove.key.list': []
                }
            },
            # 增强排重属性
            # 根据指定key排重，新增标识是否重复的属性 (log.deduplicate.key) + _ + interval_label + _duplicate_flag
            # 示例： user_id_minutely_duplicate_flag
            5: {
                'step.id': 5,
                'step.type': 'enhance',
                'step.method': 'plugin',
                'step.enabled': True,
                'properties': {
                    'class': 'format_track.DeDuplicateProcessor',
                    'cacheId': 31,
                    'log.deduplicate.key': 'user_id',  # 对日志中哪个字段取值进行排重
                    # deduplicate.interval.labels
                    # 约定：spark streaming 中排重与时间相关 正常取值：daily, minutely, hourly, monthly, yearly，
                    # 排重集合名取： collectionNamePrefix + durationKey + "_" + logDeDuplicateKey + "_" + "timeStr"，
                    # 其他取值，不用时间排重，排重集合名取： collectionNamePrefix + durationKey + "_" + logDeDuplicateKey ：
                    # 示例: deplicate_xyz_user_id (不带时间)
                    'deduplicate.interval.labels': ['daily', 'minutely'],
                    'log.collection.key': 'time',  # 从日志哪个字段取查询 mongo 的 collection 进行排重查询
                    'mongo.deduplicate.keyField': 'user_id',  # 在mongo 缓存中存储id时指定的属性名，默认取 log.deduplicate.key 指定的取值
                    'mongo.collection.name.prefix': 'deduplicate_',
                    # 设置 schema 信息
                    'add.key.list': ['user_id_daily_duplicate_flag', 'user_id_minutely_duplicate_flag'],
                    'remove.key.list': [],
                    'memory.deduplicate.enabled': True
                }
            },
            # uuid 关联 mysql.api_deviceinfo, 修正移动端的 origin_referer, spam， 增强统计维度属性
            # course_id 关联 mysql.course_meta_course, 新增课程信息 course_type, owner, status, start, end，增强课程统计维度
            # user_id 关联 mysql.auth_user ，新增课程信息 date_joined ，用于区分登录和访问活跃人次

            # 3: {
            #     'step.id': 3,
            #     'step.type': 'enhance',
            #     'step.method': 'plugin',
            #     'step.enabled': False,
            #     'properties': {
            #         'class': 'foramt_track.JdbcCacheBatchQueryProcessor',
            #         'cacheId.list': '21, 22, 23',
            #         'batchProcessor.class.list': ['foramt_track.EnhanceApiDeviceInfoProcessor',
            #                                       'foramt_track.EnhanceMsCourseInfoProcessor',
            #                                       'foramt_track.EnhanceAuthUserInfoProcessor'],
            #         'add.timeKeyInterval.minutes.list': 1,
            #         'add.timeKey.prefix': 'time_minute_'
            #     }
            # }
        }
    },
    # 测试配置解析
    3: {
        'prepares.id': 3,
        'prepares.interfaceId': 1,
        'prepares.enabled': True,
        'steps': {
        }
    }
}

COMPUTES_CONFIG = {
    # key = computeStatistics.interfaceId，定义各个接口上计算统计指标需要的配置
    1: {
        # 定义指定接口上计算统计指标需要的配置，其中：有含有2个阶段：prepares， computes， 每个阶段有若干step
        'computeStatistics.interfaceId': 1,
        'computeStatistics.id': 1,
        'computeStatistics.enabled': True,
        'computeStatistics': {
            # key = computeStatistic.id
            1: {
                'computeStatistic.id': 1,
                'computeStatistic.enabled': True,
                'prepares.steps': {
                    1: {
                        'step.id': 1,
                        'step.type': 'enhance',
                        'step.method': 'spark-sql',
                        'step.enabled': True,
                        'properties': {
                            # 'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, time_minute_1_start, time_minute_1_end, user_id_daily_duplicate_flag, user_id_minutely_duplicate_flag, course_type, course_process, course_id, date_joined',
                            'selectExprClause': '',
                            'whereClause': ''
                        }
                    },
                    2: {
                        'step.id': 2,
                        'step.type': 'enhance',
                        'step.method': 'plugin',
                        'step.enabled': False,
                        'properties': {
                            'class': ''
                        }
                    },

                },
                'computes.steps': {
                    # 注册用户数
                    1: {
                        'step.id': 1,
                        'step.enabled': False,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': "time, uuid, user_id, event_type, host, platform, origin_referer, spam, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_daily_duplicate_flag",
                            'whereClause': "event_type in ('common.student.account_created','common.student.account_success','oauth.user.register','oauth.user.register_success','weixinapp.user.register_success','api.user.oauth.register_success','api.user.register','api.user.register_success') and user_id_daily_duplicate_flag = 0",
                            # 'whereClause': '',
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.1:T1:T2,L3.2:T1:T2',
                            # 'targetKeysList': 'L0:T1:T2,L1:T1:T2',
                            'data.type': 'create_account',
                            'value.type': 'users',
                            'value.cycle': 'minute_1',
                            'value.key': 'value',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            # 'output.class': 'streaming_app_platform.ConsolePrinter',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 41
                        }
                    },
                    # 登录人次
                    2: {
                        'step.id': 2,
                        'step.enabled': True,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_minutely_duplicate_flag, date_joined',
                            'whereClause': 'substr(time, 0, 11) != date_joined',
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'L3.2.1': 'origin_referer', 'L3.2.2': 'spam',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2, L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.2.1:T1:T2,L3.2.2:T1:T2',
                            'data.type': 'login_account',
                            'value.type': 'visits',
                            'value.cycle': 'minute_1',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 42
                        }
                    },
                    # 登录人数
                    3: {
                        'step.id': 3,
                        'step.enabled': False,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_minutely_duplicate_flag, date_joined',
                            'whereClause': 'substr(time, 0, 11) != date_joined and user_id_minutely_duplicate_flag = 0',
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'L3.2.1': 'origin_referer', 'L3.2.2': 'spam',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2, L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.2.1:T1:T2,L3.2.2:T1:T2',
                            'data.type': 'login_account',
                            'value.type': 'users',
                            'value.cycle': 'minute_1',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 42
                        }
                    },
                    # 访问活跃人次
                    4: {
                        'step.id': 4,
                        'step.enabled': False,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_minutely_duplicate_flag',
                            'whereClause': '',
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'L3.2.1': 'origin_referer', 'L3.2.2': 'spam',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2, L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.2.1:T1:T2,L3.2.2:T1:T2',
                            'data.type': 'active_account',
                            'value.type': 'visits',
                            'value.cycle': 'minute_1',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 43
                        }
                    },
                    # 访问活跃人数
                    5: {
                        'step.id': 5,
                        'step.enabled': False,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_minutely_duplicate_flag',
                            'whereClause': 'user_id_minutely_duplicate_flag = 0',
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'L3.2.1': 'origin_referer', 'L3.2.2': 'spam',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2, L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.2.1:T1:T2,L3.2.2:T1:T2',
                            'data.type': 'active_account',
                            'value.type': 'users',
                            'value.cycle': 'minute_1',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 43
                        }
                    },
                    # 选课人次
                    6: {
                        'step.id': 6,
                        'step.enabled': False,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_minutely_duplicate_flag, course_type, course_type, course_process, course_id',
                            'whereClause': '',
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'L3.2.1': 'origin_referer', 'L3.2.2': 'spam',
                                                'L5': 'course_type', 'L6': 'course_process', 'L7': 'course_id',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2, L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.2.1:T1:T2,L3.2.2:T1:T2,L5:T1:T2,L6:T1:T2,L7:T1:T2',
                            'data.type': 'enrollment_state',
                            'value.type': 'visits',
                            'value.cycle': 'minute_1',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 44
                        }
                    },
                    # 选课人数
                    7: {
                        'step.id': 7,
                        'step.enabled': False,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_minutely_duplicate_flag, course_type, course_process, course_id',
                            'whereClause': 'user_id_minutely_duplicate_flag = 0',
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'L3.2.1': 'origin_referer', 'L3.2.2': 'spam',
                                                'L5': 'course_type', 'L6': 'course_process', 'L7': 'course_id',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2, L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.2.1:T1:T2,L3.2.2:T1:T2,L5:T1:T2,L6:T1:T2,L7:T1:T2',
                            'data.type': 'enrollment_state',
                            'value.type': 'users',
                            'value.cycle': 'minute_1',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 44
                        }
                    },
                    # 学习活跃人次
                    8: {
                        'step.id': 8,
                        'step.enabled': False,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_minutely_duplicate_flag, course_type, course_process, course_id',
                            'whereClause': "event_type like '/courses/%/courseware/%'",
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'L3.2.1': 'origin_referer', 'L3.2.2': 'spam',
                                                'L5': 'course_type', 'L6': 'course_process', 'L7': 'course_id',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2, L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.2.1:T1:T2,L3.2.2:T1:T2',
                            'data.type': 'access_courseware',
                            'value.type': 'visits',
                            'value.cycle': 'minute_1',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 45
                        }
                    },
                    # 学习活跃人数
                    9: {
                        'step.id': 9,
                        'step.enabled': False,
                        'step.type': 'compute',
                        'step.method': 'spark-sql',
                        'properties': {
                            'selectExprClause': 'time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id, time_minute_1_start as start_date, time_minute_1_end as end_date, user_id_minutely_duplicate_flag, course_type, course_process, course_id',
                            'whereClause': "event_type like '/courses/%/courseware/%' and user_id_minutely_duplicate_flag = 0",
                            'statisticKeyMap': {'L0':'NONE', 'L1': 'host', 'L2': 'platform',
                                                'L3.1': 'origin_referer', 'L3.2': 'spam',
                                                'L3.2.1': 'origin_referer', 'L3.2.2': 'spam',
                                                'L5': 'course_type', 'L6': 'course_process', 'L7': 'course_id',
                                                'T1': 'start_date', 'T2': 'end_date'
                                                },
                            'targetKeysList': 'L0:T1:T2, L0:T1:T2,L1:T1:T2,L2:T1:T2,L3.2.1:T1:T2,L3.2.2:T1:T2',
                            'data.type': 'access_courseware',
                            'value.type': 'users',
                            'value.cycle': 'minute_1',
                            'aggegate.key.method.list': ['user_id:count'],
                            'statistic.keyLabel.excludes': 'T1,T2',
                            'output.class': 'es_utils.ESWriter',
                            'output.dataInterfaceId': 45
                        }
                    }
                }
            }
        }

    }
}
