# -*- coding: utf-8 -*-

from pyspark.sql.types import Row
import json
from streaming_app_utils import date_string_utc2cst, update_df_schema
from mongo_utils import MongoConnectionPool
from mysql_utils import MySQLUtils


class TrackingLogFormater(object):
    """
    格式化
    """
    @staticmethod
    def get_platform(agent):
        if agent and agent.startswith("xue tang zai xian android/"):
            return "android"
        if agent and agent.startswith("xue tang zai xian androidTV/"):
            return "androidTV"
        if agent and (agent.startswith("xue tang zai xian IOS/") or agent.startswith("xuetang IOS/")):
            return "iPhone"
        if agent and agent.startswith("xuetangX-iPad IOS/"):
            return "iPad"
        return "web"

    @staticmethod
    def is_valid_number(value):
        return True if value and isinstance(value, int) else False


    @staticmethod
    def fun_rdd_map(line):
        import sys

        reload(sys)
        sys.setdefaultencoding("utf-8")
        try:
            obj = json.loads(line.strip())

            uid = obj.get('uid')
            event = obj.get('event', {})
            event_uid = event.get('uid')
            event_uuid = event.get('uuid', '')
            event_username = event.get('username', '')

            context = obj.get('context', {})
            context_user_id = context.get('user_id')

            is_event_uid_a_valid_number = TrackingLogFormater.is_valid_number(event_uid)
            is_context_user_id_a_valid_number = TrackingLogFormater.is_valid_number(context_user_id)
            is_uid_a_valid_number = TrackingLogFormater.is_valid_number(uid)

            # 获取 user_id
            if is_event_uid_a_valid_number and event_uid != -1:
                user_id = str(event_uid)
            elif is_context_user_id_a_valid_number and context_user_id != -1:
                user_id = str(context_user_id)
            elif is_uid_a_valid_number and uid != -1:
                user_id = str(uid)
            else:
                if event_username.strip() != '':
                    user_id = str(event_username)
                elif event_uuid.strip() != '':
                    user_id = str(event_uuid)
                else:
                    # user_id = str(event_uid)
                    user_id = ''

            if user_id.strip():
                print('= = ' * 10, '[myapp TrackingLogFormater.fun_rdd_map] found user_id is None, event_uid = ',
                      event_uid, ", context_user_id = ", context_user_id, ", uid = ", uid,
                      ", event_username = ", event_username, ", event_uuid = ", event_uuid)

            time = obj.get('time')
            username = event_username if event_username.strip() != '' else obj.get('username', '')
            uuid = event_uuid if event_uuid.strip() != '' else obj.get('uuid', '')

            event_type = obj.get('event_type', '')
            agent = obj.get('agent', '')
            platform = TrackingLogFormater.get_platform(agent)

            origin_referer = obj.get('origin_referer', '')
            spam = obj.get('spam', '') if obj.get('spam', '') else ''
            # print('= = ' * 20 , '[myapp TrackingLogFormater.format.fun_rdd_map] spam = ', spam)
            host = obj.get('host', '')

            event_course_id = event.get('course_id', '')
            context_course_id = context.get('course_id', '')
            course_id = event_course_id if event_course_id.strip() != '' else context_course_id.strip()

            ret_obj = {
                'user_id': user_id,
                'username': username,
                'uuid': uuid,
                'event_type': event_type,
                'host': host,
                'platform': platform,
                'origin_referer': origin_referer,
                'spam': spam,
                'time': time,
                'course_id': course_id
            }
            ret = Row(**ret_obj)

        except Exception, ex:
            print('= = ' * 10, '[myapp TrackingLogFormater.fun_rdd_map] got exception while formating log =', line)
            # raise ex
            ret = None
        print('= = ' * 10, '[myapp TrackingLogFormater.fun_rdd_map] ret =', ret, 'line = ', line)
        return ret

    @staticmethod
    def format(dstream):
        return dstream.transform(
            lambda rdd: rdd.map(
                lambda line: TrackingLogFormater.fun_rdd_map(line)
            ).filter(lambda line: line is not None))


    # @staticmethod
    # def fun_rdd_map2(line):
    #     """
    #     使用json，并添加 labels 字段
    #     """
    #     import sys
    #
    #     reload(sys)
    #     sys.setdefaultencoding("utf-8")
    #     try:
    #         obj = json.loads(line.strip())
    #
    #         uid = obj.get('uid')
    #         event = obj.get('event', {})
    #         event_uid = event.get('uid')
    #         event_uuid = event.get('uuid', '')
    #         event_username = event.get('username', '')
    #
    #         context = obj.get('context', {})
    #         context_user_id = context.get('user_id')
    #
    #         is_event_uid_a_valid_number = TrackingLogFormater.is_valid_number(event_uid)
    #         is_context_user_id_a_valid_number = TrackingLogFormater.is_valid_number(context_user_id)
    #         is_uid_a_valid_number = TrackingLogFormater.is_valid_number(uid)
    #
    #         # 获取 user_id
    #         user_id = None
    #         if is_event_uid_a_valid_number and event_uid != -1:
    #             user_id = str(event_uid)
    #         elif is_context_user_id_a_valid_number and context_user_id != -1:
    #             user_id = str(context_user_id)
    #         elif is_uid_a_valid_number and uid != -1:
    #             user_id = str(uid)
    #         else:
    #             if event_username.strip() != '':
    #                 user_id = str(event_username)
    #             elif event_uuid.strip() != '':
    #                 user_id = str(event_uuid)
    #             else:
    #                 user_id = str(event_uid)
    #
    #         if user_id is None:
    #             print('= = ' * 10, '[myapp TrackingLogFormater.fun_rdd_map] found user_id is None, event_uid = ' +
    #                   event_uid + ", context_user_id = " + context_user_id + ", uid = " + uid +
    #                   ", event_username = " + event_username + ", event_uuid = " + event_uuid)
    #         # else:
    #         # print('= = ' * 10, '[myapp TrackingLogFormater.fun_rdd_map] found user_id = ' + user_id )
    #
    #         time = obj.get('time')
    #         username = event_username if event_username.strip() != '' else obj.get('username', '')
    #         uuid = event_uuid if event_uuid.strip() != '' else obj.get('uuid', '')
    #
    #         event_type = obj.get('event_type', '')
    #         agent = obj.get('agent', '')
    #         platform = TrackingLogFormater.get_platform(agent)
    #
    #         origin_referer = obj.get('origin_referer', '').strip()
    #         spam = obj.get('spam', '').strip() if obj.get('spam', '') else ''
    #         # print('= = ' * 20 , '[myapp TrackingLogFormater.format.fun_rdd_map] spam = ', spam)
    #         host = obj.get('host', '')
    #
    #         event_course_id = event.get('course_id', '')
    #         context_course_id = context.get('course_id', '')
    #         course_id = event_course_id if event_course_id.strip() != '' else context_course_id.strip()
    #
    #         ret_obj = {
    #             'user_id': user_id,
    #             'username': username,
    #             'uuid': uuid,
    #             'event_type': event_type,
    #             'host': host,
    #             'platform': platform,
    #             'origin_referer': origin_referer,
    #             'spam': spam,
    #             'time': time,
    #             'course_id': course_id,
    #             'labels': {}
    #         }
    #
    #         # ret = [user_id, username, uuid, event_type, host, platform, origin_referer, spam, time, course_id, {}]
    #         # ret = [ret_obj.get(field) for field in schema_field_list]
    #
    #         # TODO: 测试 jsonRDD 过程中内部是否对 StructType 的 StructField 有排序操作
    #         # ret = Row(**ret_obj)
    #         ret = json.dumps(ret_obj)
    #
    #     except Exception, ex:
    #         print('= = ' * 10, '[myapp TrackingLogFormater.fun_rdd_map] got exception while formating log =', line)
    #         ret = None
    #     # print('= = ' * 10, '[myapp TrackingLogFormater.fun_rdd_map] ret =', ret, 'line = ', line)
    #     return ret


class EnhanceTimeProcessor(object):
    @staticmethod
    def fun_time_in_rdd_map(row, time_key, time_interval_minute_list, time_key_prefix, cache_conf=None):
        """
        输入: Row[**obj]
        输出: Row[**obj]
        """
        import sys

        reload(sys)
        sys.setdefaultencoding("utf-8")

        from datetime import datetime
        # obj = json.loads(line)
        obj = row.asDict()

        time = obj.get(time_key, '')
        cst_time = date_string_utc2cst(time)
        if cst_time == '':
            ret = None
        else:
            obj[time_key] = cst_time
            minute = datetime.strptime(cst_time, '%Y-%m-%d %H:%M:%S').minute

            for time_interval_minute in time_interval_minute_list:
                minute_start = (minute / time_interval_minute) * time_interval_minute
                minute_end = minute_start + time_interval_minute - 1
                time_start_key = time_key_prefix + str(time_interval_minute) + '_start'
                time_end_key = time_key_prefix + str(time_interval_minute) + '_end'
                time_start = cst_time[0:14] + str(minute_start).zfill(2) + ':00'
                time_end = cst_time[0:14] + str(minute_end).zfill(2) + ':59'
                obj[time_start_key] = time_start
                obj[time_end_key] = time_end

            # ret = json.dumps(obj)
            ret = Row(**obj)
        # print('= = ' * 15, '[myapp EnhanceTimeProcessor.process.fun_time_in_rdd_map] ret = ')
        # print(ret)
        return ret

    @staticmethod
    def process(rdd, p_schema, step_conf, cache_conf=None):
        """
        输入: Row[**obj]
        输出: Row[**obj]
        """
        time_key = step_conf['timeKeyName']
        # TODO: 新增 hour 级别配置
        time_interval_minutes_list = step_conf['add.timeKeyInterval.minutes.list']
        time_key_prefix = step_conf['add.timeKey.prefix']

        ret_rdd = rdd.map(
            lambda row: EnhanceTimeProcessor.fun_time_in_rdd_map(
                row, time_key, time_interval_minutes_list, time_key_prefix)
        ).filter(lambda row: row is not None)

        remove_key_list = step_conf.get('remove.key.list', [])
        add_key_list = step_conf.get('add.key.list', [])
        schema_new = update_df_schema(p_schema, remove_key_list, add_key_list)

        return ret_rdd, schema_new


class DeDuplicateProcessor(object):
    """
    增强排重属性

    根据指定key排重，新增标识是否重复的属性 (log.deduplicate.key) + _ + interval_label + _duplicate_flag
    示例： user_id_minutely_duplicate_flag
    """
    def __init__(self):
        self.deduplicate_cache_pools = {}

    def get_cache_pool(self, cache_id):
        return self.deduplicate_cache_pools.setdefault(cache_id, set())

    @staticmethod
    def get_deduplicate_collection_name(
            mongo_collection_name_prefix, interval_label, log_deduplicate_key, log_collection_value):

        prefix = mongo_collection_name_prefix + interval_label + '_' + log_deduplicate_key
        log_collection_value_time_string = log_collection_value.replace('-', '').replace(' ', '').replace(':', '')
        if interval_label == 'minutely':
            return prefix + log_collection_value_time_string[0:12]  # yyyyMMddHHmm
        elif interval_label == 'hourly':
            return prefix + log_collection_value_time_string[0:10]  # yyyyMMddHH
        elif interval_label == 'daily':
            return prefix + log_collection_value_time_string[0:8]  # yyyyMMdd
        elif interval_label == 'monthly':
            return prefix + log_collection_value_time_string[0:6]  # yyyyMM
        elif interval_label == 'yearly':
            return prefix + log_collection_value_time_string[0:4]  # yyyy
        else:
            raise Exception('[myapp DeDuplicateProcessor] configuration error: '
                            'found unsupported interval_label = ' + interval_label + ' in deduplicate.interval.labels')

    @staticmethod
    def fun_deduplicate_in_rdd_mapPartitions(iter_x, step_conf, cache_conf):
        """
        输入: iter[Row]
        输出: iter[Row]
        """
        log_deduplicate_key = step_conf['log.deduplicate.key']  # 对日志中哪个字段取值进行排重，示例：user_id

        # 约定：spark streaming 中排重与时间相关 正常取值：daily, minutely, hourly, monthly, yearly：
        # 排重集合名取： collectionNamePrefix + durationKey + "_" + logDeDuplicateKey + "_" + "timeStr"
        # 其他取值，不用时间排重，排重集合名取： collectionNamePrefix + durationKey + "_" + logDeDuplicateKey ：示例: deplicate_xyz_user_id (不带时间)
        deduplicate_interval_labels = step_conf['deduplicate.interval.labels']
        interval_label_list = [x.strip() for x in deduplicate_interval_labels if x.strip() != '']

        log_collection_key = step_conf.get('log.collection.key', '').strip()

        # 在mongo 缓冲中存储id时指定的属性名，默认取 log_deduplicate_key 指定的取值
        mongo_deduplicate_key_field_opt = step_conf.get('mongo.deduplicate.keyField', '')
        mongo_deduplicate_key_field = \
            mongo_deduplicate_key_field_opt.strip() if mongo_deduplicate_key_field_opt.strip() != '' else log_deduplicate_key

        mongo_collection_name_prefix = step_conf.get('mongo.collection.name.prefix', 'deDuplicate_').strip()
        assert mongo_collection_name_prefix != '', \
            '[myapp DeDuplicateProcessor.processs.get_deduplicate_collection_name] ' \
            'configuration error : mongo.collection.name.prefix should not be empty'

        mongo_url_string = cache_conf['mongo.connection.url']
        mongo_db_name = cache_conf['mongo.db']

        mongo_client = MongoConnectionPool.get_connection(mongo_url_string)
        mongo_db = mongo_client[mongo_db_name]

        # 排重的数据结构
        # For mongo
        # 方式1：db.collection(yyyy-MM-dd).document(id): 每个排重的id 存储为 一个document ，排重方式：检查document是否存在，不存在添加document; 存在，则设置重复
        # 方式2：db.collection.document(yyyy-MM-dd) : 排重的id作为每个document 的字段，排重方式：检查字段是否存在，不存在更新添加字段; 存在，则设置重复
        # 方式3：db.collection.document(yyyy-MM-dd) : 排重的id存储在每个 document 的 ids 数组字段，排重方式：检查数组中是否存在，不存在更新添加数组元素; 存在，则设置重复

        # 批次排重内存对每个interval_label 而言是独立的，都要初始化生成 cache_pool
        batch_deduplicate_cache_pools = {}

        for row in iter_x:
            obj = row.asDict()

            log_deduplicate_value = obj[log_deduplicate_key]  # 示例: user_id 的取值
            log_collection_value = obj.get(log_collection_key, '').strip() if log_collection_key != '' else ''

            for interval_label in interval_label_list:
                # 设置属性名
                duplicate_flag_key = log_deduplicate_key + '_' + interval_label + '_duplicate_flag'
                duplicate_flag_value = 0  # 初始值0 不重复

                # 约定与排重周期相关的字段正常取值是时间字符串
                if log_collection_value != '':  # 如果为空，无法判断重复情况，默认不重复

                    deduplicate_collection_name = DeDuplicateProcessor.get_deduplicate_collection_name(
                        mongo_collection_name_prefix, interval_label, log_deduplicate_key, log_collection_value)

                    deduplicate_cache_set = batch_deduplicate_cache_pools.setdefault(deduplicate_collection_name, set())

                    print('= = ' * 10,
                          '[myapp DeDuplicateProcessor.fun_deduplicate_in_rdd_mapPartitions] log_deduplicate_value = '
                          + log_deduplicate_value + ', in cache.' + deduplicate_collection_name + ' or not = ',
                          (log_deduplicate_value in deduplicate_cache_set))

                    if log_deduplicate_value in deduplicate_cache_set:
                        batch_duplicate_flag = True
                        duplicate_flag_value = 1
                    else:
                        deduplicate_cache_set.add(log_deduplicate_value)
                        batch_duplicate_flag = False

                    # 如果批次内不能判断重复，查询外部缓存
                    if not batch_duplicate_flag:

                        mongo_collection = mongo_db[deduplicate_collection_name]
                        resp = mongo_collection.find_one({mongo_deduplicate_key_field: log_deduplicate_value})

                        print('= = ' * 10,
                          '[myapp DeDuplicateProcessor.fun_deduplicate_in_rdd_mapPartitions] log_deduplicate_value = ',
                          log_deduplicate_value, ', in mongodb.', deduplicate_collection_name, ' or not = ', resp)

                        if resp is not None:
                            # 外部缓存中存在记录，重复
                            duplicate_flag_value = 1
                        else:
                            print('= = ' * 10,
                                  '[myapp DeDuplicateProcessor.fun_deduplicate_in_rdd_mapPartitions] mongodb.insert_one with '
                                  'mongo_deduplicate_key_field = ', mongo_deduplicate_key_field,
                                  ', log_deduplicate_value = ', log_deduplicate_value)
                            # 查询外部缓存没有，该记录不重复，更新外部缓存，和内部缓存
                            mongo_collection.insert_one({mongo_deduplicate_key_field: log_deduplicate_value})

                        deduplicate_cache_set.add(log_deduplicate_value)

                obj[duplicate_flag_key] = duplicate_flag_value

            yield Row(**obj)
        else:
            # 每个 task执行完成后清理内存排重缓存， TODO: 是否执行
            for deduplicate_collection_name, deduplicate_cache_set in batch_deduplicate_cache_pools.iteritems():
                print('= = ' * 10, '[myapp DeDuplicateProcessor.fun_deduplicate_in_rdd_mapPartitions] '
                                   'clean up cache.' + deduplicate_collection_name)
                deduplicate_cache_set.clear()

    @staticmethod
    def process(rdd, p_schema, step_conf, cache_conf):
        """
        输入: RDD[Row]
        输出: RDD[Row]
        """

        ret_rdd = rdd.mapPartitions(
            lambda iter_x: DeDuplicateProcessor.fun_deduplicate_in_rdd_mapPartitions(iter_x, step_conf, cache_conf)
        ).filter(lambda row: row is not None)

        remove_key_list = step_conf.get('remove.key.list', [])
        add_key_list = step_conf.get('add.key.list', [])
        schema_new = update_df_schema(p_schema, remove_key_list, add_key_list)

        print('= = ' * 10, '[myapp DeDuplicateProcessor.process] ret_rdd.is_cached = ', ret_rdd.is_cached)
        # if not ret_rdd.is_cached:
        #     ret_rdd.persist()
        return ret_rdd, schema_new


class EnhanceApiDeviceInfoProcessor(object):
    @staticmethod
    def fun_deviceinfo_in_rdd_mapPartitions(iter_x, step_conf, cache_conf):
        """
        输入: iter[Row]
        输出: iter[Row]
        """
        import re

        domain_pattern = re.compile('https?://(.*?)/.*')
        ORIGIN_REFERER_KEY = 'origin_referer'
        CHANNEL_KEY = 'channel'
        UNKNOWN_ORIGIN_REFERER_VALUE = 'unknown'

        USER_ID_KEY = 'user_id'
        UID_KEY = 'uid'

        SPAM_KEY = 'spam'
        EVENT_KEY = 'event'
        UNKNOWN_SPAM_VALUE = 'unknown'


        host = cache_conf['host']
        port = cache_conf.get('port', 3306)
        db = cache_conf['db']
        user = cache_conf['user']
        password = cache_conf.get('password', '')

        table_name = cache_conf['tableName']
        key_name = cache_conf['keyName']
        cache_key_name_list = cache_conf['cache.keyName.list']

        cache_sql = 'select ' + key_name + ', ' + cache_key_name_list + ' from ' + db + '.' + table_name
        query_sql = cache_sql + ' where ' + key_name + ' = %s'
        # TODO: 支持批量查询
        batch_query_sql = cache_sql + ' where ' + key_name + " in (?)"

        # TODO: 支持 broadcast 应用启动时cache一份外部缓存
        broadcast_enabled = cache_conf.get('broadcast.enabled', False) \
            if isinstance(cache_conf.get('broadcast.enabled'), bool) else False

        # 增量动态缓存
        cache_id = 'ds_id_' + str(cache_conf['source.id']) + '#cache_id_' + str(cache_conf['cache.id'])
        cache_pool = MySQLUtils.cache_pools.setdefault(cache_id, {})

        conn = MySQLUtils.get_connection(host=host, db=db, user=user, password=password, port=port)

        # 更新属性
        # 1 更新 origin_referer 规则：如果日志中 origin_referer 为空(null or “”)且设备信息channel字段不为空，
        #   取设备信息的channel字段值，其他情况取日志中 origin_referer 字段
        #   Note: origin_referer 暂取 domain
        # 2 更新 spam 规则：如果用户日志中 spam 为空(null or ””)且设备信息event字段不为空，
        #   取设备信息的event，其他情况取用户日志中 spam 的值
        # 3 更新 user_id 规则: 如果关联到缓存uid，更新；日志中的 user_id 可能不是有效的

        def proc_update(obj, cache_channel=None, cache_event=None, cache_uid=None):
            if not obj[ORIGIN_REFERER_KEY] and cache_channel:
                obj[ORIGIN_REFERER_KEY] = cache_channel

            if obj[ORIGIN_REFERER_KEY]:
                match_result = re.match(domain_pattern, obj[ORIGIN_REFERER_KEY])
                if match_result:
                    obj[ORIGIN_REFERER_KEY] = match_result.group(1)

            if not obj[ORIGIN_REFERER_KEY]:
                obj[ORIGIN_REFERER_KEY] = UNKNOWN_ORIGIN_REFERER_VALUE

            if not obj[SPAM_KEY] and cache_event:
                obj[SPAM_KEY] = cache_event

            if not obj[SPAM_KEY]:
                obj[SPAM_KEY] = UNKNOWN_SPAM_VALUE

            if cache_uid and obj[USER_ID_KEY] != cache_uid:
                obj[USER_ID_KEY] = cache_uid

        for row in iter_x:
            obj = row.asDict()

            key_value = obj.get(key_name, '').strip()
            print('= = ' * 10,
                  '[myapp EnhanceApiDeviceInfoProcessor.process.fun_deviceinfo_in_rdd_mapPartitions] found key_value =',
                  key_value, ', obj = ', obj)

            # 约定 key_value 唯一，为空
            if str(key_value) == '':
                # 没有关联信息处理: web端日志 uuid 为空，不需要关联 mysql.api_deviceinfo 更新 origin_referer, spam
                # 判断是否更新
                proc_update(obj)
            else:
                # 先检查内存缓存，再检查broadcast，最后检查外部缓存
                if key_value in cache_pool and isinstance(cache_pool[key_value], dict):
                    key_cache = cache_pool[key_value]
                    # 内存缓存中命中
                    proc_update(obj, key_cache[CHANNEL_KEY], key_cache[EVENT_KEY], key_cache[UID_KEY])

                elif broadcast_enabled and key_value in cache_conf.get('broadcast').value:
                    # broadcast 内存命中
                    key_cache = cache_conf.get('broadcast').value[key_value]
                    print('= = ' * 10,
                          '[myapp EnhanceApiDeviceInfoProcessor.process.fun_deviceinfo_in_rdd_mapPartitions] found '
                          'broadcast_cache = ', key_cache)
                    proc_update(obj, key_cache[CHANNEL_KEY], key_cache[EVENT_KEY], key_cache[UID_KEY])
                else:
                    # 如果内存中没有缓存，查询外部缓存
                    external_cache = MySQLUtils.query(
                        conn=conn, sql_text=query_sql, key_name=key_name, sql_args=(key_value,))
                    print('= = ' * 10,
                          '[myapp EnhanceApiDeviceInfoProcessor.process.fun_deviceinfo_in_rdd_mapPartitions] found '
                          'external_cache = ', external_cache)
                    if len(external_cache) == 0:
                        # 外部缓存也没有关联信息时处理: 根据日志中的 origin_referer, spam 字段信息，更新 origin_referer, spam
                        proc_update(obj)
                    else:
                        # 查到关联信息后处理，先根据关联信息更新字段，再更新 origin_referer, spam
                        key_cache = external_cache[key_value]
                        print('= = ' * 10,
                          '[myapp EnhanceApiDeviceInfoProcessor.process.fun_deviceinfo_in_rdd_mapPartitions] found '
                          'mysql_cache = ', key_cache)
                        proc_update(obj, key_cache[CHANNEL_KEY], key_cache[EVENT_KEY], key_cache[UID_KEY])
                        # 更新内存缓存
                        for k, v in external_cache.iteritems():
                            cache_pool[k] = v
                        external_cache.clear()

            yield Row(**obj)


    @staticmethod
    def process(rdd, p_schema, step_conf, cache_conf):
        """
        输入: RDD[Row]
        输出: RDD[Row]
        """

        ret_rdd = rdd.mapPartitions(
            lambda iter_x: EnhanceApiDeviceInfoProcessor.fun_deviceinfo_in_rdd_mapPartitions(iter_x, step_conf, cache_conf)
        ).filter(lambda row: row is not None)

        remove_key_list = step_conf.get('remove.key.list', [])
        add_key_list = step_conf.get('add.key.list', [])
        schema_new = update_df_schema(p_schema, remove_key_list, add_key_list)

        return ret_rdd, schema_new


class EnhanceCourseInfoProcessor(object):
    @staticmethod
    def fun_courseinfo_in_rdd_mapPartitions(iter_x, step_conf, cache_conf):
        """
        输入: iter[Row]
        输出: iter[Row]
        """
        from datetime import datetime, timedelta

        COURSE_TYPE_KEY = 'course_type'
        COURSE_OWNER_KEY = 'owner'
        COURSE_STATUS_KEY = 'status'
        COURSE_START_KEY = 'start'
        COURSE_END_KEY = 'end'

        COURSE_PROCESS_KEY = 'course_process'

        host = cache_conf['host']
        port = cache_conf.get('port', 3306)
        db = cache_conf['db']
        user = cache_conf['user']
        password = cache_conf.get('password', '')

        table_name = cache_conf['tableName']
        key_name = cache_conf['keyName']  # course_id
        cache_key_name_list = cache_conf['cache.keyName.list']

        cache_sql = 'select ' + key_name + ', ' + cache_key_name_list + ' from ' + db + '.' + table_name
        print('= = ' * 10,
              '[myapp EnhanceCourseInfoProcessor.process.fun_courseinfo_in_rdd_mapPartitions] cache_sql = ' + cache_sql)
        query_sql = cache_sql + ' where ' + key_name + ' = %s'
        # TODO: 支持批量查询
        batch_query_sql = cache_sql + ' where ' + key_name + " in (?)"

        # TODO: 支持 broadcast 应用启动时cache一份外部缓存
        broadcast_enabled = cache_conf.get('broadcast.enabled', False) \
            if isinstance(cache_conf.get('broadcast.enabled'), bool) else False

        # 增量动态缓存
        cache_id = 'ds_id_' + str(cache_conf['source.id']) + '#cache_id_' + str(cache_conf['cache.id'])
        cache_pool = MySQLUtils.cache_pools.setdefault(cache_id, {})

        conn = MySQLUtils.get_connection(host=host, db=db, user=user, password=password, port=port)

        # def check_process(course_id, course_type, status, start, end, et, course_map):
        #     """
        #     离线计算使用
        #     """
        #     if course_type == "0":
        #         return -1 if (start is None or start > et or status == "-1") else (1 if (end < et or course_map.has_key(course_id)) else 0)
        #     if course_type == "1":
        #         return 1 if status == "-1" else 0
        #     return -1

        def check_process(course_type, course_status, course_start, course_end, check_date):
            """
            实时计算使用
            et: 取处理记录时的 CST 时间
            course_map: 不用判断
            """
            if course_type == "0":
                return -1 \
                    if (course_start is None or course_start > check_date or course_status == "-1") \
                    else (1 if (course_end < check_date) else 0)
            elif course_type == "1":
                return 1 if course_status == "-1" else 0
            else:
                return -1

        # 更新属性
        def proc_update(
                obj,
                course_type=None, course_owner=None, course_status=None, course_start=None, course_end=None,
                check_date=(datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')):
            obj[COURSE_TYPE_KEY] = course_type
            obj[COURSE_OWNER_KEY] = course_owner
            obj[COURSE_STATUS_KEY] = course_status
            obj[COURSE_START_KEY] = course_start
            obj[COURSE_END_KEY] = course_end

            course_process = check_process(course_type, course_status, course_start, course_end, check_date)

            obj[COURSE_PROCESS_KEY] = course_process

        for row in iter_x:

            obj = row.asDict()

            key_value = obj.get(key_name, '').strip()  # course_id

            # 约定 key_value 唯一，为空
            if str(key_value) == '':
                # 没有关联信息处理: web端日志 uuid 为空，不需要关联 mysql.api_deviceinfo 更新 origin_referer, spam
                # 判断是否更新
                proc_update(obj)
            else:
                if key_value in cache_pool:
                    key_cache = cache_pool[key_value]
                    # 内存缓存中命中
                    proc_update(obj,
                                key_cache[COURSE_TYPE_KEY], key_cache[COURSE_OWNER_KEY], key_cache[COURSE_STATUS_KEY],
                                key_cache[COURSE_START_KEY], key_cache[COURSE_END_KEY])
                elif broadcast_enabled and key_value in cache_conf.get('broadcast').value:
                    # broadcast 内存命中
                    key_cache = cache_conf.get('broadcast').value[key_value]
                    print('= = ' * 10,
                          '[myapp EnhanceCourseInfoProcessor.process.fun_courseinfo_in_rdd_mapPartitions] found '
                          'broadcast_cache = ', key_cache)
                    proc_update(obj,
                                key_cache[COURSE_TYPE_KEY], key_cache[COURSE_OWNER_KEY], key_cache[COURSE_STATUS_KEY],
                                key_cache[COURSE_START_KEY], key_cache[COURSE_END_KEY])
                else:
                    # 如果内存中没有缓存，查询外部缓存
                    external_cache = MySQLUtils.query(
                        conn=conn, sql_text=query_sql, key_name=key_name, sql_args=(key_value,))

                    if len(external_cache) == 0:
                        # 外部缓存也没有关联信息时处理: 设置默认值
                        proc_update(obj)
                    else:
                        # 查到关联信息后处理，先根据关联信息更新字段，再更新课程信息
                        key_cache = external_cache[key_value]
                        proc_update(obj,
                                    key_cache[COURSE_TYPE_KEY], key_cache[COURSE_OWNER_KEY], key_cache[COURSE_STATUS_KEY],
                                    key_cache[COURSE_START_KEY], key_cache[COURSE_END_KEY])
                        # 更新内存缓存
                        for k, v in external_cache.iteritems():
                            cache_pool[k] = v
                        external_cache.clear()

            yield Row(**obj)


    @staticmethod
    def process(rdd, p_schema, step_conf, cache_conf):
        """
        输入: RDD[Row]
        输出: RDD[Row]
        """
        ret_rdd = rdd.mapPartitions(
            lambda iter_x: EnhanceCourseInfoProcessor.fun_courseinfo_in_rdd_mapPartitions(iter_x, step_conf, cache_conf)
        ).filter(lambda row: row is not None)

        remove_key_list = step_conf.get('remove.key.list', [])
        add_key_list = step_conf.get('add.key.list', [])
        schema_new = update_df_schema(p_schema, remove_key_list, add_key_list)

        return ret_rdd, schema_new


class EnhanceUserInfoProcessor(object):
    @staticmethod
    def fun_userinfo_in_rdd_mapPartitions(iter_x, step_conf, cache_conf):
        """
        输入: iter[Row]
        输出: iter[Row]
        """
        from datetime import datetime, timedelta

        DATA_JOIN_KEY = 'date_joined'

        host = cache_conf['host']
        port = cache_conf.get('port', 3306)
        db = cache_conf['db']
        user = cache_conf['user']
        password = cache_conf.get('password', '')

        table_name = cache_conf['tableName']
        key_name = cache_conf['keyName']  # course_id
        cache_key_name_list = cache_conf['cache.keyName.list']

        cache_sql = 'select ' + key_name + ', ' + cache_key_name_list + ' from ' + db + '.' + table_name
        query_sql = cache_sql + ' where ' + key_name + ' = %s'
        # TODO: 支持批量查询
        # batch_query_sql = cache_sql + ' where ' + key_name + " in (?)"

        # TODO: 支持 broadcast 应用启动时cache一份外部缓存
        broadcast_enabled = cache_conf.get('broadcast.enabled', False) \
            if isinstance(cache_conf.get('broadcast.enabled'), bool) else False

        # 增量动态缓存
        cache_id = 'ds_id_' + str(cache_conf['source.id']) + '#cache_id_' + str(cache_conf['cache.id'])
        cache_pool = MySQLUtils.cache_pools.setdefault(cache_id, {})

        conn = MySQLUtils.get_connection(host=host, db=db, user=user, password=password, port=port)

        # 更新属性
        def proc_update(obj, date_joined=None):
            obj[DATA_JOIN_KEY] = date_joined if date_joined else ''

        for row in iter_x:

            obj = row.asDict()

            key_value = obj.get(key_name, '').strip()  # course_id

            # 约定 key_value 唯一，为空
            if str(key_value) == '':
                # 没有关联信息处理: web端日志 uuid 为空，不需要关联 mysql.api_deviceinfo 更新 origin_referer, spam
                # 判断是否更新
                proc_update(obj)
            else:
                if key_value in cache_pool:
                    key_cache = cache_pool[key_value]
                    # 内存缓存中命中
                    proc_update(obj, key_cache[DATA_JOIN_KEY])
                elif broadcast_enabled and key_value in cache_conf.get('broadcast').value:
                    # broadcast 内存命中
                    key_cache = cache_conf.get('broadcast').value[key_value]
                    print('= = ' * 10,
                          '[myapp EnhanceUserInfoProcessor.process.fun_userinfo_in_rdd_mapPartitions] found '
                          'broadcast_cache = ', key_cache)
                    proc_update(obj, key_cache[DATA_JOIN_KEY])
                else:
                    # 如果内存中没有缓存，查询外部缓存
                    external_cache = MySQLUtils.query(
                        conn=conn, sql_text=query_sql, key_name=key_name, sql_args=(key_value,))

                    if len(external_cache) == 0:
                        # 外部缓存也没有关联信息时处理: 设置默认值
                        proc_update(obj)
                    else:
                        # 查到关联信息后处理，先根据关联信息更新字段，再更新课程信息
                        key_cache = external_cache[key_value]
                        proc_update(obj, key_cache[DATA_JOIN_KEY])
                        # 更新内存缓存
                        for k, v in external_cache.iteritems():
                            cache_pool[k] = v
                        external_cache.clear()

            yield Row(**obj)


    @staticmethod
    def process(rdd, p_schema, step_conf, cache_conf):
        """
        输入: RDD[Row]
        输出: RDD[Row]
        """
        ret_rdd = rdd.mapPartitions(
            lambda iter_x: EnhanceUserInfoProcessor.fun_userinfo_in_rdd_mapPartitions(iter_x, step_conf, cache_conf)
        ).filter(lambda row: row is not None)

        remove_key_list = step_conf.get('remove.key.list', [])
        add_key_list = step_conf.get('add.key.list', [])
        schema_new = update_df_schema(p_schema, remove_key_list, add_key_list)

        return ret_rdd, schema_new
