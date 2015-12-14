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

from pyspark.sql.types import Row
from streaming_app_utils import date_string_utc2cst


def fun_map_row(row, time_key, time_interval_minute_list, time_key_prefix):
    reload(sys)
    sys.setdefaultencoding("utf-8")

    from datetime import datetime

    obj = row.asDict()

    time = obj.get(time_key, '')
    # print('= = ' * 10, '[myapp test_utils.fun_map_row] found time = ', time, ', obj = ', obj)
    cst_time = date_string_utc2cst(time)
    obj[time_key] = cst_time
    if cst_time == '':
        res = None
        print('= = ' * 10, '[myapp test_utils.fun_map_row] found ivalid cst_time')
    else:
        labels = obj['labels']
        minute = datetime.strptime(cst_time, '%Y-%m-%d %H:%M:%S').minute

        for time_interval_minute in time_interval_minute_list:
            minute_start = (minute / time_interval_minute) * time_interval_minute
            minute_end = minute_start + time_interval_minute - 1
            time_start_key = time_key_prefix + str(time_interval_minute) + '_start'
            time_end_key = time_key_prefix + str(time_interval_minute) + '_end'
            time_start = cst_time[0:14] + str(minute_start).zfill(2) + ':00'
            time_end = cst_time[0:14] + str(minute_end).zfill(2) + ':59'
            labels[time_start_key] = time_start
            labels[time_end_key] = time_end
        obj['labels'] = labels

        res = Row(**obj)
        print('= = ' * 10, '[myapp test_utils.fun_map_row] res.time = ', res.time, ', labels = ', res.labels)

    return res


def fun_map_json(json_string, time_key, time_interval_minute_list, time_key_prefix):
    import json
    reload(sys)
    sys.setdefaultencoding("utf-8")

    from datetime import datetime

    obj = json.loads(json_string)

    time = obj.get(time_key, '')
    # print('= = ' * 10, '[myapp test_utils.fun_map_row] found time = ', time, ', obj = ', obj)
    cst_time = date_string_utc2cst(time)
    obj[time_key] = cst_time
    if cst_time == '':
        res = None
        print('= = ' * 10, '[myapp test_utils.fun_map_row] found ivalid cst_time')
    else:
        labels = obj['labels']
        minute = datetime.strptime(cst_time, '%Y-%m-%d %H:%M:%S').minute

        for time_interval_minute in time_interval_minute_list:
            minute_start = (minute / time_interval_minute) * time_interval_minute
            minute_end = minute_start + time_interval_minute - 1
            time_start_key = time_key_prefix + str(time_interval_minute) + '_start'
            time_end_key = time_key_prefix + str(time_interval_minute) + '_end'
            time_start = cst_time[0:14] + str(minute_start).zfill(2) + ':00'
            time_end = cst_time[0:14] + str(minute_end).zfill(2) + ':59'
            labels[time_start_key] = time_start
            labels[time_end_key] = time_end
        obj['labels'] = labels

        res = Row(**obj)
        print('= = ' * 10, '[myapp test_utils.fun_map_row] res.time = ', res.time, ', labels = ', res.labels)

    return res
