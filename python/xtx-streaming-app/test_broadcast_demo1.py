# -*- coding = utf-8 -*-

import os
import sys
import pprint

print "[{0}] sys.path before import:".format(sys.argv[0])
pprint.pprint(sys.path)

# For spark
sys.path.append('/data01/app/bigdata/spark/python')
sys.path.append('/data01/app/bigdata/spark/python/lib/py4j-0.8.2.1-src.zip')

# sys.path.append('/home/hadoop/app/spark/python')
# sys.path.append('/home/hadoop/app/spark/python/lib/py4j-0.8.2.1-src.zip')

# sys.path.append('/data01/app/bigdata/spark/test1') # For Test

# For MySQLdb
sys.path.append('/usr/local/lib/python2.7/site-packages')

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/data01/app/bigdata/spark'
SPARK_HOME = os.environ['SPARK_HOME']

from pyspark import SparkContext, SparkConf
from mysql_utils import MySQLUtils


def get_api_deviceinfo():
    host = '192.168.9.228'
    port = 3306
    db = 'edxapp'
    user = 'root'
    password = ''
    #
    table_name = 'api_deviceinfo'
    key_name = 'uuid'
    cache_key_name_list = 'channel, event, uid'
    #
    cache_sql = 'select ' + key_name + ', ' + cache_key_name_list + ' from ' + db + '.' + table_name
    #
    conn = MySQLUtils.get_connection(host=host, db=db, user=user, password=password, port=port)
    #
    external_cache = MySQLUtils.query(conn, sql_text=cache_sql, key_name=key_name)
    return external_cache


def main():
    # master = 'local[2]'
    master = 'spark://192.168.9.164:7077'
    app_name = 'test-broadcast'
    # spark_home = '/data01/app/bigdata/spark'  # local
    spark_home = '/home/hadoop/app/spark'  # test

    pyFiles = ['mysql_utils.py']
    spark_conf = SparkConf()
    spark_conf.setMaster(master).setAppName(app_name).setSparkHome(spark_home)
    sc = SparkContext(conf=spark_conf)
    for path in (pyFiles or []):
        sc.addPyFile(path)

    external_cache = get_api_deviceinfo()

    deviceinfo_b = sc.broadcast(external_cache)


    sc.stop()

if __name__ == '__main__':
    main()


import sys
sys.path.append('/usr/local/lib/python2.7/site-packages')
sys.path.append('/home/hadoop/app/spark/python')
sys.path.append('/home/hadoop/app/spark/python/lib/py4j-0.8.2.1-src.zip')
from pyspark import SparkContext, SparkConf
from mysql_utils import MySQLUtils
master = 'local[2]'
app_name = 'test-broadcast'
# spark_home = '/data01/app/bigdata/spark'  # local
spark_home = '/home/hadoop/app/spark'  # test

pyFiles = ['mysql_utils.py']
spark_conf = SparkConf()
spark_conf.setMaster(master).setAppName(app_name).setSparkHome(spark_home)
sc = SparkContext(conf=spark_conf)
for path in (pyFiles or []):
    sc.addPyFile(path)

external_cache = get_api_deviceinfo()

deviceinfo_b = sc.broadcast(external_cache)

