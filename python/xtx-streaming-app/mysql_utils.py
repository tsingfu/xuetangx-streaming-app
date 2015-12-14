# -*- coding: utf-8 -*-

import sys
# For MySQLdb
sys.path.append('/usr/local/lib/python2.7/site-packages')

import MySQLdb


class MySQLUtils:
    cache_pools = {}
    conn_pool = {}

    @staticmethod
    def get_connection(host, db, user, password=None, port=3306, charset='utf8'):
        conn_id = user + '@' + host + ':' + str(port) + '/' + db \
                  + "#password=" + ('' if password is None else str(password))
        conn = MySQLUtils.conn_pool.setdefault(
            conn_id,
            MySQLdb.connect(host=host, user=user, passwd=password, port=port, charset=charset))
        conn.select_db(db)
        return conn

    @staticmethod
    def get_cursor_class(cursor_class_mode):
        CURSOR_MODE = 0
        DICTCURSOR_MODE = 1
        SSCURSOR_MODE = 2
        SSDICTCURSOR_MODE = 3

        if cursor_class_mode == CURSOR_MODE:
            cursor_class = MySQLdb.cursors.Cursor
        elif cursor_class_mode == DICTCURSOR_MODE:
            cursor_class = MySQLdb.cursors.DictCursor
        # elif cursor_class_mode == SSCURSOR_MODE:
        #     cursor_class = MySQLdb.cursors.SSCursor
        # elif cursor_class_mode == SSDICTCURSOR_MODE:
        #     cursor_class = MySQLdb.cursors.SSDictCursor
        else:
            raise Exception('unsupported cursor_style')

        return cursor_class

    @staticmethod
    def query(conn, sql_text, key_name, sql_args=None, fetch_size=None):
        cursor_class_mode = 1
        cursor_class = MySQLUtils.get_cursor_class(cursor_class_mode)
        cur = conn.cursor(cursorclass=cursor_class)

        if sql_args:
            cur.execute(sql_text, sql_args)
        else:
            cur.execute(sql_text)

        result = {}
        if isinstance(fetch_size, int):
            rs = cur.fetchmany(fetch_size)
            while rs:
                for row_dict in rs:
                    key_value = row_dict[key_name]
                    result[key_value] = row_dict
                rs = cur.fetchmany(fetch_size)
        else:
            rs = cur.fetchall()
            for row_dict in rs:
                key_value = row_dict[key_name]
                result[key_value] = row_dict

        cur.close()

        return result

    @staticmethod
    def cache(result_dicts, cache_id, update_flag=False):
        cache_pool = MySQLUtils.cache_pools.setdefault(cache_id, {})
        for k, v in result_dicts.iteritems():
            if update_flag or not (k in cache_pool):
                cache_pool[k] = v

        return cache_pool


def test1():
    # host = '192.168.9.228'
    host = '10.0.0.168'
    db = 'edxapp'
    # user = 'root'
    # password = ''
    user = 'mysql_ro'
    password = 'xuetangx.com168mysql'

    # port = 3306

    conn = MySQLUtils.get_connection(host=host, db=db, user=user, password=password)
    # select uuid, channel, event, uid from edxapp.api_deviceinfo where uuid = 'e37938db3f6821a2796cacfd0c7520f1';
    sql1 = "select uuid, channel, event, uid from edxapp.api_deviceinfo where uuid = %s"
    result1 = MySQLUtils.query(conn=conn, sql_text=sql1, key_name='uuid', sql_args=('e37938db3f6821a2796cacfd0c7520f1', ))
    print result1

    result2 = MySQLUtils.query(conn=conn, sql_text=sql1, key_name='uuid', sql_args=('a1ae9b0f1aa28e9162613d15afc97076', ))
    print result2

    result3 = MySQLUtils.query(conn=conn, sql_text=sql1, key_name='uuid', sql_args=('727f6cf87195b991d8d1b04683c02b95', ))
    print result3

    sql2 = "select course_id, course_type, owner, status, date_format(date_add(start, interval +8 hour), '%Y-%m-%d %H:%i:%s') as start, date_format(date_add(end, interval +8 hour), '%Y-%m-%d %H:%i:%s') as end from edxapp.course_meta_course where course_id = %s"


    conn.close()


if __name__ == '__main__':
    test1()

