# -*- coding: utf-8 -*-

# For elasticsearch
import sys
sys.path.append('/data01/data/datadir_python_3rd_pacakge/usr/local/lib/python2.7/site-packages')

from elasticsearch import Elasticsearch
from streaming_app_utils import dict_merge


def md5(p_str):
    import hashlib

    m = hashlib.md5()
    m.update(p_str)
    return m.hexdigest()


class EsConnectionPool(object):
    pool = {}

    def __init__(self):
        pass

    @staticmethod
    def get_connection(es_host):
        client_key = None
        if isinstance(es_host, str):
            client_key = es_host.strip()
        elif isinstance(es_host, list):
            client_key = ','.join(x.strip() for x in es_host)
        # print('client_key before md5 = ', client_key)
        client_id = md5(client_key)
        return EsConnectionPool.pool.setdefault(client_id, Elasticsearch(es_host))

    @staticmethod
    def close_connection(es_host):
        client_key = None
        if isinstance(es_host, str):
            client_key = es_host
        elif isinstance(es_host, list):
            client_key = ','.join(es_host)
        client_key = md5(client_key)
        if client_key in EsConnectionPool.pool:
            EsConnectionPool.pool[client_key] = None
            del EsConnectionPool.pool[client_key]


class ESWriter(object):

    @staticmethod
    def fun_output_in_rdd_mapPartitions(iter_x, step_conf, pre_output_conf):
        """
        输入: Row
        输出: obj_dict
        """
        STATISTIC_INFO_PREFIX_KEY = 'statistic.'
        print('= = ' * 5, 'pre_output_conf = ', [(k, v) for k, v in pre_output_conf.iteritems()
                                                 if k.startswith(STATISTIC_INFO_PREFIX_KEY)])
        added_info_dict = dict([(k[10:], v) for k, v in pre_output_conf.iteritems()
                                if k.startswith(STATISTIC_INFO_PREFIX_KEY)])

        print('= = ' * 10, 'added_info_dict =', added_info_dict)
        import json

        idx = pre_output_conf['index']
        typ = pre_output_conf['type']
        es_host = pre_output_conf['serverPort.list'].split(',')
        es = EsConnectionPool.get_connection(es_host)

        # 取 id
        value_key = pre_output_conf['value.key']

        # 添加 raw_data
        for row in iter_x:
            obj = dict_merge(row.asDict(), added_info_dict)  # value_key 字段取值需要更新

            raw_data_tmp = dict([(k, v) for k, v in obj.iteritems() if k != value_key])  # 去掉 value_key 构造 id
            md5id = md5(json.dumps(raw_data_tmp))

            # 先查询，在更新
            resp = es.get(index=idx, doc_type=typ, id=md5id, ignore=404)
            # print('= = ' * 5, type(resp), 'get(index=' + idx + ', doc_type=' + typ + ', id=' + md5id +
            #       '), resp=', resp, ', with obj = ', obj)
            # 常见 resp
            # {u'status': 404, u'error': u'IndexMissingException[[opkpi] missing]'}

            if resp.get('status') == 404:
                # TODO: 创建索引，插入
                pass
            elif resp.get('exists'):
                value_update = obj[value_key] + resp['_source'][value_key]
                # 更新value_key 取值
                obj[value_key] = value_update
                obj['raw_data'] = json.dumps(obj)
            else:
                obj['raw_data'] = json.dumps(obj)

            es.index(index=idx, doc_type=typ, id=md5id, body=obj)

            print('= = ' * 10 + '[myapp ESWriter.output.fun_output_in_rdd_mapPartitions] obj = ', obj)

            yield obj


    @staticmethod
    def fun_output_in_rdd_mapPartitions_new(iter_x, step_conf, pre_output_conf):
        """
        构造输出到es的结构，rdd.collection返回到driver端，然后统一执行
        输入: Row
        输出: obj_dict
        """
        STATISTIC_INFO_PREFIX_KEY = 'statistic.'
        print('= = ' * 5, 'pre_output_conf = ', [(k, v) for k, v in pre_output_conf.iteritems()
                                                 if k.startswith(STATISTIC_INFO_PREFIX_KEY)])
        added_info_dict = dict([(k[10:], v) for k, v in pre_output_conf.iteritems()
                                if k.startswith(STATISTIC_INFO_PREFIX_KEY)])

        print('= = ' * 10, 'added_info_dict =', added_info_dict)

        idx = pre_output_conf['index']
        typ = pre_output_conf['type']
        value_key = pre_output_conf['value.key']

        # 添加 raw_data
        for row in iter_x:
            ret = {}
            obj = dict_merge(row.asDict(), added_info_dict)

            raw_data_tmp = dict([(k, v) for k, v in obj.iteritems() if k != value_key])  # 去掉 value_key 构造 id

            ret['serverPort.list'] = pre_output_conf['serverPort.list']
            ret['index'] = idx
            ret['doct_type'] = typ
            ret['id_raw_data_without_value_key'] = raw_data_tmp

            ret['value_key'] = value_key
            ret['body'] = obj
            print('= = ' * 10 + '[myapp ESWriter.output.fun_output_in_rdd_mapPartitions] obj = ', obj)
            print('= = ' * 10 + '[myapp ESWriter.output.fun_output_in_rdd_mapPartitions] ret = ', ret)

            yield ret


    @staticmethod
    def output(rdd, step_conf, pre_output_conf):
        """
        输入: RDD[Row]
        输出: RDD[obj_dict]
        """
        rdd2 = rdd.mapPartitions(
            # 调整es写入方式
            # fun_output_in_rdd_mapPartitions： executor写ES
            # lambda iter_x: ESWriter.fun_output_in_rdd_mapPartitions(iter_x, step_conf, pre_output_conf))

            # fun_output_in_rdd_mapPartitions_new： driver写ES
            lambda iter_x: ESWriter.fun_output_in_rdd_mapPartitions_new(iter_x, step_conf, pre_output_conf))
        return rdd2

    @staticmethod
    def driver_output(obj_list):
        """
        在driver端输出统计信息
        输入: list[obj]， 其中obj 结构: {serverPort.list:, index:, doc_type:, id_raw_data_without_value_key:, body:, value_key:}
        """

        for obj in obj_list:
            import json

            es_host = obj['serverPort.list'].split(',')
            es = EsConnectionPool.get_connection(es_host)
            idx = obj['index']
            typ = obj['doct_type']
            id_raw_data_without_value_key = obj['id_raw_data_without_value_key']
            md5id = md5(json.dumps(id_raw_data_without_value_key))
            value_key = obj['value_key']
            body = obj['body']  # 需要更新 key_value 字段取值，添加 raw_data

            # 先查询，在更新
            resp = es.get(index=idx, doc_type=typ, id=md5id, ignore=404)
            print('= = ' * 5, '[myapp ESWriter.driver_output] resp = ', resp, ', obj = ', obj)
            # 常见 resp
            # {u'status': 404, u'error': u'IndexMissingException[[opkpi] missing]'}

            if resp.get('status') == 404:
                # TODO: 创建索引，插入
                pass
            elif resp.get('exists') == True:
                value_update = body[value_key] + resp['_source'][value_key]
                # 更新value_key 取值
                body[value_key] = value_update
                body['raw_data'] = json.dumps(body)
            else:
                body['raw_data'] = json.dumps(body)

            es.index(index=idx, doc_type=typ, id=md5id, body=body)


def test1():
    es_host1 = '192.168.9.164'
    es1 = EsConnectionPool.get_connection(es_host1)
    es1_1 = EsConnectionPool.get_connection(es_host1)

    print('es1 == es1_1 ? or not = ', es1 == es1_1)
    es_host2 = '192.168.9.164:9200'
    es2 = EsConnectionPool.get_connection(es_host2)
    print('es1 == es2 ? or not = ', es1 == es2)

    print(len(EsConnectionPool.pool))
    EsConnectionPool.close_connection(es_host1)
    print(len(EsConnectionPool.pool))


if __name__ == '__main__':
    test1()

