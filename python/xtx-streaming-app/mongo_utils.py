# -*- coding: utf-8 -*-

from pymongo import MongoClient


def md5(p_str):
    import hashlib

    m = hashlib.md5()
    m.update(p_str)
    return m.hexdigest()


class MongoConnectionPool(object):
    # key = mongo_url_string.md5
    pool = {}

    def __init__(self):
        pass

    @staticmethod
    def get_connection(mongo_url_string):
        client_id = md5(mongo_url_string.strip())
        return MongoConnectionPool.pool.setdefault(client_id, MongoClient(mongo_url_string.strip()))
