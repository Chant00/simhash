#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 2020-03-20

@author: Chant
"""
import collections
import redis


class Storage(object):
    def __init__(self):
        pass

    def get(self, k):
        pass

    def add(self, k, v):
        pass

    def remove(self, k, v):
        pass

    def clear(self):
        pass


class MemoryMapStorage(Storage):
    def __init__(self):
        """use a python dict to store the simhash -> obj_id map"""
        super().__init__()
        self.map = dict()

    def get(self, k):
        return self.map.get(k)

    def add(self, k, v):
        self.map[k] = v

    def remove(self, k, v):
        if k in self.map:
            self.map.pop(k)

    def clear(self):
        self.map.clear()


class RedisMapStorage(Storage):
    def __init__(self, r: redis.client.Redis, redis_key):
        """use a redis map to store the simhash -> obj_id map"""
        super().__init__()
        self.redis_key = redis_key
        self.r = r

    def get(self, k):
        return self.r.hget(self.redis_key, k)

    def add(self, k, v):
        self.r.hset(self.redis_key, k, v)

    def remove(self, k, v):
        self.r.hdel(self.redis_key, k)

    def clear(self):
        self.r.expire(self.redis_key, 0)


class MemoryStorage(Storage):
    def __init__(self):
        super().__init__()
        self.bucket = collections.defaultdict(set)

    def get(self, k):
        return self.bucket.get(k)

    def add(self, k, v):
        self.bucket[k].add(v)

    def remove(self, k, v):
        if v in self.bucket[k]:
            self.bucket[k].remove(v)

    def clear(self):
        self.bucket.clear()


class RedisStorage(Storage):

    def __init__(self, r: redis.client.Redis,
                 expire=7 * 24 * 60 * 60,
                 keys_key='bucket_keys'):
        super().__init__()
        self.r = r
        self.pipe = r.pipeline()
        self.expire = expire
        self.bucket_keys = set()
        self.keys_key = keys_key

    def get(self, k):
        return self.r.smembers(k)

    def add(self, k, v):
        self.r.sadd(k, v)
        self.r.expire(k, self.expire)
        # 记录下所有的bucket的key，方便统一删除
        self.r.sadd(self.keys_key, k)

    def remove(self, k, v):
        self.r.srem(k, v)

    def clear(self, batch_size=1000):
        keys = self.r.smembers(self.keys_key)

        for i, key in enumerate(keys):
            self.pipe.expire(key, 0)
            if i % batch_size == 0:
                self.pipe.execute()
                print(f'批量删除redis中数据，删除至{i}条')
