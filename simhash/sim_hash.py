#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 2020-03-03

@author: Chant
"""
import collections
import hashlib
import logging
import numbers

from .key_funcs import get_keys0
from .tokenizer import tokenize
from .storage import Storage, MemoryStorage, RedisStorage

F = 64  # `f` is the dimensions of fingerprints
K = 7  # `k` is the tolerance


def write_idf_dic(d, path):
    with open(path, 'w') as f:
        for k, v in d.items():
            f.write(f'{k} {v}\n')


def load_idf_dic(path):
    try:
        with open(path, 'r') as f:
            idf_fic = dict()
            for line in f:
                word, idf = line.split(' ')
                idf_fic[word] = float(idf)
        return idf_fic
    except Exception as e:
        print(f'Fail in loading idf_dic from {path}, '
              f'set default as an empty dict, exception: {e}')
        return dict()


JIEBA_IDF_DIC = load_idf_dic('static/idf.txt.big')


def hash_func(x):
    return int(hashlib.md5(x).hexdigest(), 16)


class Simhash(object):

    def __init__(self, value, f=F, hashfunc=hash_func, idf_dic=JIEBA_IDF_DIC):
        """

        :param value: might be an instance of Simhash,
            a string text,
            a integer that represent a Simhash value,
            a list of unweighted tokens (a weight of 1 will be assumed),
            a list of (token, weight) tuples,
            a token -> weight dict.
        :param f: the dimensions of fingerprints
        :param hashfunc: accepts a utf-8 encoded string and returns a
            unsigned integer in at least `f` bits.
        :param idf_dic: a token -> idf_weight dict.
        """
        self.f = f
        self.idf_dic = idf_dic
        self.hashfunc = hashfunc

        if isinstance(value, Simhash):
            self.value = value.value
        elif isinstance(value, str):
            self.build_by_text(value)
        elif isinstance(value, collections.Iterable):
            self.build_by_features(value)
        elif isinstance(value, numbers.Integral):
            self.value = value
        else:
            raise Exception('Bad parameter with type {}'.format(type(value)))

    def __eq__(self, other):
        """Compare two simhashes by their value"""
        return self.value == other.value

    def tf_idf(self, text="处处闻啼鸟，why are you so diao ?"):
        """cut the text and calculate the tf_idf value of each word
        可以考虑使用jieba自带的tf_idf提取器。可导入自己的idf文件。
        https://github.com/fxsjy/jieba#基于-tf-idf-算法的关键词抽取

        :param text: {str} text content
        :return: a dict of word and weight
        """
        words = tokenize(text)
        # count = {k: sum(1 for _ in g) for k, g in groupby(sorted(words))}
        count = collections.Counter(words)  # tf
        for i in count:
            count[i] = count[i] * self.idf_dic.get(i, 5)  # multiplied by idf
        return count

    def build_by_text(self, content):
        return self.build_by_features(self.tf_idf(content))

    def build_by_features(self, features):
        """

        :param features: might be a list of unweighted tokens (a weight of 1
            will be assumed), a list of (token, weight) tuples or
            a token -> weight dict.
        """
        v = [0] * self.f
        masks = [1 << i for i in range(self.f)]
        if isinstance(features, dict):
            features = features.items()
        for f in features:
            if isinstance(f, str):
                h = self.hashfunc(f.encode('utf-8'))
                w = 1
            else:
                assert isinstance(f, collections.Iterable)
                h = self.hashfunc(f[0].encode('utf-8'))
                w = f[1]
            for i in range(self.f):
                v[i] += w if h & masks[i] else -w
        ans = 0
        for i in range(self.f):
            if v[i] > 0:
                ans |= masks[i]
        self.value = ans

    def distance(self, another):
        """hamming distance between self and another Simhash"""
        assert self.f == another.f
        x = (self.value ^ another.value) & ((1 << self.f) - 1)
        ans = 0
        while x:
            ans += 1
            x &= x - 1
        return ans


def to_simhash(text):
    return str(Simhash(text).value)


class SimhashIndex(object):

    def __init__(self, objs=None, storage: Storage = MemoryStorage, key_pre='',
                 f=F, k=K, log=None, key_func=get_keys0):
        """
        优化：存储上其实可以不存obj_id，只存储simhash值，得到hash值后再去查表得到id和内容。能节省一定的存储和计算量。

        :param objs: a list of (obj_id, simhash)
            obj_id is a string, simhash is an instance of Simhash
        :param storage: {Storage} the storage backend
        :param key_pre: {str} prefix to add ahead of the key, when you're dealing with more than  redis storage
        :param f:  the same with the one for Simhash
        :param k: the tolerance
        :param log: logger
        :param key_func: function for keys generation
            `key_func` accepts a Simhash and returns a list of keys,
            which is split from Simhash.value by bits
        """
        self.k = k
        self.f = f
        self.key_pre = key_pre
        self.get_keys = lambda x: key_func(x, f, k, key_pre)

        if log is None:
            self.log = logging.getLogger("simhash")
        else:
            self.log = log

        self.storage = storage

        if objs:
            count = len(objs)
            self.log.info('Initializing %s data.', count)

            for i, q in enumerate(objs):
                if i % 10000 == 0 or i == count - 1:
                    self.log.info('%s/%s', i + 1, count)
                self.add(*q)

    def get_one_near_dup(self, simhash):
        """find one near duplication under the distance tolerance k

        :param simhash: an instance of Simhash
        :return: a tuple of (obj_id, distance)
        """
        assert simhash.f == self.f

        for key in self.get_keys(simhash):
            dups = self.storage.get(key)
            self.log.debug('key:%s', key)
            if len(dups) > 2000:
                self.log.warning('Big bucket found. key:%s, len:%s', key,
                                 len(dups))

            for dup in dups:
                sim2, obj_id = dup.split(',', 1)
                sim2 = Simhash(int(sim2, 16), self.f)

                d = simhash.distance(sim2)
                if d <= self.k:
                    return int(obj_id), d

    def get_near_dups(self, simhash):
        """find all near duplication under the distance tolerance k

        :param simhash: an instance of Simhash
        :return: a list of (obj_id, distance) tuple
        """
        assert simhash.f == self.f

        unique = set()
        id_score = []

        for key in self.get_keys(simhash):
            dups = self.storage.get(key)
            self.log.debug('key:%s', key)
            if len(dups) > 2000:
                self.log.warning('Big bucket found. key:%s, len:%s', key,
                                 len(dups))

            for dup in dups:
                sim2, obj_id = dup.split(',', 1)
                sim2 = Simhash(int(sim2, 16), self.f)

                d = simhash.distance(sim2)
                if d <= self.k:
                    if obj_id not in unique:
                        unique.add(obj_id)
                        id_score.append((int(obj_id), d))
        return id_score

    def get_near_dups2(self, simhash, cur_id):
        """find all near duplication under the distance tolerance k,
        and add current simhash to the index backend.
        查询相似帖, 且将当前帖子id及simhash值存入buckets

        :param simhash: {Simhash}
        :param cur_id: {int or str} 当前查询帖子的id
        :return: a list of (obj_id, distance) tuple
        """
        assert simhash.f == self.f

        id_distance = list()  # [(id, distance),...]
        unique = {str(cur_id)}  # 用于去重，首先去掉当前查询贴
        flag = 1
        for key in self.get_keys(simhash):
            dups = self.storage.get(key)
            self.log.debug('key:%s', key)
            if len(dups) > 3000:
                self.log.warning(
                    f'Big bucket found. key:{key}, len:{len(dups)}')

            for dup in dups:
                sim2, obj_id = dup.split(',', 1)
                sim2 = Simhash(int(sim2, 16), self.f)

                d = simhash.distance(sim2)
                if d <= self.k:
                    if obj_id not in unique:
                        unique.add(obj_id)
                        id_distance.append((int(obj_id), d))
                    # 发现完全重复的帖子，则不添加当前帖子的simhash到redis
                    if d == 0 and obj_id != cur_id:
                        flag = 0
        # 将当前帖子id及simhash值存入redis
        if flag == 1:
            self.add(cur_id, simhash)
        return id_distance

    def add(self, obj_id, simhash):
        """
        `obj_id` is a string
        `simhash` is an instance of Simhash
        """
        assert simhash.f == self.f

        for key in self.get_keys(simhash):
            v = '%x,%s' % (simhash.value, obj_id)
            self.storage.add(key, v)

    def delete(self, obj_id, simhash):
        """
        `obj_id` is a string
        `simhash` is an instance of Simhash
        """
        assert simhash.f == self.f

        for key in self.get_keys(simhash):
            v = '%x,%s' % (simhash.value, obj_id)
            self.storage.delete(key, v)


def test2():
    for i in range(1000):
        a, b = '123456789,13131'.split(',')
    return a, b


def test():
    content = "将短频骄傲普吉岛怕就怕都安排打破奥啪啪"
    post_id = 24962412
    sir = SimhashIndex(storage=RedisStorage())
    sir.get_near_dups(Simhash(content))
    sir.get_near_dups2(Simhash(content), post_id)

    data = {
        1: u'How are you? I Am fine. blar blar blar blar blar Thanks.',
        2: u'How are you i am fine. blar blar blar blar blar than',
        3: u'This is simhash test.',
    }
    objs = [(str(k), Simhash(v)) for k, v in data.items()]
    s1 = Simhash(u'How are you? I Am fine. blar blar blar blar blar Thanks.')
    s2 = Simhash(u'ldadapdopapdampdadll懒得看拉大考虑打开打卡nks.')
    si = SimhashIndex(objs, k=7, storage=MemoryStorage())
    si.get_near_dups(s1)
    si.get_near_dups3(s1)
    si.get_near_dups(s2)
    si.get_near_dups3(s2)
    si.get_near_dups2(s1, 11)
    si.get_near_dups2(s2, 12)
