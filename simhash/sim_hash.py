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
from .storage import Storage, MemoryStorage, RedisStorage, MemoryMapStorage

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

    def __init__(self, objs=None,
                 storage: Storage = MemoryStorage(),
                 map_storage: Storage = MemoryMapStorage(),
                 key_pre='',
                 f=F, k=K, log=None, key_func=get_keys0, with_id=True):
        """split simhash into keys, index them into buckets,
        provide the function to find near duplications.

        :param objs: a list of (obj_id, simhash)
            obj_id is a string, simhash is an instance of Simhash
        :param map_storage: {Storage} the storage for simhash -> obj_id map
        :param storage: {Storage} the storage backend
        :param key_pre: {str} prefix to add ahead of the key,
            when you're dealing with more than 2 corpus with redis storage,
            you'll need this prefix to prevent the mixture of the keys
        :param f: {int} the same with the one for Simhash
        :param k: {int} the tolerance
        :param log: {logger}
        :param key_func: function for keys generation
            `key_func` accepts a Simhash and returns a list of keys,
            which is split from Simhash.value by bits
        """
        self.k = k
        self.f = f
        self.key_pre = key_pre
        self.get_keys = lambda x: key_func(x, f, k, key_pre)
        self.storage = storage
        if with_id:
            self.with_id = with_id
            self.hash2id = map_storage

        if log is None:
            self.log = logging.getLogger("simhash")
        else:
            self.log = log

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
        :return: return a (obj_id, distance) tuple if self.with_id set
            to True else return a (hex simhash, distance) tuple
        """
        assert simhash.f == self.f

        for key in self.get_keys(simhash):
            dups = self.storage.get(key)
            self.log.debug('key:%s', key)
            if len(dups) > 2000:
                self.log.warning('Big bucket found. key:%s, len:%s', key,
                                 len(dups))

            for dup_hex in dups:
                dup_hash = Simhash(int(dup_hex, 16), self.f)
                d = simhash.distance(dup_hash)
                if d <= self.k:
                    if self.with_id:
                        return int(self.hash2id.get(dup_hex)), d
                    else:
                        return dup_hash, d
        return None, None

    def get_near_dups(self, simhash):
        """find all near duplication under the distance tolerance k.
        use this function when you're dealing with historical data

        :param simhash: an instance of Simhash
        :return: return a list of (obj_id, distance) tuple if self.with_id set
            to True else return a list of (hex simhash, distance) tuple
        """
        assert simhash.f == self.f

        unique = set()  # to distinct the result
        id_dist = []  # [(id, distance),...]

        for key in self.get_keys(simhash):
            dups = self.storage.get(key)
            self.log.debug('key:%s', key)
            if len(dups) > 2000:
                self.log.warning('Big bucket found. key:%s, len:%s', key,
                                 len(dups))

            for dup_hex in dups:
                dup_hash = Simhash(int(dup_hex, 16), self.f)

                d = simhash.distance(dup_hash)
                if d <= self.k:
                    if dup_hex not in unique:
                        unique.add(dup_hex)
                        if self.with_id:
                            id_dist.append((int(self.hash2id.get(dup_hex)), d))
                        else:
                            id_dist.append((dup_hex, d))
        return id_dist

    def get_near_dups2(self, simhash, cur_id):
        """find all near duplication under the distance tolerance k, meanwhile,
        add current simhash to the storage.
        use this function when you're dealing with real-time query

        :param simhash: {Simhash}
        :param cur_id: {int or str} 当前查询帖子的id
        :return: return a list of (obj_id, distance) tuple if self.with_id set
            to True else return a list of (hex simhash, distance) tuple
        """
        assert simhash.f == self.f

        id_dist = list()  # [(id, distance),...]
        unique = set()  # to distinct the result
        flag = 1

        for key in self.get_keys(simhash):
            dups = self.storage.get(key)
            self.log.debug('key:%s', key)
            if len(dups) > 3000:
                self.log.warning(
                    f'Big bucket found. key:{key}, len:{len(dups)}')

            for dup_hex in dups:
                dup_hash = Simhash(int(dup_hex, 16), self.f)
                d = simhash.distance(dup_hash)

                if d <= self.k:
                    if dup_hex not in unique:
                        unique.add(dup_hex)
                        if self.with_id:
                            id_dist.append((int(self.hash2id.get(dup_hex)), d))
                        else:
                            id_dist.append((dup_hex, d))
                    if d == 0:
                        flag = 0
        # No completely duplicate simhash found,
        # adding current simhash to the storage
        if flag == 1:
            self.add(cur_id, simhash)
        return id_dist

    def add(self, obj_id, simhash):
        """adding the simhash to the storage"""
        assert simhash.f == self.f, f"index's f={self.f},simhash's f={simhash.f}"

        v = '%x' % simhash.value  # format to hex
        if self.with_id:
            self.hash2id.add(v, obj_id)
        for key in self.get_keys(simhash):
            self.storage.add(key, v)

    def remove(self, simhash):
        """remove the simhash from the storage"""
        assert simhash.f == self.f

        v = '%x' % simhash.value  # format to hex
        if self.with_id:
            self.hash2id.remove(v, 0)
        for key in self.get_keys(simhash):
            self.storage.remove(key, v)


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
