#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 2020-03-20

@author: Chant
这里存放的是所有的拆分simhash生成key的函数，
按照论文的写法，在k较大时，容易出现分桶倾斜，某些桶下的simhash量巨大，导致运行速度极慢
源代码作者的意见是，更换hashfunc
"""


def simple_split(hash_str, k):
    """将hash_str按顺序拆为k+1份, 前k份长度相同，第k+1份取剩余的全部，可能会很长"""
    k1s = []
    size = len(hash_str) // (k + 1)
    i = 0
    for i in range(k):
        k1s.append(hash_str[size * i:size * (i + 1)])
    k1s.append(hash_str[size * (i + 1):])
    return k1s


def even_split(hash_str, k):
    """将hash_str按顺序均匀拆为k+1份，将余数均匀分给之前的每一份，任意两份长度差最大为1"""
    quotient, remainder = divmod(len(hash_str), k + 1)
    # 拆分为remainder * (quotient + 1) + (k + 1 - remainder) * 4
    k1s = []
    # 长度+1的部分
    for i in range(remainder):
        size = quotient + 1
        k1s.append(hash_str[size * i:size * (i + 1)])
    # 原始长度的部分
    for i in range(k + 1 - remainder):
        size = quotient
        k1s.append(hash_str[size * i:size * (i + 1)])
    return k1s


def get_keys(simhash, f=64, k=3, key_pre=''):
    """拆分一次，生成key，加上前缀key_pre"""
    # 补0
    hash_str = bin(simhash.value)[2:]
    if len(hash_str) != f:
        hash_str = "0" * (f - len(hash_str)) + hash_str
    k1s = even_split(hash_str, k)  # 拆分
    # k - idx 是为了和之前的位运算的get_keys保持一致
    return ['%s%x:%s' % (key_pre, int(i, 2), k - idx) for idx, i in
            enumerate(k1s)]


def get_keys2(simhash, f=64, k=3, key_pre=''):
    """拆分2次，生成key，加上前缀key_pre"""
    # 补0
    hash_str = bin(simhash.value)[2:]
    if len(hash_str) != f:
        hash_str = "0" * (f - len(hash_str)) + hash_str
    # 拆分
    keys = []
    k1s = even_split(hash_str, k)
    for idx1, k1 in enumerate(k1s):
        # todo: 优化：可以循环拿走pop再补充append，因为顺序其实无关
        # 不能用i != k1来判断，因为i可能会有重复的，比如有好几个'0000'
        left = ''.join([i for _idx, i in enumerate(k1s) if _idx != idx1])
        k2s = even_split(left, k)
        for idx2, k2 in enumerate(k2s):
            # key = f'{k1}:{idx1}:{k2}:{idx2}'
            key = '%s%x:%s:%s:%x' % (
                key_pre, int(k1, 2), idx1, int(k2, 2), idx2)
            keys.append(key)
    return keys


def get_keys0(simhash, f=64, k=3, key_pre=''):
    """位运算的方式生成key，加上前缀key_pre，yield省内存，但是速度比get_keys1慢"""
    offsets = [f // (k + 1) * i for i in range(k + 1)]
    for i, offset in enumerate(offsets):
        if i == (len(offsets) - 1):
            m = 2 ** (f - offset) - 1
        else:
            m = 2 ** (offsets[i + 1] - offset) - 1
        c = simhash.value >> offset & m
        yield '%s%x:%x' % (key_pre, c, i)
