#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 2020-03-20

@author: Chant
"""
import os
import re
import urllib.request

import jieba

BASE_DIR = os.path.split(os.path.realpath(__file__))[0]

MATCH_CH = re.compile('^[\u4e00-\u9fa5]*$')  # 匹配中文
MATCH_CH_EN = re.compile('^[\u4e00-\u9fcca-zA-Z]*$')  # 匹配中英文
HTML_TAG_PATTERN = re.compile(r'<[^>]+>', re.S)  # html标签正则

# 用户自定义词典
DIR_OF_MEDICAL_BEAUTY = os.path.join(BASE_DIR, '../static/userdict.dic')
# 所有词典的集合
DICTS = (DIR_OF_MEDICAL_BEAUTY,)
# 中文停用词表
DIR_OF_STOP_WORDS = os.path.join(BASE_DIR, '../static/stop_words.txt')


def load_user_dict_for_jieba(user_dicts=DICTS):
    """使用jieba加载用户自定义词典"""
    for user_dict in user_dicts:
        jieba.load_userdict(user_dict)
        print(f'loading user define dict from {user_dict}')


def get_stop_words():
    """读取停用词表，返回一个包含停用词的set

    :return: {set} 停用词集合
    """
    with open(DIR_OF_STOP_WORDS, encoding='utf8') as f:
        stop_words = set(i.rstrip('\n') for i in f.readlines())
    print(f'loading stop words from {DIR_OF_STOP_WORDS}')
    return stop_words


STOP_WORDS = get_stop_words()  # 加载停用词


def tokenize(content):
    """分词且只保留中英文。

    :param content: {str} 要分词的字符串
    :return: {list} [word, word, ...] 分词后的单词集合
    """
    if not jieba.dt.initialized:
        load_user_dict_for_jieba()  # 加载自定义词典

    content = remove_html_tags(content)
    return [i for i in jieba.cut(content) if
            is_ch_en(i) and i not in STOP_WORDS]


def remove_html_tags(content):
    """去除content中的<p></p>等html标签"""
    cleaned_content = HTML_TAG_PATTERN.sub('', content)
    return cleaned_content


def is_chinese(content):
    """检查是否是纯中文"""
    return MATCH_CH.match(content) is not None


def is_ch_en(content):
    """检查是否是纯中文或英文"""
    return MATCH_CH_EN.match(content)


def get_html(url):
    _html = urllib.request.urlopen(url).read()
    return str(_html)


def demo():
    tmp = "处处闻啼鸟，why are you so diao ?"
    print(tokenize(tmp))


if __name__ == '__main__':
    demo()
