# coding:utf-8

import time
import json
import traceback

from conf.mysql import Cursor
from conf.redis import RedisTemplate
from models.cache_const import AFFILIATION_ALL_ID, AFFILIATION_RELATED_ARTICLE_ID_KEY_TEMPLATE,AFFILIATION_RELATED_AUTHOR_ID_KEY_TEMPLATE
from utils.time import HOUR
from utils.util import chunks

from functools import lru_cache


class AffiliationLoader:
    def __init__(self):
        self.affiliation_ids = []
        self.related_article_dict = {}
        self.related_author_dict = {}


    def load_affiliation_data(self):
        self._load_affiliation_ids()
        self._load_related_article_dict()
        self._load_related_author_dict()
        self._save_to_redis()

    def get_affiliation_data(self):
        try:
            affiliation_ids = RedisTemplate.get(AFFILIATION_ALL_ID)
            affiliation_ids = json.loads(affiliation_ids)
            self.affiliation_ids = affiliation_ids
            for ids in chunks(self.affiliation_ids, 500):
                article_keys = [AFFILIATION_RELATED_ARTICLE_ID_KEY_TEMPLATE.format(i) for i in ids]
                article_values = RedisTemplate.mget(keys=article_keys)
                article_id_dict = {}
                author_keys = [AFFILIATION_RELATED_AUTHOR_ID_KEY_TEMPLATE.format(i) for i in ids]
                author_values = RedisTemplate.mget(keys = author_keys)
                author_id_dict = {}
                for i in range(len(ids)):
                    if article_values[i] is None:
                        article_id_dict[ids[i]] = []
                    else:
                        article_id_dict[ids[i]] = json.loads(article_values[i])
                    if author_values[i] is None:
                        author_id_dict[ids[i]] = []
                    else:
                        author_id_dict[ids[i]] = json.loads(author_values[i])
                self.related_article_dict.update(article_id_dict)
                self.related_author_dict.update(author_id_dict)
        except Exception:
            traceback.format_exc()
            return

    @lru_cache()
    def _load_affiliation_ids(self):
        sql = '''SELECT id FROM affiliation'''
        Cursor.execute(sql)
        raw_result = list(Cursor.fetchall())
        self.affiliation_ids = [i[0] for i in raw_result]
        print("{} affiliation_total_count: {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(self.affiliation_ids)))

    @lru_cache()
    def _load_related_author_dict(self):

        for ids in chunks(self.affiliation_ids, 500):
            sql = '''
                    SELECT affiliation_id, group_concat(author_id) as author_ids
                    FROM affiliation_author
                    WHERE affiliation_id IN %s
                    GROUP BY affiliation_id 
            '''
            Cursor.execute(sql,(ids,))
            raw_result = list(Cursor.fetchall())
            related_dict = {}
            for info in raw_result:
                if info is None:
                    continue
                if info[1] is None or len(info) == 0:
                    self.related_article_dict[info[0]] = []
                else:
                    related_dict[info[0]] = info[1].split(',')
            self.related_author_dict.update(related_dict)
            time.sleep(1)
        print("{} related_author_dict_len: {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),len(self.related_author_dict)))

    @lru_cache()
    def _load_related_article_dict(self):

        for ids in chunks(self.affiliation_ids, 500):
            sql = '''
                    SELECT affiliation_id, group_concat(article_id) as article_ids
                    FROM affiliation_article
                    WHERE affiliation_id IN %s
                    GROUP BY affiliation_id
                '''
            Cursor.execute(sql, (ids,))
            raw_result = list(Cursor.fetchall())
            related_dict = {}
            for info in raw_result:
                if info is None:
                    continue
                if info[1] is None or len(info) == 0:
                    self.related_article_dict[info[0]] = []
                else:
                    related_dict[info[0]] = info[1].split(',')
            self.related_article_dict.update(related_dict)
            time.sleep(1)
        print("{} related_article_dict_len: {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), len(self.related_article_dict)))

    def _save_to_redis(self):
        if len(self.affiliation_ids) > 0:
            RedisTemplate.set(AFFILIATION_ALL_ID, json.dumps(self.affiliation_ids), ex=1*30*24*HOUR)

        pipeline = RedisTemplate.pipeline()
        for ids in chunks(self.affiliation_ids, 500):
            for _id in ids:
                article_key = AFFILIATION_RELATED_ARTICLE_ID_KEY_TEMPLATE.format(_id)
                author_key = AFFILIATION_RELATED_AUTHOR_ID_KEY_TEMPLATE.format(_id)
                related_article_ids = self.related_article_dict.get(_id)
                related_author_ids = self.related_author_dict.get(_id)
                if related_article_ids:
                    pipeline.set(article_key, json.dumps(self.related_article_dict[_id]), ex=1 * 30 * 24 * HOUR)
                if related_author_ids:
                    pipeline.set(author_key,json.dumps(self.related_author_dict[_id]),ex=1 * 30 * 24 * HOUR)
            pipeline.execute()
            time.sleep(1)
        print("{} save_to_redis finished".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))