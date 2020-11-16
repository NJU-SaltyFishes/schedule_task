# coding:utf-8
import json

from conf.mysql import Cursor
from conf.redis import RedisTemplate
import traceback
from utils import time
from models import cache_const
import time as ti


def parseInfo(info: str):
    dict = {
        2010: 0,
        2011: 0,
        2012: 0,
        2013: 0,
        2014: 0,
        2015: 0,
        2016: 0,
        2017: 0,
        2018: 0,
        2019: 0,
        2020: 0,
        2021: 0,
    }

    info = info.split(',')
    for k in info:
        if len(k) == 4:
            dict[int(k)] += 1
    return dict


def update_one_affiliation_year_count(id, pipe):
    try:
        Cursor.execute('''
            select
                group_concat(concat(year(art.date)) separator ",")
            from article art, affiliation_article afar
                where afar.article_id = art.id and afar.affiliation_id = 
        ''' + id + ";")
        res = json.dumps(parseInfo(Cursor.fetchone()[0].strip()))
        pipe.set(cache_const.AFFILIATION_YEAR_COUNT.format(id), res)
    except Exception as e:
        traceback.print_exc()
        print(e)


from job.loader.affiliation_loader import AffiliationLoader


def update_affiliation_year_count():
    aff = AffiliationLoader()
    aff.get_affiliation_data()
    pipe = RedisTemplate.pipeline()
    for i in range(0, len(aff.affiliation_ids)):
        update_one_affiliation_year_count(str(aff.affiliation_ids[i]), pipe)
        if (i != 0 and i % 100 == 0) or i == len(aff.affiliation_ids) - 1:
            ti.sleep(5)
            pipe.execute()


# def update_affliation_year_count():
#
#     try:
#         Cursor.execute("""
#             SELECT
#                 afar.affiliation_id, group_concat(concat(year(art.date)) separator ",")
#             from article art, affiliation_article afar
#                 where afar.article_id = art.id
#             group by afar.affiliation_id
#             ;
#         """)
#         result = Cursor.fetchall()
#
#         pipe = RedisTemplate.pipeline()
#
#         for affInfo in result:
#             id = affInfo[0]
#             info = json.dumps(parseInfo(affInfo[1].strip())).strip()
#             # print(id, info)
#
#             pipe.set("affliationID" + str(id), info,  ex = time.MONTH)
#         pipe.execute()
#     except Exception as e:
#         traceback.print_exc()
#         print(e)


if __name__ == "__main__":
    update_affiliation_year_count()
