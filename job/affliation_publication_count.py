# coding:utf-8
import json

from conf.mysql import Cursor
from conf.redis import RedisTemplate
import traceback
from utils import time


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


def update_affliation_year_count():
    try:
        Cursor.execute("""
            SELECT
                afar.affiliation_id, group_concat(concat(year(art.date)) separator ",")
            from article art, affiliation_article afar
                where afar.article_id = art.id
            group by afar.affiliation_id
            ;
        """)
        result = Cursor.fetchall()

        pipe = RedisTemplate.pipeline()

        for affInfo in result:
            id = affInfo[0]
            info = json.dumps(parseInfo(affInfo[1].strip())).strip()
            # print(id, info)

            pipe.set("affliationID" + str(id), info, ex=time.MONTH)
        pipe.execute()
    except Exception as e:
        traceback.print_exc()
        print(e)


if __name__ == "__main__":
    update_affliation_year_count()
