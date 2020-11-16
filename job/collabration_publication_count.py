# coding:utf-8
import json

from conf.mysql import Cursor
from conf.redis import RedisTemplate
import traceback
from utils import time
from job.loader.affiliation_loader import AffiliationLoader
import time as ti
from models import cache_const
from models.cache_const import AFFILIATION_COLLABORATION_PUBLICATION_COUNT


def sort(li):
    for i in range(len(li) - 1, 0, -1):
        for j in range(0, i):
            if li[j][1] < li[j + 1][1]:
                t = li[j]
                li[j] = li[j + 1]
                li[j + 1] = t
    return li


def parse_collaboration_info(info):
    collas = info.split(',')
    pub = {}
    for col in collas:
        if len(col) > 0:
            if col in pub:
                pub[col] += 1
            else:
                pub[col] = 1

    res = []
    for k in pub:
        res.append([k, pub[k]])
    res = sort(res)
    if len(res) > 6:
        res = res[0:6]

    for i in range(len(res)):
        affid = res[i][0]
        try:
            Cursor.execute('select name from affiliation where id = {};'.format(affid))
            res[i][0] = Cursor.fetchone()[0]
            res[i] = {res[i][0]: res[i][1]}
        except Exception as e:
            traceback.print_exc()
            print(e)
    return res


def update_one_affiliation_collaboration(id, pipe):
    sql = '''
        select group_concat(aff2.affiliation_id)
        from affiliation_article aff1, affiliation_article aff2
        where aff1.affiliation_id <> aff2.affiliation_id and aff1.article_id = aff2.article_id and aff1.affiliation_id={}
    '''.format(id)

    try:
        Cursor.execute(sql)
        inf =Cursor.fetchone()[0]
        if inf and len(inf)>0:
            res = parse_collaboration_info(inf)
            pipe.set(AFFILIATION_COLLABORATION_PUBLICATION_COUNT.format(id), json.dumps(res), ex = time.MONTH)
    except Exception as e:
        traceback.print_exc()
        print(e)


def affiliation_collaboration_publication_count():
    aff = AffiliationLoader()
    aff.get_affiliation_data()

    ids = aff.affiliation_ids
    pipe = RedisTemplate.pipeline()

    for i in range(0, len(ids)):
        update_one_affiliation_collaboration(str(ids[i]), pipe)
        if (i != 0 and i % 100 == 0) or i == len(ids) - 1:
            ti.sleep(5)
            pipe.execute()


if __name__ == "__main__":
    affiliation_collaboration_publication_count()
