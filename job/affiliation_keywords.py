# coding:utf-8
import time
from job.loader.affiliation_loader import AffiliationLoader
from utils.util import chunks
from conf.mysql import Cursor
from conf.redis import RedisTemplate
from models.cache_const import AFFILIATION_RELATED_KEYWORD_KEY_TEMPLATE
import json
from utils.time import MONTH
def parseKeyword(keyword):
    return {"keyword_id":keyword[0],
            "keyword_desc":keyword[1],
            "keyword_appear_num":keyword[2]}
def update_affiliation_keyword_job():
    affiliation = AffiliationLoader()
    affiliation.get_affiliation_data()
    related_article_list = sorted(affiliation.related_article_dict.items(), key=lambda x: x[0], reverse=False)
    related_keyword_dict = {}
    sql = '''SELECT keyword_id,keyword_desc,COUNT(article_id)AS num FROM keyword_article
        WHERE article_id IN %s
        GROUP BY keyword_id,keyword_desc
        ORDER BY num DESC'''
    for affiliations_articles in chunks(related_article_list,500):
        related_dict = {}
        for affiliation_articles in affiliations_articles:
            affiliation_id = affiliation_articles[0]
            articles = affiliation_articles[1]

            #机构没有对应的文章
            if not articles or len(articles)==0:
                continue

            Cursor.execute(sql, (articles,))
            raw_result = list(Cursor.fetchall())
            if raw_result is None:
                continue
            keywords = list(map(parseKeyword,raw_result))
            related_dict[affiliation_id] = keywords
        related_keyword_dict.update(related_dict)
    print("{} related_keyword_dict_len: {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                                                             len(related_keyword_dict)))
    pipeline = RedisTemplate.pipeline()
    for articles in chunks(related_article_list, 500):
        for article in articles:
            article_key = AFFILIATION_RELATED_KEYWORD_KEY_TEMPLATE.format(article[0])
            keywords = related_keyword_dict.get(article[0])
            if keywords:
                pipeline.set(article_key, json.dumps(keywords))
        pipeline.execute()
        time.sleep(1)
    print("{} update_affiliation_keyword_job".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

if __name__ == "__main__":
    start_time = time.time()
    update_affiliation_keyword_job()
    end_time = time.time()
    duration = end_time - start_time
    print('update_affiliation_keyword_job runtime is:{0:.3f}s'.format(duration))
