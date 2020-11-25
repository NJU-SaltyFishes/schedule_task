# coding:utf-8
import time
from utils.util import chunks
from job.loader.affiliation_loader import AffiliationLoader
from conf.mysql import Cursor
from conf.redis import RedisTemplate
from utils.time import MONTH
import json
from models.cache_const import AFFILIATION_RELATED_NEW_ARTICLE_ID_KEY_TEMPLATE
def update_affiliation_new_article_job():
    affiliation = AffiliationLoader()
    affiliation.get_affiliation_data()
    related_article_list = sorted(affiliation.related_article_dict.items(),key=lambda x:x[0],reverse=False)
    related_new_article_dict = {}
    sql = '''SELECT id FROM article
            WHERE id IN %s
            ORDER BY date DESC LIMIT 1'''
    for affiliations_articles in chunks(related_article_list,500):
        related_dict = {}
        for affiliation_articles in affiliations_articles:
            affiliation_id = affiliation_articles[0]
            articles = affiliation_articles[1]

            #机构没有对应的文章
            if not articles or len(articles)==0:
                continue

            Cursor.execute(sql, (articles,))
            raw_result = list(Cursor.fetchone())
            if raw_result is None:
                continue
            article_id = raw_result[0]
            related_dict[affiliation_id] = article_id
        related_new_article_dict.update(related_dict)
    print("{} related_new_article_dict_len: {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                                                             len(related_new_article_dict)))
    pipeline = RedisTemplate.pipeline()
    for articles in chunks(related_article_list,500):
        for article in articles:
            article_key = AFFILIATION_RELATED_NEW_ARTICLE_ID_KEY_TEMPLATE.format(article[0])
            new_article_id = related_new_article_dict.get(article[0])
            if new_article_id:
                pipeline.set(article_key,json.dumps(new_article_id),ex=1 * MONTH)
        pipeline.execute()
        time.sleep(1)
    print("{} update_affiliation_new_article_job finished".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

if __name__ == "__main__":
    start_time = time.time()
    update_affiliation_new_article_job()
    end_time = time.time()
    duration = end_time - start_time
    print('update_affiliation_new_article_job runtime is:{0:.3f}s'.format(duration))
