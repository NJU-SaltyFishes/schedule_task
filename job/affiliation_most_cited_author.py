# coding:utf-8
from job.loader.affiliation_loader import AffiliationLoader
from models.cache_const import AFFILIATION_RELATED_MOST_CITED_AUTHOR_KEY_TEMPLATE
from utils.util import chunks
from conf.mysql import Cursor
from conf.redis import RedisTemplate
import json
import time
from utils.time import MONTH
def update_affiliation_most_cited_author_job():
    affiliation = AffiliationLoader()
    affiliation.get_affiliation_data()
    related_author_list = sorted(affiliation.related_author_dict.items(),key=lambda x:x[0],reverse=False)
    related_most_cited_author_dict = {}
    sql = '''
                        SELECT aut.id AS authorId,aut.name AS authorName,SUM(art.citation_count) AS citedNum
                        FROM article art,author aut,author_article auar
                        WHERE aut.id IN %s
                        AND aut.id = auar.author_id AND art.id = auar.article_id
                        GROUP BY authorId,authorName
                        order by citedNum desc LIMIT 1
                '''
    for affiliations_authors in chunks(related_author_list,500):
        related_dict = {}
        for affiliation_authors in affiliations_authors:
            affiliation_id = affiliation_authors[0]
            authors = affiliation_authors[1]
            Cursor.execute(sql, (authors,))
            raw_result = list(Cursor.fetchone())
            if raw_result is None:
                continue
            author_id = raw_result[0]
            author_name = raw_result[1]
            cited_num = int(str(raw_result[2]))
            related_dict[affiliation_id] = (author_id,author_name,cited_num)
        related_most_cited_author_dict.update(related_dict)
    print("{} related_most_cited_author_dict_len: {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                                                   len(related_most_cited_author_dict)))

    pipeline = RedisTemplate.pipeline()
    for authors in chunks(related_author_list, 500):
        for author in authors:
            author_key = AFFILIATION_RELATED_MOST_CITED_AUTHOR_KEY_TEMPLATE.format(author[0])
            most_cited_author = related_most_cited_author_dict.get(author[0])
            if most_cited_author:
                pipeline.set(author_key, json.dumps(most_cited_author), ex=1 * MONTH)
        pipeline.execute()
        time.sleep(1)
    print("{} save_to_redis finished".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))





if __name__ == "__main__":
    start = time.time()
    update_affiliation_most_cited_author_job()
    end = time.time()
    c = end - start
    print('Format Runtime is:{0:.3f}s'.format(c))