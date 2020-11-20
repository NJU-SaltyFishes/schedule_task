# coding:utf-8
from job.loader.affiliation_loader import AffiliationLoader
from models.cache_const import AFFILIATION_RELATED_MOST_CITED_AUTHOR_KEY_TEMPLATE
from utils.util import chunks
from conf.mysql import Cursor,Connection
from conf.redis import RedisTemplate
import json
import time
import datetime
from utils.time import MONTH
from decimal import Decimal

def update_affiliation_database_job():
    affiliation = AffiliationLoader()
    affiliation.get_affiliation_data()
    related_article_list = sorted(affiliation.related_article_dict.items(),key=lambda x:x[0],reverse=False)
    affiliation_info_list = []
    sql = '''
                        SELECT aff.name,AVG(art.citation_count),SUM(art.citation_count),
                        COUNT(art.id),MIN(YEAR(art.date)),MAX(YEAR(art.date)),
                        COUNT(art.pdf_link),AVG(art.total_usage-art.citation_count)
                        FROM article art,affiliation aff
                        WHERE art.id IN %s
                        AND aff.id = %s
                '''
    back_up_sql = '''
                        SELECT aff.name
                        FROM affiliation aff
                        WHERE aff.id = %s
                '''
    update_sql = '''
                        INSERT INTO affiliation_info
                        (affiliation_id,affiliation_name,average_citation_per_article,
                        citation_count,publication_count,start_year,end_year,
                        available_download,average_download_per_article,
                        create_time,update_time)
                        VALUES 
                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY
                        UPDATE affiliation_name = VALUES (affiliation_name),
                        average_citation_per_article = VALUES (average_citation_per_article),
                        citation_count = VALUES (citation_count),
                        publication_count = VALUES(publication_count),
                        start_year = VALUES (start_year),
                        end_year = VALUES (end_year),
                        available_download = VALUES (available_download),
                        average_download_per_article = VALUES (average_download_per_article),
                        update_time = VALUES (update_time)
                '''
    for affiliations_articles in chunks(related_article_list,500):
        for affiliation_articles in affiliations_articles:
            affiliation_id = affiliation_articles[0]
            articles = affiliation_articles[1]
            update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not articles or len(articles)==0:
                Cursor.execute(back_up_sql, (affiliation_id,))
                affiliation_name = raw_result[0]
                affiliation_info_list.append((affiliation_id,affiliation_name,
                                              0.0,0,0,-1,-1,0,0.0,update_time,
                                              update_time))
                continue
            Cursor.execute(sql, (articles,affiliation_id,))
            raw_result = list(Cursor.fetchone())
            if raw_result is None:
                continue
            affiliation_name = raw_result[0]
            average_citation_per_article = float(str(raw_result[1].quantize(Decimal('0.00'))))
            citation_count = int(str(raw_result[2]))
            publication_count = raw_result[3]
            start_year = raw_result[4]
            end_year = raw_result[5]
            available_download = raw_result[6]
            average_download_per_article = float(str(raw_result[7].quantize(Decimal('0.00'))))
            affiliation_info_list.append((affiliation_id,affiliation_name,average_citation_per_article,citation_count,
                   publication_count,start_year,end_year,available_download,
                    average_download_per_article,update_time,update_time))

    print("{} affiliation_info_list_len: {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                                                   len(affiliation_info_list)))


    for affiliation_infos in chunks(affiliation_info_list, 500):
        try:
            Cursor.executemany(update_sql,affiliation_infos)
            Connection.commit();
        except Exception as e:
            print(e)
            Connection.rollback()
        time.sleep(1)
    print("{} save_to_mysql finished".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))





if __name__ == "__main__":
    start = time.time()
    update_affiliation_database_job()
    end = time.time()
    print('Format Runtime is:{0:.3f}s'.format(end-start))