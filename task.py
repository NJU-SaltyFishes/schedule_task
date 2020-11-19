# coding:utf-8 

import time
import schedule
from multiprocessing import Process

from job.affiliation_keywords import update_affiliation_keyword_job
from job.new_publish_atricle import update_affiliation_new_article_job
from job.load_affiliation_data import load_affiliation_data_job
from job.affiliation_most_cited_author import update_affiliation_most_cited_author_job


def main_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)


def pool_job(func, args=()):
    p = Process(target=func, args=args)
    p.start()


def register_schedule_task():
    # 机构研究关键词
    # schedule.every().saturday.at("12:00").do(pool_job, update_affiliation_keyword_job)
    schedule.every(2).seconds.do(pool_job, update_affiliation_keyword_job)

    # 机构最新发表论文
    # schedule.every().saturday.at("16:00").do(pool_job,update_affiliation_new_article_job())
    schedule.every(4).seconds.do(pool_job, update_affiliation_new_article_job)

    #
    schedule.every().saturday.at("20:30").do(pool_job, load_affiliation_data_job)
    schedule.every().sunday.at("4:00").do(pool_job,update_affiliation_most_cited_author_job)


if __name__ == "__main__":
    register_schedule_task()
    main_loop()
