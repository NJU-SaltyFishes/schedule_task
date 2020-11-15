# coding:utf-8

import time
import traceback
from job.loader.affiliation_loader import AffiliationLoader


def load_affiliation_data_job():
    affiliationLoader = AffiliationLoader()
    try:
        affiliationLoader.load_affiliation_data()
    except Exception:
        traceback.format_exc()
        # TODO 加载异常 发邮件

    # TODO 数据异常 发邮件
    if len(affiliationLoader.affiliation_ids) == 0 or len(affiliationLoader.related_article_dict) == 0:
        print("{} affiliation_total_count: {}, final_affiliation_count: {}".format(
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            len(affiliationLoader.affiliation_ids),
            len(affiliationLoader.related_article_dict)))


if __name__ == "__main__":
    load_affiliation_data_job()
