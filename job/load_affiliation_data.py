# coding:utf-8

import traceback
from conf.mysql import Cursor


def get_all_affiliation_ids():
    try:
        Cursor.execute('''select id from affiliation''')
        res = Cursor.fetchall()
        return res, True
    except Exception:
        traceback.print_exc()
        return [], False


def load_affiliation_data_job():
    res, ok = get_all_affiliation_ids()
    print(ok, res)


if __name__ == "__main__":
    load_affiliation_data_job()
