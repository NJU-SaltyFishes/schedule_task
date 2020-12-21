# coding:utf-8

from redis import ConnectionPool, StrictRedis
from conf.config import REDIS_HOST, REDIS_PORT,REDIS_PASSWD


class Redis:
    def __init__(self):
        self.pool = ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWD
        )
        self.redisTemplate = StrictRedis(connection_pool=self.pool)


RedisTemplate = Redis().redisTemplate
