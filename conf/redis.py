# coding:utf-8

from redis import ConnectionPool, StrictRedis
import conf.config as cf

class Redis:
    def __init__(self):
        self.pool = ConnectionPool(
            host=cf.REDIS_HOST,
            port=cf.REDIS_PORT
        )
        self.redisTemplate = StrictRedis(connection_pool=self.pool)


RedisTemplate = Redis().redisTemplate

