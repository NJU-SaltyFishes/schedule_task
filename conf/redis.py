# coding:utf-8

from redis import ConnectionPool, StrictRedis


class Redis:
    def __init__(self):
        self.pool = ConnectionPool(
            host="",
            port=""
        )
        self.redisTemplate = StrictRedis(connection_pool=self.pool)


RedisTemplate = Redis().redisTemplate
