import redis


def multi_lpop(redis, queue, number):
    ''' Pops multiple elements from a list in an atomic way :) should be on the specs! '''
    p = redis.pipeline(transaction=True)
    p.lrange(queue, 0, number - 1)
    p.ltrim(queue, number, -1)
    return p.execute()[0]


def get_redis_connection(redis_host, redis_db, redis_port=6379):
    r = redis.Redis(redis_host, db=redis_db, port=redis_port)
    try:
        if not r.ping():
            raise Exception('No redis ping')
    except:
        raise Exception('No Redis')
    return r