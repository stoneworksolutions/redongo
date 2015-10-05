import contextlib
import pymongo
import redis
from redongo import redongo_server
import sys
import signal

REDIS_HOST = 'localhost'
REDIS_DB = 0
REDIS_QUEUE = 'REDONGO_TEST_QUEUE'
REDIS_QUEUE_FAILED = 'REDONGO_TEST_QUEUE_FAILED'
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'mydb_test'
MONGO_COLLECTION = 'mycollection_test'
MONGO_USER = 'test'
MONGO_PASSWORD = 'test123'

# uncomment on our environment
# REDIS_HOST = 'dev-redis'
# MONGO_HOST = 'dev-mongo'
# MONGO_DB = 'mgalan'


@contextlib.contextmanager
def redirect_argv(*args):
    sys._argv = sys.argv[:]
    sys.argv = list(args)
    yield
    sys.argv = sys._argv


class TestServer:
    def test__RedongoServer__OK1(self):
        signal.alarm(10)

        with redirect_argv('redongo_server.py', '-r', str(REDIS_HOST), '-d', str(REDIS_DB), '-q', str(REDIS_QUEUE), '-l', '0'):
            redongo_server.main()

        r = redis.Redis(REDIS_HOST, db=REDIS_DB)
        assert r.llen(REDIS_QUEUE) == 0
        assert r.llen(REDIS_QUEUE_FAILED) == 1
        mongo_client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(MONGO_USER, MONGO_PASSWORD, MONGO_HOST, MONGO_DB))
        assert mongo_client[MONGO_DB][MONGO_COLLECTION].count() == 8
        assert mongo_client[MONGO_DB][MONGO_COLLECTION].find({"test": 5}).count() == 3
