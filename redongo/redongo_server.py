import datetime
import logging
import logging.handlers
import os
import pymongo
import redis
import server_exceptions
import signal
import sys
import time
import traceback
import ujson
from utils import redis_utils
from utils import cipher_utils
from optparse import OptionParser
from pymongo.errors import DuplicateKeyError
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

# LOGGER CONFIG
FACILITY = "local0"
logging.basicConfig()
logger = logging.getLogger()

formatter = logging.Formatter('PID:%(process)s %(filename)s %(funcName)s %(levelname)s %(message)s')

logger.setLevel(logging.DEBUG)

required_options = ['REDIS', 'REDIS_DB', 'REDIS_QUEUE']

rs = None
options = None
args = None


class RedongoServer(object):
    def __init__(self, *args, **kwargs):
        def __get_sk__():
            result = self.redis.get('redongo_sk')
            if not result:
                result = os.urandom(16)
                self.redis.set('redongo_sk', result)
            return result

        logger.debug('Starting Redongo Server..')
        self.redis = redis_utils.get_redis_connection(options.redisIP, redis_db=options.redisDB)
        self.keep_going = True
        self.redisQueue = options.redisQueue
        self.popSize = options.popSize
        self.bulks = {}
        self.objs = []
        self.busy = False
        self.cipher = cipher_utils.AESCipher(__get_sk__())

    def check_object(self, obj):
        if type(obj) != list or len(obj) != 2:
            raise server_exceptions.ObjectValidationError('Type not valid')

    def get_application_settings(self, application_name):
        # TODO: Add settings validation
        try:
            application_settings = ujson.loads(self.redis.get('redongo_{0}'.format(application_name)))
            fields_to_validate = [
                'mongo_host',
                'mongo_port',
                'mongo_database',
                'mongo_collection',
                'mongo_user',
                'mongo_password',
                'bulk_size',
                'bulk_expiration',
            ]

            for f in fields_to_validate:
                if not application_settings.get(f, None):
                    raise server_exceptions.ApplicationSettingsError('No {0} value in {1} application settings'.format(f, application_name))

            return application_settings
        except TypeError:
            raise server_exceptions.ApplicationSettingsError('Not existing conf for application {0}'.format(application_name))
        except ValueError:
            raise server_exceptions.ApplicationSettingsError('Invalid existing conf for application {0}'.format(application_name))

    def save_to_failed_queue(self, application_name, bulk):
        i = 0
        for obj in bulk['data']:
            self.redis.rpush('{0}_FAILED'.format(self.redisQueue), ujson.dumps([application_name, obj]))
            i += 1
        logger.warning('Moved {0} objects from application {1} to queue {2}_FAILED'.format(i, application_name, self.redisQueue))

    def run(self):
        try:
            logger.debug('Running!')
            while self.keep_going:
                try:
                    self.objs.append(self.redis.blpop(self.redisQueue)[1])
                except redis.TimeoutError:
                    continue
                self.objs.extend(redis_utils.multi_lpop(self.redis, self.redisQueue, self.popSize-1))
                while self.busy:
                    time.sleep(0.1)
                self.busy = True
                while self.objs:
                    obj = ujson.loads(self.objs.pop())
                    try:
                        self.check_object(obj)
                        application_settings = self.get_application_settings(obj[0])
                    except (server_exceptions.ObjectValidationError, server_exceptions.ApplicationSettingsError), e:
                        logger.error('Discarding {0} object because of {1}'.format(obj[0], e))
                        continue
                    application_bulk = self.bulks.setdefault(obj[0], {'data': []})
                    application_bulk.setdefault('inserted_date', datetime.datetime.utcnow())
                    application_bulk.update(application_settings)
                    application_bulk['data'].append(obj[1])
                self.busy = False
        except:
            logger.error('Stopping redongo because unexpected exception: {0}'.format(traceback.format_exc()))
            stopApp()

    def back_to_redis(self):
        logger.debug('Returning memory data to Redis')
        objects_returned = 0
        for application_name, bulk in self.bulks.iteritems():
            for obj in bulk['data']:
                self.redis.rpush(self.redisQueue, ujson.dumps([application_name, obj]))
                objects_returned += 1
        logger.debug('{0} objects returned to Redis'.format(objects_returned))

    def get_mongo_collection(self, bulk):
        mongo_client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(bulk['mongo_user'], self.cipher.decrypt(bulk['mongo_password']), bulk['mongo_host'], bulk['mongo_database']))
        mongo_db = mongo_client[bulk['mongo_database']]
        collection = mongo_db[bulk['mongo_collection']]
        return collection

    def save_to_mongo(self, application_name, bulk):
        try:
            collection = self.get_mongo_collection(bulk)
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ConfigurationError, pymongo.errors.OperationFailure), e:
            logger.error('Not saving {0} (moving to failed queue) from application {1} due to connection bad data: {2}'.format(bulk, application_name, e))
            self.save_to_failed_queue(application_name, bulk)
            return
        try:
            collection.insert(bulk['data'])
        except DuplicateKeyError:
            while bulk['data']:
                try:
                    try:
                        obj = bulk['data'].pop()
                        collection.insert(obj)
                    except DuplicateKeyError:
                        collection.update({'_id': obj['_id']}, obj)
                except:
                    bulk['data'].append(obj)
                    raise
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ConfigurationError, pymongo.errors.OperationFailure), e:
            logger.error('Not saving {0} (moving to failed queue) from application {1} due to connection bad data: {2}'.format(bulk, application_name, e))
            self.save_to_failed_queue(application_name, bulk)

    def check_completed_bulks(self):
        try:
            if not self.busy:
                self.busy = True
                applications_to_delete = []
                for application_name, bulk in self.bulks.iteritems():
                    if len(bulk['data']) >= bulk['bulk_size'] or bulk['inserted_date'] + datetime.timedelta(seconds=bulk['bulk_expiration']) <= datetime.datetime.utcnow():
                        self.save_to_mongo(application_name, bulk)
                        applications_to_delete.append(application_name)
                        pass
                for app_to_delete in applications_to_delete:
                    del(self.bulks[app_to_delete])
                self.busy = False
        except:
            stopApp()


def sigtermHandler():
    global rs
    rs.keep_going = False
    logger.debug('Waiting 10 seconds before returning data to Redis')
    time.sleep(10)
    rs.back_to_redis()
    logger.debug('Exiting program!')


def stopApp():
    logger.debug('Stopping app')
    reactor.stop()


def closeApp(signum, frame):
    logger.debug('Received signal {0}'.format(signum))
    stopApp()


def main():
    global rs
    global options
    global args
    global logger

    parser = OptionParser(description='Startup options')
    parser.add_option('--redis', '-r', dest='redisIP', help='Redis server IP Address', metavar='REDIS')
    parser.add_option('--redisdb', '-d', dest='redisDB', help='Redis server DB', metavar='REDIS_DB')
    parser.add_option('--redisqueue', '-q', dest='redisQueue', help='Redis Queue', metavar='REDIS_QUEUE')
    parser.add_option('--popsize', '-p', dest='popSize', help='Redis Pop Size', metavar='REDIS_POP_SIZE', default=100)
    parser.add_option('--logger', '-l', dest='logger', help='Logger Usage', metavar='LOGGER_USAGE', default='1')
    (options, args) = parser.parse_args()

    for required_option in filter(lambda x: x.__dict__['metavar'] in required_options, parser.option_list):
        if not getattr(options, required_option.dest):
            logger.error('Option {0} not found'.format(required_option.metavar))
            sys.exit(-1)

    # With this line the logs are sent to syslog.
    if options.logger != '0':
        handler = logging.handlers.SysLogHandler("/dev/log", FACILITY)
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    signal.signal(signal.SIGHUP, closeApp)
    signal.signal(signal.SIGTERM, closeApp)
    signal.signal(signal.SIGINT, closeApp)
    signal.signal(signal.SIGALRM, closeApp)

    # Handler for SIGTERM
    reactor.addSystemEventTrigger('before', 'shutdown', sigtermHandler)

    rs = RedongoServer()

    lc = LoopingCall(rs.check_completed_bulks)
    lc.start(1, now=False)

    reactor.callInThread(rs.run)

    # Start the reactor
    reactor.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(25)
