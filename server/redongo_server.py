import datetime
import logging
import logging.handlers
import os
import pymongo
import signal
import sys
import time
import ujson
import redongo_utils
from optparse import OptionParser
from pymongo.errors import DuplicateKeyError
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

# LOGGER CONFIG
FACILITY = "local0"
logging.basicConfig()
logger = logging.getLogger()

# With this line the logs are sent to syslog.
handler = logging.handlers.SysLogHandler("/dev/log", FACILITY)
formatter = logging.Formatter('PID:%(process)s %(filename)s %(funcName)s %(levelname)s %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.setLevel(logging.DEBUG)

required_options = ['REDIS', 'REDIS_DB', 'REDIS_QUEUE']

parser = OptionParser(description='Startup options')
parser.add_option('--redis', '-r', dest='redisIP', help='Redis server IP Address', metavar='REDIS')
parser.add_option('--redisdb', '-d', dest='redisDB', help='Redis server DB', metavar='REDIS_DB')
parser.add_option('--redisqueue', '-q', dest='redisQueue', help='Redis Queue', metavar='REDIS_QUEUE')
parser.add_option('--popsize', '-p', dest='popSize', help='Redis Pop Size', metavar='REDIS_POP_SIZE', default=100)
(options, args) = parser.parse_args()

for required_option in filter(lambda x: x.__dict__['metavar'] in required_options, parser.option_list):
    if not getattr(options, required_option.dest):
        logger.error('Option {0} not found'.format(required_option.metavar))
        sys.exit(-1)

rs = None


class ObjectValidationError(Exception):
    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


class ApplicationSettingsError(Exception):
    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


class RedongoServer(object):
    def __init__(self, *args, **kwargs):
        def __get_sk__():
            result = self.redis.get('redongo_sk')
            if not result:
                result = os.urandom(16)
                self.redis.set('redongo_sk', result)
            return result

        logger.debug('Starting Redongo Server..')
        self.redis = redongo_utils.get_redis_connection(options.redisIP, redis_db=options.redisDB)
        self.keep_going = True
        self.redisQueue = options.redisQueue
        self.popSize = options.popSize
        self.bulks = {}
        self.objs = []
        self.busy = False
        self.cipher = redongo_utils.AESCipher(__get_sk__())

    def check_object(self, obj):
        if type(obj) != list or len(obj) != 2:
            raise ObjectValidationError('Type not valid')

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
                    raise ApplicationSettingsError('No {0} value in {1} application settings'.format(f, application_name))

            return application_settings
        except TypeError:
            raise ApplicationSettingsError('Not existing conf for application {0}'.format(application_name))
        except ValueError:
            raise ApplicationSettingsError('Invalid existing conf for application {0}'.format(application_name))

    def run(self):
        try:
            logger.debug('Running!')
            while self.keep_going:
                self.objs.append(self.redis.blpop(self.redisQueue)[1])
                self.objs.extend(redongo_utils.multi_lpop(self.redis, self.redisQueue, self.popSize-1))
                while self.busy:
                    time.sleep(0.1)
                self.busy = True
                while self.objs:
                    obj = ujson.loads(self.objs.pop())
                    try:
                        self.check_object(obj)
                        application_settings = self.get_application_settings(obj[0])
                    except (ObjectValidationError, ApplicationSettingsError), e:
                        logger.error('Discarding {0} object because of {1}'.format(obj[0], e))
                        continue
                    application_bulk = self.bulks.setdefault(obj[0], {'data': []})
                    application_bulk.setdefault('inserted_date', datetime.datetime.utcnow())
                    application_bulk.update(application_settings)
                    application_bulk['data'].append(obj[1])
                self.busy = False
        except:
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

    def save_to_mongo(self, bulk):
        collection = self.get_mongo_collection(bulk)
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

    def check_completed_bulks(self):
        try:
            if not self.busy:
                self.busy = True
                applications_to_delete = []
                for application_name, bulk in self.bulks.iteritems():
                    if len(bulk['data']) >= bulk['bulk_size'] or bulk['inserted_date'] + datetime.timedelta(seconds=bulk['bulk_expiration']) <= datetime.datetime.utcnow():
                        self.save_to_mongo(bulk)
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
    rs.back_to_redis()
    logger.debug('Exiting program!')
    os._exit(0)


def stopApp():
    logger.debug('Stopping app')
    reactor.stop()


def closeApp(signum, frame):
    logger.debug('Received signal {0}'.format(signum))
    stopApp()


def main():
    global rs
    signal.signal(signal.SIGHUP, closeApp)
    signal.signal(signal.SIGTERM, closeApp)
    signal.signal(signal.SIGINT, closeApp)

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
