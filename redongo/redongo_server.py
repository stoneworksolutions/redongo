import datetime
import logging
import logging.handlers
import os
import pymongo
import redis
import server_exceptions
import general_exceptions
import signal
import sys
import time
import traceback
from utils import utils
from utils import redis_utils
from utils import cipher_utils
from utils import serializer_utils
from utils import queue_utils
from optparse import OptionParser
from pymongo.errors import DuplicateKeyError
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from redlock import Redlock

try:
    from bson.objectid import ObjectId
    from bson.errors import InvalidId
except ImportError:
    from pymongo.objectid import ObjectId, InvalidId

try:
    import cPickle as pickle
except:
    import pickle

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
        self.redis = redis_utils.get_redis_connection(options.redisIP, redis_db=options.redisDB, redis_port=options.redisPort)
        self.keep_going = True
        self.redisQueue = options.redisQueue
        self.popSize = int(options.popSize)
        self.redisQueueSize = int(options.redisQueueSize)
        self.bulks = {}
        self.completed_bulks = set()
        self.objs = []
        self.cipher = cipher_utils.AESCipher(__get_sk__())
        self.disk_queue = queue_utils.Queue(queue_name=options.diskQueue)
        self.returned_disk_queue = queue_utils.Queue(queue_name='{0}_returned'.format(options.diskQueue))
        self.redlock = Redlock([{"host": options.redisIP, "port": options.redisPort, "db": options.redisDB}])
        self.lock_key = '{0}_LOCK'.format(self.redisQueue)

    def check_object(self, obj):
        if type(obj) != list or len(obj) != 2:
            raise server_exceptions.ObjectValidationError('Type not valid')

    def get_application_settings(self, application_name):
        return utils.get_application_settings(application_name, self.redis)

    def save_to_failed_queue(self, application_name, bulk):
        i = 0
        for obj, command, original_object in bulk['data']:
            self.redis.rpush('{0}_FAILED'.format(self.redisQueue), original_object)
            i += 1
        logger.warning('Moved {0} objects from application {1} to queue {2}_FAILED'.format(i, application_name, self.redisQueue))

    def run(self):
        failed_objects = []
        first_run = True
        try:
            logger.debug('Running!')

            while self.keep_going:
                object_found = False

                lock = self.redlock.lock(self.lock_key, 60 * 1000)
                while not lock:
                    time.sleep(1)
                    lock = self.redlock.lock(self.lock_key, 60 * 1000)

                if first_run:
                    while self.returned_disk_queue._length > 0:
                        self.objs.append(self.returned_disk_queue.pop())
                        object_found = True
                    first_run = False
                    if object_found:
                        logger.debug('Got {0} objects from disk queue {1}'.format(len(self.objs), self.returned_disk_queue._disk_queue_name))

                if self.disk_queue._length > 0:
                    for i in range(0, self.popSize):
                        if self.disk_queue._length:
                            self.objs.append(self.disk_queue.pop())
                            object_found = True
                        else:
                            break
                else:
                    try:
                        self.objs.append(self.redis.blpop(self.redisQueue)[1])
                        object_found = True
                    except redis.TimeoutError:
                        pass
                    if object_found:
                        self.objs.extend(redis_utils.multi_lpop(self.redis, self.redisQueue, self.popSize-1))

                if lock:
                    self.redlock.unlock(lock)

                if object_found:
                    while self.objs:
                        orig_obj = self.objs.pop(0)
                        obj = pickle.loads(orig_obj)
                        try:
                            self.check_object(obj)
                            application_settings = self.get_application_settings(obj[0][0])
                        except (server_exceptions.ObjectValidationError, general_exceptions.ApplicationSettingsError), e:
                            logger.error('Discarding {0} object because of {1}'.format(obj[0], e))
                            failed_objects.append(obj[0])
                            continue
                        application_bulk = self.bulks.setdefault(obj[0][0], {'serializer': obj[0][1], 'data': []})
                        application_bulk.setdefault('inserted_date', datetime.datetime.utcnow())
                        application_bulk.update(application_settings)
                        ser = serializer_utils.serializer(obj[0][1])
                        obj_data = ser.loads(obj[1])
                        application_bulk['data'].append((self.normalize_object(obj_data), obj[0][2], orig_obj))

                while self.completed_bulks:
                    self.consume_application(self.completed_bulks.pop())

                # Guarantee that the looping call can access the lock
                time.sleep(.2)

        except:
            logger.error('Stopping redongo because unexpected exception: {0}'.format(traceback.format_exc()))
            stopApp()

    def back_to_disk(self):
        logger.debug('Returning memory data to Disk Queue')
        objects_returned = 0
        for application_name, bulk in self.bulks.iteritems():
            for obj, command, original_object in bulk['data']:
                self.returned_disk_queue.push(original_object)
                objects_returned += 1
        logger.debug('{0} objects returned to Disk Queue'.format(objects_returned))

    def get_mongo_collection(self, bulk):
        mongo_client = pymongo.MongoClient('mongodb://{0}:{1}@{2}/{3}'.format(bulk['mongo_user'], self.cipher.decrypt(bulk['mongo_password']), bulk['mongo_host'], bulk['mongo_database']))
        mongo_db = mongo_client[bulk['mongo_database']]
        collection = mongo_db[bulk['mongo_collection']]
        return collection

    def normalize_object(self, obj):
        objectid_fields = obj.pop('objectid_fields', [])
        for f in objectid_fields:
            if obj.get(f, None):
                try:
                    obj[f] = ObjectId(obj[f])
                except InvalidId:
                    pass
                except TypeError:
                    pass

        return obj

    def deal_with_mongo(self, application_name):
        bulk = self.bulks[application_name]
        set_of_objects = []
        to_failed = []
        result = None
        try:
            collection = self.get_mongo_collection(bulk)
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ConfigurationError, pymongo.errors.OperationFailure), e:
            logger.error('Not saving bulk {0} (moving to failed queue) from application {1} due to connection bad data: {2}'.format(bulk, application_name, e))
            self.save_to_failed_queue(application_name, bulk)
            return
        # Separates objects with different commands. When appears any object with other command, executes current command for all readed objects
        current_command = bulk['data'][0][1]
        while bulk['data']:
            obj, command, original_object = bulk['data'].pop(0)
            if command == current_command:
                set_of_objects.append(obj)
            else:
                # Execute command for all readed objects
                if current_command == 'save':
                    result = self.save_to_mongo(collection, set_of_objects)
                elif current_command == 'add':
                    result = self.add_in_mongo(collection, set_of_objects)
                # Notify on failure
                if result:
                    logger.error('Not saving {0} objects (moving to failed queue) from application {1} due to connection bad data'.format(len(result), application_name))
                    to_failed += map(lambda x: (x, command), result)
                current_command = command
                set_of_objects = [obj]
        # Last set
        if current_command == 'save':
            result = self.save_to_mongo(collection, set_of_objects)
        elif current_command == 'add':
            result = self.add_in_mongo(collection, set_of_objects)
        # Notify on failure
        if result:
            logger.error('Not saving {0} objects (moving to failed queue) from application {1} due to connection bad data'.format(len(result), application_name))
            to_failed += map(lambda x: (x, current_command), result)

        # If an error occurred, it notifies and inserts the required objects
        if to_failed:
            bulk['data'] = to_failed
            self.save_to_failed_queue(application_name, bulk)

    def save_to_mongo(self, collection, objs):
        to_insert = []
        to_update = []
        to_failed = []
        differents = set()
        while objs:
            obj = objs.pop(0)
            if '_id' not in obj:
                to_insert.append(obj)
            elif obj['_id'] not in differents:
                to_insert.append(obj)
                differents.add(obj['_id'])
            else:
                to_update.append(obj)
        # Bulk insert
        try:
            collection.insert(to_insert)
        except DuplicateKeyError:
            to_update = to_insert + to_update

        # One-to-one update
        while to_update:
            obj = to_update.pop(0)
            try:
                collection.update({'_id': obj['_id']}, obj)
            except (pymongo.errors.ConnectionFailure, pymongo.errors.ConfigurationError, pymongo.errors.OperationFailure):
                to_failed.append(obj)
        # Return unsaved objects
        return to_failed

    def create_add_query(self, obj, previous_field='', query={}):
        for field, value in obj.iteritems():
            if field == '_id':
                continue
            type_field = type(value)
            # Numeric and logical fields perform an addition (Complex number are not supported by mongo)
            if type_field is int or type_field is long or type_field is float or type_field is bool:  # type_field is complex:
                x = query.setdefault('$inc', {})
                x[previous_field + field] = value
            # String fields perform a set
            elif type_field is str:
                x = query.setdefault('$set', {})
                x[previous_field + field] = value
            # List fields perform a concatenation
            elif type_field is list:
                x = query.setdefault('$push', {})
                x[previous_field + field] = {'$each': value}
            # Dict fields will be treated as the original object
            elif type_field is dict:
                query = self.create_add_query(value, '{0}{1}.'.format(previous_field, field), query)
        return query

    def add_in_mongo(self, collection, objs):
        to_failed = []
        # One-to-one update
        while objs:
            obj = objs.pop(0)
            try:
                collection.update({'_id': obj['_id']}, self.create_add_query(obj), upsert=True)
            except (pymongo.errors.ConnectionFailure, pymongo.errors.ConfigurationError, pymongo.errors.OperationFailure):
                to_failed.append(obj)
        # Return unadded objects and info
        return to_failed

    def consume_application(self, application_name):
        # In case that check_completed_bulks reads while main thread was saving on previous iteration
        if application_name in self.bulks:
            self.deal_with_mongo(application_name)
            self.bulks.pop(application_name)

    def check_completed_bulks(self):
        try:
            for application_name, bulk in self.bulks.items():
                if len(bulk['data']) >= bulk['bulk_size'] or bulk['inserted_date'] + datetime.timedelta(seconds=bulk['bulk_expiration']) <= datetime.datetime.utcnow():
                    self.completed_bulks.add(application_name)
        except:
            stopApp()

    def check_redis_queue(self):
        try:
            if self.redis.llen(self.redisQueue) > self.redisQueueSize or self.disk_queue._length > 0:
                to_disk_queue = []
                object_found = False
                lock = self.redlock.lock(self.lock_key, 60 * 1000)
                cont = 0
                while not lock:
                    if cont % 10 == 0:
                        logger.debug('Redis queue watcher waiting to get lock...')
                    time.sleep(.2)
                    lock = self.redlock.lock(self.lock_key, 60 * 1000)
                    cont += 1
                while self.redis.llen(self.redisQueue) > self.redisQueueSize:
                    try:
                        to_disk_queue.append(self.redis.blpop(self.redisQueue)[1])
                        object_found = True
                    except redis.TimeoutError:
                        pass
                    if object_found:
                        to_disk_queue.extend(redis_utils.multi_lpop(self.redis, self.redisQueue, self.popSize-1))
                    self.save_to_disk_queue(to_disk_queue)
                self.redlock.unlock(lock)
        except redis.TimeoutError:
            pass

    def save_to_disk_queue(self, objs):
        while objs:
            obj = objs.pop(0)
            self.disk_queue.push(obj)

    def close_disk_queues(self):
        try:
            self.disk_queue.close()
        except:
            logger.error('Could not close disk queue {0}: {1}'.format(self.disk_queue._disk_queue_name, traceback.format_exc()))
        try:
            self.returned_disk_queue.close()
        except:
            logger.error('Could not close disk queue {0}: {1}'.format(self.returned_disk_queue._disk_queue_name, traceback.format_exc()))


def sigtermHandler():
    global rs
    rs.keep_going = False
    logger.debug('Waiting 10 seconds before returning data to Disk')
    time.sleep(10)
    rs.back_to_disk()
    rs.close_disk_queues()
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
    parser.add_option('--port', '-P', dest='redisPort', help='Redis Port', metavar='REDIS_PORT', default=6379)
    parser.add_option('--queuesize', '-s', dest='redisQueueSize', help='Max Redis Queue Size', metavar='REDIS_QUEUE_SIZE', default=10000)
    parser.add_option('--diskqueue', '-Q', dest='diskQueue', help='Disk Queue', metavar='DISK_QUEUE', default='redongo_disk_queue')
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

    lc_redis_queue = LoopingCall(rs.check_redis_queue)
    lc_redis_queue.start(1, now=False)

    reactor.callInThread(rs.run)

    # Start the reactor
    reactor.run(installSignalHandlers=False)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(25)
