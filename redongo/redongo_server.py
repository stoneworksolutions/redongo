import datetime
import logging
import logging.handlers
import os
import pymongo
import redis
from redis.sentinel import Sentinel
import server_exceptions
import general_exceptions
import signal
import stoneredis
import sys
import time
import traceback
import utils
import cipher_utils
import serializer_utils
import queue_utils
from optparse import OptionParser
from pymongo.errors import DuplicateKeyError
from twisted.internet import reactor
from twisted.internet.error import ReactorNotRunning
from twisted.internet.task import LoopingCall

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

rs = None
options = None
args = None
run_stopped = False


class RedongoServer(object):
    def __init__(self, mode, *args, **kwargs):
        def __get_sk__():
            result = self.redis.get('redongo_sk')
            if not result:
                result = os.urandom(16)
                self.redis.set('redongo_sk', result)
            return result
        logger.info('Starting Redongo Server..')
        self.mode = mode
        self.create_redis_connection()
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
        self.lock_key = '{0}_LOCK'.format(self.redisQueue)

    def create_redis_connection(self):
        if self.mode == 'Redis':
            self.redis = stoneredis.StoneRedis(options.redisIP, db=options.redisDB, port=options.redisPort, socket_connect_timeout=5, socket_timeout=5)
            self.redis.connect()
        else:
            SENTINEL_POOL = Sentinel(
                options.sentinelServers,
                socket_timeout=0.1,
                max_connections=1000,
            )

            self.redis = SENTINEL_POOL.master_for(
                options.sentinelName,
                redis_class=stoneredis.StoneRedis,
                socket_timeout=5,
                socket_connect_timeout=5,
            )

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
        global run_stopped
        failed_objects = []
        first_run = True
        try:
            logger.info('Running!')

            while self.keep_going:
                object_found = False

                lock = self.redis.wait_for_lock(self.lock_key, 60, auto_renewal=True)

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
                            logger.debug('Got {0} objects from disk queue {1}'.format(len(self.objs), self.disk_queue._disk_queue_name))
                        else:
                            break
                else:
                    try:
                        self.objs.append(self.redis.blpop(self.redisQueue)[1])
                        logger.debug('Got {0} objects from redis queue {1}'.format(len(self.objs), self.redisQueue))
                        object_found = True
                    except redis.TimeoutError:
                        pass
                    if object_found:
                        self.objs.extend(self.redis.multi_lpop(self.redisQueue, self.popSize-1))

                if lock:
                    self.redis.release_lock(lock)

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
                time.sleep(.05)

            logger.info('Setting run_stopped to True')
            run_stopped = True

        except:
            logger.error('Stopping redongo because unexpected exception: {0}'.format(traceback.format_exc()))
            logger.info('Setting run_stopped to True')
            run_stopped = True
            stopApp()

    def back_to_disk(self):
        logger.info('Returning memory data to Disk Queue')
        objects_returned = 0
        for application_name, bulk in self.bulks.iteritems():
            for obj, command, original_object in bulk['data']:
                self.returned_disk_queue.push(original_object)
                objects_returned += 1
        logger.info('{0} objects returned to Disk Queue'.format(objects_returned))

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
                lock = self.redis.wait_for_lock(self.lock_key, 60, auto_renewal=True)
                while self.redis.llen(self.redisQueue) > self.redisQueueSize:
                    try:
                        to_disk_queue.append(self.redis.blpop(self.redisQueue)[1])
                        object_found = True
                    except redis.TimeoutError:
                        pass
                    if object_found:
                        to_disk_queue.extend(self.redis.multi_lpop(self.redisQueue, self.popSize-1))
                    self.save_to_disk_queue(to_disk_queue)
                self.redis.release_lock(lock)
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
    global run_stopped
    rs.keep_going = False

    logger.info('Waiting for run_stopped')
    while not run_stopped:
        time.sleep(0.1)
    rs.back_to_disk()
    rs.close_disk_queues()
    logger.info('Exiting program!')


def stopApp():
    global run_stopped

    logger.info('Stopping app')
    try:
        reactor.stop()
    except ReactorNotRunning:
        run_stopped = True


def closeApp(signum, frame):
    logger.info('Received signal {0}'.format(signum))
    stopApp()


def validate(parser, options, required_options, silent=True):
    for required_option in filter(lambda x: x.__dict__['metavar'] in required_options, parser.option_list):
        if not getattr(options, required_option.dest):
            if not silent:
                logger.error('Option {0} not found'.format(required_option.metavar))
            return False
    return True


def validateRedisClient(parser, options):
    required_options = ['REDIS', 'REDIS_DB']
    return validate(parser, options, required_options, silent=False)


def validateSentinelClient(parser, options):
    required_options = ['SENTINEL_SERVERS', 'SENTINEL_NAME']
    return validate(parser, options, required_options, silent=False)


def validateArgs(parser, options):

    if validateRedisClient(parser, options):
        mode = 'Redis'
    elif validateSentinelClient(parser, options):
        mode = 'Sentinel'
    else:
        logger.error('Parameters for Redis connection not valid!\n\tUse -r HOST -d DB for Standard Redis mode\n\tUse -n GROUP NAME -S host1 port1 -S host2 port2 .. -S hostN:portN for Sentinel mode')
        sys.exit(-1)

    required_options = ['REDIS_QUEUE']

    if not validate(parser, options, required_options, silent=False):
        sys.exit(-1)

    return mode


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
    parser.add_option('--sentinelservers', '-S', dest='sentinelServers', help='Sentinel Servers (-S host1 port1 -S host2 port2 .. -S hostN portN)', metavar='SENTINEL_SERVERS', action='append', nargs=2)
    parser.add_option('--sentinelname', '-n', dest='sentinelName', help='Sentinel Group Name', metavar='SENTINEL_NAME')
    parser.add_option('--queuesize', '-s', dest='redisQueueSize', help='Max Redis Queue Size', metavar='REDIS_QUEUE_SIZE', default=10000)
    parser.add_option('--diskqueue', '-Q', dest='diskQueue', help='Disk Queue', metavar='DISK_QUEUE', default='redongo_disk_queue')
    parser.add_option('--logger', '-L', dest='logger', help='Logger Usage', metavar='LOGGER_USAGE', default='1')
    parser.add_option('--log', '-l', dest='logLevel', help='Logger Level', metavar='LOG_LEVEL', default='debug')
    (options, args) = parser.parse_args()

    logger.setLevel(getattr(logging, options.logLevel.upper(), 'DEBUG'))

    mode = validateArgs(parser, options)

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

    rs = RedongoServer(mode)

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
