import os
from utils import utils
from utils import redis_utils
from utils import cipher_utils
from utils import serializer_utils
import client_exceptions
import general_exceptions
import warnings
try:
    import ujson as json
except:
    import json
try:
    from bson.objectid import ObjectId
except ImportError:
    from pymongo.objectid import ObjectId

try:
    import cPickle as pickle
except:
    import pickle
import copy


class RedongoClient():
    def __init__(self, redis_host, redis_db, redis_queue, redis_port=6379):
        self.serializer_types = ['json', 'ujson', 'pickle']
        self.redis = redis_utils.get_redis_connection(redis_host, redis_db=redis_db, redis_port=redis_port)
        if redis_queue:
            self.redis_queue = redis_queue
        else:
            raise client_exceptions.Client_NoQueueParameter('Not valid queue received: {0}'.format(redis_queue))

        self.app_data = {}

    def set_application_settings(self, application_name, mongo_host, mongo_port, mongo_database, mongo_collection, mongo_user, mongo_password, bulk_size=100, bulk_expiration=60, serializer_type='pickle'):

        def __get_sk__():
            result = self.redis.get('redongo_sk')
            if not result:
                result = os.urandom(16)
                self.redis.set('redongo_sk', result)
            return result

        if not application_name:
            raise general_exceptions.Register_NoApplicationName('Can\'t set application settings: No application name')
        if serializer_type not in self.serializer_types:
            raise general_exceptions.Register_NoValidSerializer('Can\'t set application settings: No valid serializer')

        app_data = {}
        app_data['mongo_host'] = mongo_host
        app_data['mongo_port'] = mongo_port if mongo_port else 27017
        app_data['mongo_database'] = mongo_database
        app_data['mongo_collection'] = mongo_collection
        app_data['mongo_user'] = mongo_user
        cipher = cipher_utils.AESCipher(__get_sk__())
        app_data['mongo_password'] = cipher.encrypt(mongo_password)
        if bulk_size > 1000:
            bulk_size = 1000
            warnings.warn("deprecated", DeprecationWarning)
        app_data['bulk_size'] = bulk_size
        app_data['bulk_expiration'] = bulk_expiration
        app_data['serializer_type'] = serializer_type

        for key, value in app_data.iteritems():
            if not value:
                raise client_exceptions.Register_NoAttributeReceived('Can\'t set application {1} settings: No value set for {0}'.format(key, application_name))

        # APP_DATA OK!
        self.app_data[application_name] = app_data

        self.redis.set('redongo_{0}'.format(application_name), pickle.dumps(self.app_data[application_name]))

    def get_application_settings(self, application_name):
        if application_name in self.app_data:
            return self.app_data[application_name]
        else:
                return utils.get_application_settings(application_name, self.redis)

    def remove_application_settings(self, application_name):
        if not application_name:
            raise general_exceptions.Register_NoApplicationName('Can\'t remove application settings: No application name')

        if application_name in self.app_data:
            self.app_data.pop(application_name)
        self.redis.delete('redongo_{0}'.format(application_name))

    def serialize_django_object(self, obj):
        copied_obj = copy.deepcopy(obj)
        fields = set()
        objectid_fields = []
        excluded_fields = set(['_id'])
        if not getattr(copied_obj, '_id', None) and getattr(copied_obj, 'pk', None):
            pk_name = copied_obj._meta.pk.name
            copied_obj._id = getattr(copied_obj, pk_name)
            delattr(copied_obj, pk_name)
        for field in copied_obj._meta.fields:
            if getattr(field, 'to_fields', None):
                objectid_fields.append(field.column)
            fields.add(field.column)
        fields_to_delete = set()
        for attr, value in copied_obj.__dict__.iteritems():
            if attr not in fields:
                fields_to_delete.add(attr)
        for ftd in fields_to_delete-excluded_fields:
            copied_obj.__delattr__(ftd)
        obj_serialized = copied_obj.__dict__
        obj_serialized['objectid_fields'] = objectid_fields
        return copied_obj.__dict__

    def is_django_object(self, obj):
        obj_class = type(obj)
        django_object = False
        while obj_class.__bases__:
            obj_class = obj_class.__bases__[0]
            if obj_class.__module__ == 'django.db.models.base' and obj_class.__name__ == 'Model':
                django_object = True
                break
        return django_object

    def serialize_object_by_type(self, obj):
        obj_serialized = None
        if type(obj) == dict:
            obj_serialized = obj
        elif type(obj) == str:
            try:
                obj_serialized = json.loads(obj)
            except (ValueError, TypeError):
                pass
        elif self.is_django_object(obj):
            obj_serialized = self.serialize_django_object(obj)
        else:
            try:
                obj_serialized = obj.__dict__
            except AttributeError:
                pass

        if obj_serialized:
            if 'objectid_fields' not in obj_serialized:
                obj_serialized['objectid_fields'] = []
            obj_serialized['objectid_fields'].append('_id')
            return obj_serialized

        raise client_exceptions.Save_InvalidClass('Saving invalid type')

    def serialize_object(self, obj):
        obj = self.serialize_object_by_type(obj)
        if obj.get('_id', None):
            if type(obj['_id']) is ObjectId:
                obj['_id'] = str(obj['_id'])
        return obj

    def order_to_server(self, application_name, objects_to_deal, command):
        application_config = {}
        if not ((application_name in self.app_data) or self.redis.exists('redongo_{0}'.format(application_name))):
            raise client_exceptions.Save_InexistentAppSettings('Application settings for app {0} does not exist'.format(application_name))
        application_config = self.get_application_settings(application_name)
        if not hasattr(objects_to_deal, '__iter__') or type(objects_to_deal) == dict:
            objects_to_deal = [objects_to_deal]
        final_objects_to_deal = []
        for obj in objects_to_deal:
            obj = self.serialize_object(obj)
            final_objects_to_deal.append(obj)
        if final_objects_to_deal:
            ser = serializer_utils.serializer(application_config['serializer_type'])
            self.redis.rpush(self.redis_queue, *map(lambda x: pickle.dumps([(application_name, application_config['serializer_type'], command), ser.dumps(x)]), final_objects_to_deal))

    def save_to_mongo(self, application_name, objects_to_save):
        self.order_to_server(application_name, objects_to_save, 'save')

    def add_in_mongo(self, application_name, objects_to_update):
        self.order_to_server(application_name, objects_to_update, 'add')


class RedongoSentinelClient(RedongoClient):
    ''' Just like the regular Redongo but accepts a redis connection instead of connection parameters '''
    def __init__(self, redis_conn, redis_queue):
        self.serializer_types = ['json', 'ujson', 'pickle']
        self.redis = redis_conn
        if redis_queue:
            self.redis_queue = redis_queue
        else:
            raise client_exceptions.Client_NoQueueParameter('Not valid queue received: {0}'.format(redis_queue))

        self.app_data = {}
