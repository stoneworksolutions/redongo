import os
from utils import utils
from utils import redis_utils
from utils import cipher_utils
from utils import serializer_utils
import client_exceptions
import general_exceptions
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
        self.app_data[application_name] = {}
        self.app_data[application_name]['mongo_host'] = mongo_host
        self.app_data[application_name]['mongo_port'] = mongo_port if mongo_port else 27017
        self.app_data[application_name]['mongo_database'] = mongo_database
        self.app_data[application_name]['mongo_collection'] = mongo_collection
        self.app_data[application_name]['mongo_user'] = mongo_user
        cipher = cipher_utils.AESCipher(__get_sk__())
        self.app_data[application_name]['mongo_password'] = cipher.encrypt(mongo_password)
        self.app_data[application_name]['bulk_size'] = bulk_size
        self.app_data[application_name]['bulk_expiration'] = bulk_expiration
        self.app_data[application_name]['serializer_type'] = serializer_type

        for key, value in self.app_data[application_name].iteritems():
            if not value:
                raise client_exceptions.Register_NoAttributeReceived('Can\'t set application {1} settings: No value set for {0}'.format(key, application_name))

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
        fields = set()
        excluded_fields = set(['_id'])
        if not getattr(obj, '_id', None) and  getattr(obj, 'pk', None):
            obj._id = obj.pk
        for field in obj._meta.fields:
            fields.add(field.column)
        fields_to_delete = set()
        for attr, value in obj.__dict__.iteritems():
            if attr not in fields:
                fields_to_delete.add(attr)
        for ftd in fields_to_delete-excluded_fields:
            obj.__delattr__(ftd)
        return obj.__dict__

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
        if type(obj) == dict:
            return obj
        elif type(obj) == str:
            try:
                return json.loads(obj)
            except (ValueError, TypeError):
                pass
        elif self.is_django_object(obj):
            return self.serialize_django_object(obj)
        else:
            try:
                return obj.__dict__
            except AttributeError:
                pass

        raise client_exceptions.Save_InvalidClass('Saving invalid type')

    def serialize_object(self, obj):
        obj = self.serialize_object_by_type(obj)
        if obj.get('_id', None):
            if type(obj['_id']) is ObjectId:
                obj['_id'] = str(obj['_id'])
        return obj

    def save_to_mongo(self, application_name, objects_to_save, serializer_type):
        application_config = {}
        if not ((application_name in self.app_data) or self.redis.exists('redongo_{0}'.format(application_name))):
            raise client_exceptions.Save_InexistentAppSettings('Application settings for app {0} does not exist'.format(application_name))
        application_config = self.get_application_settings(application_name)
        if not hasattr(objects_to_save, '__iter__') or type(objects_to_save) == dict:
            objects_to_save = [objects_to_save]
        final_objects_to_save = []
        for obj in objects_to_save:
            obj = self.serialize_object(obj)
            final_objects_to_save.append(obj)
        if final_objects_to_save:
            ser = serializer_utils.serializer(application_config['serializer_type'])
            self.redis.rpush(self.redis_queue, *map(lambda x: pickle.dumps([(application_name, application_config['serializer_type']), ser.dumps(x)]), final_objects_to_save))

