import os
from utils import redis_utils
from utils import cipher_utils
import client_exceptions
import ujson


class RedongoClient():
    def __init__(self, redis_host, redis_db, redis_queue, redis_port=6379):
        self.redis = redis_utils.get_redis_connection(redis_host, redis_db=redis_db, redis_port=redis_port)
        if redis_queue:
            self.redis_queue = redis_queue
        else:
            raise client_exceptions.Client_NoQueueParameter('Not valid queue received: {0}'.format(redis_queue))

    def set_application_settings(self, application_name, mongo_host, mongo_port, mongo_database, mongo_collection, mongo_user, mongo_password, bulk_size=100, bulk_expiration=60):

        def __get_sk__():
            result = self.redis.get('redongo_sk')
            if not result:
                result = os.urandom(16)
                self.redis.set('redongo_sk', result)
            return result

        if not application_name:
            raise client_exceptions.Register_NoApplicationName('Can\'t set application settings: No application name')
        app_data = {}
        app_data['mongo_host'] = mongo_host
        app_data['mongo_port'] = mongo_port if mongo_port else 27017
        app_data['mongo_database'] = mongo_database
        app_data['mongo_collection'] = mongo_collection
        app_data['mongo_user'] = mongo_user
        cipher = cipher_utils.AESCipher(__get_sk__())
        app_data['mongo_password'] = cipher.encrypt(mongo_password)
        app_data['bulk_size'] = bulk_size
        app_data['bulk_expiration'] = bulk_expiration

        for key, value in app_data.iteritems():
            if not value:
                raise client_exceptions.Register_NoAttributeReceived('Can\'t set application {1} settings: No value set for {0}'.format(key, application_name))

        self.redis.set('redongo_{0}'.format(application_name), ujson.dumps(app_data))

    def remove_application_settings(self, application_name):
        if not application_name:
            raise client_exceptions.Register_NoApplicationName('Can\'t remove application settings: No application name')
        self.redis.delete('redongo_{0}'.format(application_name))

    def serialize_django_object(self, obj):
        fields = set()
        excluded_fields = set(['_id'])
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
        # if issubclass(type(obj), models.Model)
        obj_class = type(obj)
        django_object = False
        while obj_class.__bases__:
            obj_class = obj_class.__bases__[0]
            if obj_class.__module__ == 'django.db.models.base' and obj_class.__name__ == 'Model':
                django_object = True
                break
        return django_object

    def save_to_mongo(self, application_name, objects_to_save):
        if not self.redis.exists('redongo_{0}'.format(application_name)):
            raise client_exceptions.Save_InexistentAppSettings('Application settings for app {0} does not exist'.format(application_name))
        if not hasattr(objects_to_save, '__iter__') or type(objects_to_save) == dict:
            objects_to_save = [objects_to_save]
        final_objects_to_save = []
        for obj in objects_to_save:
            valid = True
            if type(obj) != dict:
                if self.is_django_object(obj):
                    obj = self.serialize_django_object(obj)
                else:
                    valid = False

            if not valid:
                raise client_exceptions.Save_InvalidClass('Saving invalid class')
            final_objects_to_save.append(obj)
        self.redis.rpush(self.redis_queue, *map(lambda x: ujson.dumps([application_name, x]), final_objects_to_save))
