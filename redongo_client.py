import os
import redongo_utils
import ujson

class InvalidClass(Exception):
    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


class InexistentAppSettings(Exception):
    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)

class RedongoClient():
    def __init__(self, redis_host, redis_db, redis_queue, redis_port=6379):
        self.redis = redongo_utils.get_redis_connection(redis_host, redis_db=redis_db, redis_port=redis_port)
        if redis_queue:
            self.redis_queue = redis_queue
        else:
            raise Exception('Not valid queue received: {0}'.format(redis_queue))

    def set_application_settings(self, application_name, mongo_host, mongo_port, mongo_database, mongo_collection, mongo_user, mongo_password, bulk_size=100, bulk_expiration=60):

        def __get_sk__():
            result = self.redis.get('redongo_sk')
            if not result:
                result = os.urandom(16)
                self.redis.set('redongo_sk', result)
            return result

        if not application_name:
            raise Exception('Can\'t set application settings: No application name')
        app_data = {}
        app_data['mongo_host'] = mongo_host
        app_data['mongo_port'] = mongo_port if mongo_port else 27017
        app_data['mongo_database'] = mongo_database
        app_data['mongo_collection'] = mongo_collection
        app_data['mongo_user'] = mongo_user
        cipher = redongo_utils.AESCipher(__get_sk__())
        app_data['mongo_password'] = cipher.encrypt(mongo_password)
        app_data['bulk_size'] = bulk_size
        app_data['bulk_expiration'] = bulk_expiration

        for key, value in app_data.iteritems():
            if not value:
                raise Exception('Can\'t set application {1} settings: No value set for {0}'.format(key, application_name))

        self.redis.set('redongo_{0}'.format(application_name), ujson.dumps(app_data))

    def remove_application_settings(self, application_name):
        if not application_name:
            raise Exception('Can\'t remove application settings: No application name')
        self.redis.delete(application_name)

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
            raise InexistentAppSettings('Application settings for app {0} does not exist'.format(application_name))
        if not hasattr(objects_to_save, '__iter__') or type(objects_to_save) == str:
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
                raise InvalidClass('Saving invalid class')
            final_objects_to_save.append(obj)
        self.redis.rpush(self.redis_queue, *map(lambda x: ujson.dumps([application_name, x]), final_objects_to_save))
