import pytest
import redis
from redongo import redongo_client
from redongo import client_exceptions, general_exceptions
try:
    from bson.objectid import ObjectId
except ImportError:
    from pymongo.objectid import ObjectId

APPNAME = 'testApp'
APPNAME2 = 'testApp2'
APPNAME3 = 'testApp3'
APPNAME_FAKE = 'testAppFake'
APPNAME_JSON = 'testAppJson'
APPNAME_UJSON = 'testAppuJson'
APPNAME_PICKLE = 'testAppPickle'
import os
env = os.getenv('TRAVIS')
if env == 'true':
    REDIS_HOST = 'localhost'
    MONGO_HOST = 'localhost'
    MONGO_DB = 'mydb_test'
else:
    REDIS_HOST = 'dev-redis'
    MONGO_HOST = 'dev-mongo'
    MONGO_DB = 'mgalan'
REDIS_DB = 0
REDIS_QUEUE = 'REDONGO_TEST_QUEUE'
MONGO_PORT = 27017
MONGO_COLLECTION = 'mycollection_test'
MONGO_USER = 'test'
MONGO_PASSWORD = 'test123'
SERIALIZER_JSON = 'json'
SERIALIZER_UJSON = 'ujson'
SERIALIZER_PICKLE = 'pickle'
test_redongo = None

OBJECTID = ObjectId()


class TestClient:

    def test__initialize(self):
        r = redis.Redis(REDIS_HOST, db=REDIS_DB)
        try:
            r.delete(*r.keys('{0}*'.format(REDIS_QUEUE)))
        except redis.exceptions.ResponseError:
            pass
        try:
            r.delete(*r.keys('REDONGO_*'))
        except redis.exceptions.ResponseError:
            pass
        try:
            r.delete(*r.keys('redongo_*'))
        except redis.exceptions.ResponseError:
            pass

    def test__RedongoClient__NoOK1(self):
        with pytest.raises(redis.ConnectionError):
            redongo_client.RedongoClient('192.168.33.33', 0, REDIS_QUEUE)

    def test__RedongoClient__NoOK2(self):
        with pytest.raises(client_exceptions.Client_NoQueueParameter):
            redongo_client.RedongoClient(REDIS_HOST, REDIS_DB, None)

    def test__RedongoClient__OK(self):
        global test_redongo
        test_redongo = redongo_client.RedongoClient(REDIS_HOST, REDIS_DB, REDIS_QUEUE)

    def test__set_application_settings__NoOK1(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, None, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__NoOK2(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, None, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__NoOK3(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, MONGO_DB, None, MONGO_USER, MONGO_PASSWORD, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__NoOK4(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, None, MONGO_PASSWORD, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__NoOK5(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, bulk_size=None, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__NoOK6(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, bulk_size=100, bulk_expiration=None, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__NoOK7(self):
        global test_redongo
        with pytest.raises(general_exceptions.Register_NoApplicationName):
            test_redongo.set_application_settings('', MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__OK1(self):
        global test_redongo
        test_redongo.set_application_settings(APPNAME, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, bulk_size=2, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__OK2(self):
        # This test will generate a FAILED_QUEUE element in Redongo Server test
        global test_redongo
        test_redongo.set_application_settings(APPNAME2, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, '123', bulk_size=1, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__OK3(self):
        global test_redongo
        test_redongo.set_application_settings(APPNAME3, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__OK4(self):
        global test_redongo
        test_redongo.set_application_settings(APPNAME_JSON, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, bulk_size=1, serializer_type=SERIALIZER_JSON)

    def test__set_application_settings__OK5(self):
        global test_redongo
        test_redongo.set_application_settings(APPNAME_UJSON, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, bulk_size=1, serializer_type=SERIALIZER_UJSON)

    def test__set_application_settings__OK6(self):
        global test_redongo
        test_redongo.set_application_settings(APPNAME_PICKLE, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, bulk_size=1, serializer_type=SERIALIZER_PICKLE)

    def test__save_to_mongo__NoOK1(self):
        global test_redongo
        with pytest.raises(client_exceptions.Save_InexistentAppSettings):
            test_redongo.save_to_mongo(APPNAME_FAKE, [{'test': 1}])

    def test__save_to_mongo__NoOK2(self):
        global test_redongo
        with pytest.raises(client_exceptions.Save_InvalidClass):
            test_redongo.save_to_mongo(APPNAME, ['test'])

    def test__save_to_mongo__OK1(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME, [{'test': 1}])

    def test__save_to_mongo__OK2(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME, {'test': 2})

    def test__save_to_mongo__OK3(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME2, {'test': 3})

    def test__save_to_mongo__OK4(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME, {'_id': '123', 'test': 4})

    def test__save_to_mongo__OK5(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME, {'_id': 123, 'test': 4})

    def test__save_to_mongo__OK6(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME, {'_id': OBJECTID, 'test': 4})

    def test__save_to_mongo__OK7(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME_JSON, {'test': 'json'})

    def test__save_to_mongo__OK8(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME_UJSON, {'test': 'ujson'})

    def test__save_to_mongo__OK9(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME_PICKLE, {'test': 'pickle'})

    def test__update_to_mongo__OK1(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME, {'_id': '123', 'test': 5})

    def test__update_to_mongo__OK2(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME, {'_id': 123, 'test': 5})

    def test__update_to_mongo__OK3(self):
        global test_redongo
        test_redongo.save_to_mongo(APPNAME, {'_id': OBJECTID, 'test': 5})

    def test__add_in_mongo__OK1(self):
        global test_redongo
        obj = {
            '_id': 123454321,
            'int_field': 1,
            'long_field': 1L,
            'float_field': 1.0,
            # 'complex_field': 1j,
            'list_field': ['list_element'],
            'dict_field': {
                'int_field': 1,
                'long_field': 1L,
                'float_field': 1.0,
                # 'complex_field': 1j,
                'list_field': ['list_element'],
            }
        }
        test_redongo.add_in_mongo(APPNAME_PICKLE, [obj]*100)

    def test__remove_application_settings__NoOK(self):
        global test_redongo
        with pytest.raises(general_exceptions.Register_NoApplicationName):
            test_redongo.remove_application_settings(None)

    def test__remove_application_settings__OK(self):
        global test_redongo
        test_redongo.remove_application_settings(APPNAME3)

    def test__get_application_settings__OK(self):
        global test_redongo
        test_redongo.get_application_settings(APPNAME)

    def test__get_application_settings__NoOK1(self):
        global test_redongo
        with pytest.raises(general_exceptions.Register_NoApplicationName):
            test_redongo.get_application_settings(None)

    def test__get_application_settings__NoOK2(self):
        global test_redongo
        with pytest.raises(general_exceptions.Register_NoApplicationName):
            test_redongo.get_application_settings('')

    def test__get_application_settings__NoOK3(self):
        global test_redongo
        with pytest.raises(general_exceptions.ApplicationSettingsError):
            test_redongo.get_application_settings(APPNAME3)

    def test__results_assert(self):
        r = redis.Redis(REDIS_HOST, db=REDIS_DB)
        # 2 app settings + redongo_sk
        assert len(r.keys('redongo*')) == 6
        # 3 elements in queue
        assert r.llen(REDIS_QUEUE) == 112
