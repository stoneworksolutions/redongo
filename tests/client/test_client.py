import pytest
import redis
from redongo import redongo_client
from redongo import client_exceptions

APPNAME = 'testApp'
APPNAME2 = 'testApp2'
APPNAME3 = 'testApp3'
APPNAME_FAKE = 'testAppFake'
REDIS_HOST = 'localhost'
REDIS_DB = 0
REDIS_QUEUE = 'REDONGO_TEST__QUEUE'
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'localhost'
MONGO_COLLECTION = 'localhost'
MONGO_USER = 'localhost'
MONGO_PASSWORD = 'localhost'
test_redongo = None


class TestClient:

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
            test_redongo.set_application_settings(APPNAME_FAKE, None, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD)

    def test__set_application_settings__NoOK2(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, None, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD)

    def test__set_application_settings__NoOK3(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, MONGO_DB, None, MONGO_USER, MONGO_PASSWORD)

    def test__set_application_settings__NoOK4(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, None, MONGO_PASSWORD)

    def test__set_application_settings__NoOK5(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, bulk_size=None)

    def test__set_application_settings__NoOK6(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoAttributeReceived):
            test_redongo.set_application_settings(APPNAME_FAKE, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD, bulk_size=100, bulk_expiration=None)

    def test__set_application_settings__NoOK7(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoApplicationName):
            test_redongo.set_application_settings('', MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD)

    def test__set_application_settings__OK(self):
        global test_redongo
        test_redongo.set_application_settings(APPNAME, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD)

    def test__set_application_settings__OK2(self):
        global test_redongo
        test_redongo.set_application_settings(APPNAME2, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD)

    def test__set_application_settings__OK3(self):
        global test_redongo
        test_redongo.set_application_settings(APPNAME3, MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION, MONGO_USER, MONGO_PASSWORD)

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

    def test__remove_application_settings__NoOK(self):
        global test_redongo
        with pytest.raises(client_exceptions.Register_NoApplicationName):
            test_redongo.remove_application_settings(None)

    def test__remove_application_settings__OK(self):
        global test_redongo
        test_redongo.remove_application_settings(APPNAME3)
