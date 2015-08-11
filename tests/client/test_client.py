import pytest
from redongo import redongo_client
from redongo import client_exceptions


class TestClient:
    def test_instanceclientOK(self):
        redongo_client.RedongoClient('localhost', 0, 'REDONGO_TEST_QUEUE')

    def test_instanceclientNOQUEUE(self):
        with pytest.raises(client_exceptions.NoQueueParameter):
            redongo_client.RedongoClient('localhost', 0, None)
