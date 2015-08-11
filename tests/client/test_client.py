from redongo import redongo_client

class TestClient:
    def test_instanceclientOK(self):
	redongo_client.RedongoClient()
	assert 1 == 2

    def test_two(self):
        x = "hello"
        assert hasattr(x, 'check')
