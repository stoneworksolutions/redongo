import base64
from Crypto.Cipher import AES
from Crypto import Random
import redis


class AESCipher:
    def __init__(self, key):
        self.key = key
        self.BS = 16
        self.pad = lambda s: s + (self.BS - len(s) % self.BS) * chr(self.BS - len(s) % self.BS)
        self.unpad = lambda s: s[:-ord(s[len(s)-1:])]

    def encrypt(self, raw):
        if not raw:
            return 'None'
        raw = self.pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def decrypt(self, enc):
        if enc == 'None':
            return None
        enc = base64.b64decode(enc)
        iv = enc[:16]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self.unpad(cipher.decrypt(enc[16:]))


def multi_lpop(redis, queue, number):
    ''' Pops multiple elements from a list in an atomic way :) should be on the specs! '''
    p = redis.pipeline(transaction=True)
    p.lrange(queue, 0, number - 1)
    p.ltrim(queue, number, -1)
    return p.execute()[0]


def get_redis_connection(redis_host, redis_db, redis_port=6379):
    r = redis.Redis(redis_host, db=redis_db, port=redis_port)
    try:
        if not r.ping():
            raise Exception('No redis ping')
    except:
        raise Exception('No Redis')
    return r
