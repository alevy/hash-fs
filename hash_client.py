import md5
import pickle
import sys

sys.path.insert(0, 'gen-py')
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import donut.service.HashService

class KeyLocatorClientFactory:
    def __init__(self, hostname, port):
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(hostname, port))
        self.client = donut.service.HashService.Client(TBinaryProtocol.TBinaryProtocol(self.transport))

    def build(self):
        self.transport.open()
        return self.client

    def destroy(self):
        self.transport.close()


class HashClient:
    def __init__(self, client):
        self.client = client

    def get(self, key):
        try:
            return pickle.loads(self.client.get(key))
        except donut.service.ttypes.DataNotFoundException:
            return None

    def put(self, key, data):
        self.client.put(key, pickle.dumps(data))

    def remove(self, key):
        self.client.remove(key)

