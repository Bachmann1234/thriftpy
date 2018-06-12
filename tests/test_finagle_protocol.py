import multiprocessing
import os
import time
import pytest
import thriftpy
from thriftpy.rpc import make_server, client_context
from thriftpy.protocol import TFinagleProtocolFactory
from thriftpy.transport import TFramedTransportFactory

addressbook = thriftpy.load(os.path.join(os.path.dirname(__file__),
                                         "addressbook.thrift"))


class Handler():
    def hello(self, name):
        return "hello " + name


@pytest.fixture(scope="module")
def server(request):
    server = make_server(
        addressbook.AddressBookService,
        Handler(),
        host="127.0.0.1",
        port=6080,
        trans_factory=TFramedTransportFactory(),
        proto_factory=TFinagleProtocolFactory()
    )
    ps = multiprocessing.Process(target=server.serve)
    ps.start()

    time.sleep(0.1)

    def fin():
        if ps.is_alive():
            ps.terminate()
    request.addfinalizer(fin)


def client():
    return client_context(
        addressbook.AddressBookService,
        host="127.0.0.1",
        port=6080,
        trans_factory=TFramedTransportFactory(),
        proto_factory=TFinagleProtocolFactory(client_id='finagle-tests'),
    )


def test_string_api(server):
    with client() as c:
        assert c.hello("world") == "hello world"

if __name__ == '__main__':
    class Bla():
        def __init__(self):
            self.finalizers = []
        
        def addfinalizer(self, fin):
            self.finalizers.append(fin)
    
    dog = Bla()
    server(dog)
    try:
        test_string_api(None)
    finally:
        for f in dog.finalizers:
            f()
