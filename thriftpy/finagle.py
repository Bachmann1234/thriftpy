import contextlib
import os
import logging
import threading
import thriftpy
from thriftpy import rpc
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.transport import TFramedTransportFactory
from thriftpy.thrift import TProcessor, TMessageType, TApplicationException, TPayload
from thriftpy.contrib.finagle.tracing.trace import Trace

_UPGRADE_METHOD = "__can__finagle__trace__v3__"
logger = logging.getLogger(__name__)
finagle_dir = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "contrib",
    "finagle",
)
tracing_thrift = thriftpy.load(
    os.path.join(finagle_dir, 'tracing', 'tracing.thrift')
)
finagle_thrift = thriftpy.load(
    os.path.join(finagle_dir, "finagle.thrift")
)
thread_local = threading.local()

class TFinagleProtocol(TBinaryProtocol):
    """Implementation of Twitter's extension of the thrift binary protocol"""

    def __init__(self, trans, client_id, finagle_connection_state,
                 strict_read=True, strict_write=True,
                 decode_response=True):
        super(TFinagleProtocol, self).__init__(
            trans,
            strict_read,
            strict_write,
            decode_response
        )
        if not hasattr(thread_local, 'finagle_enabled'):
            thread_local.finagle_enabled = False
        self.client_id = client_id

    def read_message_begin(self):
        if thread_local.finagle_enabled:
            header = tracing_thrift.ResponseHeader()
            header.read(self)
            thread_local.last_response = header
        return super(TFinagleProtocol, self).read_message_begin()

    def write_message_begin(self, name, ttype, seqid):
        if thread_local.finagle_enabled:
            if not hasattr(thread_local, 'trace'):
                thread_local.trace = Trace()
            print("DOG")
            trace_id = thread_local.trace.get()
            tracing_thrift.RequestHeader(
                trace_id=trace_id.trace_id.value,
                parent_span_id=trace_id.parent_id.value,
                span_id=trace_id.span_id.value,
                sampled=trace_id.sampled,
                client_id=self.client_id
            ).write(self)
            with thread_local.trace.push(trace_id):
                super(TFinagleProtocol, self).write_message_begin(
                    name, ttype, seqid
                )
        else:
            super(TFinagleProtocol, self).write_message_begin(
                name, ttype, seqid
            )

class TFinagleProtocolFactory(object):
    def __init__(
        self,
        client_id=None,
        strict_read=True,
        strict_write=True,
        decode_response=True
    ):
        self.strict_read = strict_read
        self.strict_write = strict_write
        self.decode_response = decode_response
        self.client_id = tracing_thrift.ClientId(name=client_id)

    def get_protocol(self, trans):
        return TFinagleProtocol(
            trans,
            self.client_id,
            self.strict_read, 
            self.strict_write,
            self.decode_response
        )

class TFinagleProcessor(TProcessor):
    def __init__(self, service, handler):
        service.thrift_services.append(_UPGRADE_METHOD)

        def reply(connection_options):
            thread_local.finagle_enabled = True
        
        setattr(
            service,
            "{}_args".format(_UPGRADE_METHOD), 
            getattr(
                finagle_thrift.FinagleUpgradeService,
                "{}_args".format(_UPGRADE_METHOD)
            )
        )
        setattr(
            service, 
            "{}_result".format(_UPGRADE_METHOD), 
            getattr(
                finagle_thrift.FinagleUpgradeService,
                "{}_result".format(_UPGRADE_METHOD)
            )
        )
        setattr(
            handler,
            _UPGRADE_METHOD, 
            reply
        )
        super(TFinagleProcessor, self).__init__(service, handler)
    
    # def process_in(self, iprot):
    #     if not hasattr(thread_local, 'finagle_enabled'):
    #         thread_local.finagle_enabled = False
    #     api, seqid, result, call = super(
    #         TFinagleProcessor, 
    #         self
    #     ).process_in(iprot)
    #     if api == _UPGRADE_METHOD:
    #         thread_local.finagle_enabled = True
    #         args = finagle_thrift.FinagleUpgradeService.upgrade_args().read(iprot)
    #         iprot.read_message_end()
    #         result = finagle_thrift.FinagleUpgradeService.upgrade_result()
    #         api_args = [args.thrift_spec[k][1] for k in sorted(args.thrift_spec)]
    #         def _call():
    #             finagle_thrift.FinagleUpgradeService.upgrade(
    #                 *(args.__dict__[k] for k in api_args)
    #             )
    #         iprot.read_message_end()
    #         result = UpgradeResult()
    #         call = _call
    #     return api, seqid, result, call

def make_client(
    service, 
    host="localhost", 
    port=9090, 
    unix_socket=None,
    proto_factory=None,
    trans_factory=TFramedTransportFactory(),
    timeout=None,
    cafile=None, 
    ssl_context=None,
    certfile=None,
    keyfile=None,
    client_id=None
):
    if not proto_factory:
        proto_factory = TFinagleProtocolFactory(
            client_id=client_id
        )
    client = rpc.make_client(
        service=service,
        host=host,
        port=port,
        unix_socket=unix_socket,
        proto_factory=proto_factory,
        trans_factory=trans_factory,
        timeout=timeout,
        cafile=cafile,
        ssl_context=ssl_context,
        certfile=certfile,
        keyfile=keyfile,
    )
    return client

def make_server(
    service, 
    handler,
    host="localhost", 
    port=9090, 
    unix_socket=None,
    proto_factory=TFinagleProtocolFactory(),
    trans_factory=TFramedTransportFactory(),
    client_timeout=3000, 
    certfile=None
):
    processor = TFinagleProcessor(service, handler)

    server = rpc.make_server(
        service=service,
        handler=handler,
        host=host,
        port=port,
        unix_socket=unix_socket,
        proto_factory=proto_factory,
        trans_factory=trans_factory,
        client_timeout=client_timeout,
        certfile=certfile
    )
    server.processor = processor
    return server


@contextlib.contextmanager
def client_context(    
    service, 
    host="localhost", 
    port=9090, 
    unix_socket=None,
    proto_factory=None,
    trans_factory=TFramedTransportFactory(),
    timeout=None,
    cafile=None, 
    ssl_context=None,
    certfile=None,
    keyfile=None,
    client_id=None
):
    if not proto_factory:
        proto_factory = TFinagleProtocolFactory(client_id=client_id)
    yield rpc.client_context(
        service=service,
        host=host,
        port=port,
        unix_socket=unix_socket,
        proto_factory=proto_factory,
        trans_factory=trans_factory,
        timeout=timeout,
        cafile=cafile,
        ssl_context=ssl_context,
        certfile=certfile,
        keyfile=keyfile
    )
    
