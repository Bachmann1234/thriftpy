import os
import logging
import threading
import thriftpy
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.thrift import TMessageType, TApplicationException, TPayload
from thriftpy.contrib.finagle.tracing.trace import Trace

_UPGRADE_METHOD = "__can__finagle__trace__v3__"
logger = logging.getLogger(__name__)
finagle_thrift = thriftpy.load(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "contrib",
        "finagle",
        "tracing",
        "tracing.thrift"
    )
)

class TFinagleProtocol(TBinaryProtocol):
    """Implementation of Twitter's extension of the thrift binary protocol"""

    def __init__(self, trans, client_id,
                 strict_read=True, strict_write=True,
                 decode_response=True):
        super(TFinagleProtocol, self).__init__(
            trans,
            strict_read,
            strict_write,
            decode_response
        )
        self.client_id = client_id
        self.finagle_enabled = None
        self.locals = threading.local()

    def _attempt_finagle_upgrade(self):
        self.write_message_begin(_UPGRADE_METHOD, TMessageType.CALL, 0)
        self.write_message_end()
        self.trans.flush()

        api, ttype, _ = self.read_message_begin()
        if api != _UPGRADE_METHOD:
            return False
        if ttype == TMessageType.EXCEPTION:
            exc = TApplicationException()
            exc.read(self)
            self.read_message_end()
            raise exc
        reply = finagle_thrift.UpgradeReply()
        reply.read(self)
        self.read_message_end()
        return True

    def _twitter_enabled(self):
        if self.finagle_enabled is None:
            self.finagle_enabled = False
            try:
                logger.debug("Attempting finagle upgrade")
                self.finagle_enabled = self._attempt_finagle_upgrade()
            except TApplicationException as e:
                logger.warn(e)
                logger.warn(
                    "Unable to upgrade to finagle protocol. Falling back to binary",
                )
        return self.finagle_enabled


    def read_message_begin(self):
        if self._twitter_enabled():
            header = finagle_thrift.ResponseHeader()
            header.read(self)
            self.locals.last_response = header
        return super(TFinagleProtocol, self).read_message_begin()

    def write_message_begin(self, name, ttype, seqid):
        if self._twitter_enabled():
            if not hasattr(self.locals, 'trace'):
                self.locals.trace = Trace()
            trace_id = self.locals.trace.get()
            finagle_thrift.RequestHeader(
                trace_id=trace_id.trace_id.value,
                parent_span_id=trace_id.parent_id.value,
                span_id=trace_id.span_id.value,
                sampled=trace_id.sampled,
                client_id=self.client_id
            ).write(self)
            with self.locals.trace.push(trace_id):
                super(TFinagleProtocol, self).write_message_begin(
                    name, ttype, seqid
                )
        else:
            super(TFinagleProtocol, self).write_message_begin(
                name, ttype, seqid
            )

class TFinagleProtocolFactory(object):
    def __init__(self, client_id, strict_read=True, strict_write=True,
                 decode_response=True):
        self.strict_read = strict_read
        self.strict_write = strict_write
        self.decode_response = decode_response
        self.client_id = finagle_thrift.ClientId(name=client_id)

    def get_protocol(self, trans):
        return TFinagleProtocol(
            trans,
            self.client_id,
            self.strict_read, 
            self.strict_write,
            self.decode_response
        )
