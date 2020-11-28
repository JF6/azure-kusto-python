import logging
from logging.handlers import MemoryHandler


class FlushableMemoryHandler(MemoryHandler):
    """

    A MemoryHandler which can call the target flush method. This lets the target processing
    the effective sending upon flush instead of emit, increasing performance overall. 
    """
    def __init__(self, capacity, flushLevel=logging.ERROR, target=None,
                 flushOnClose=True, flushTarget=False):
        """
        Initialize the handler with the buffer size, the level at which
        flushing should occur and an optional target.
        Note that without a target being set either here or via setTarget(),
        a MemoryHandler is no use to anyone!
        The ``flushOnClose`` argument is ``True`` for backward compatibility
        reasons - the old behaviour is that when the handler is closed, the
        buffer is flushed, even if the flush level hasn't been exceeded nor the
        capacity exceeded. To prevent this, set ``flushOnClose`` to ``False``.
        The ``flushTarget`` argument is ``False`` for backward compatility reasons 
        as the target flush method was not called. Set it to ``True``to invoke the
        target's flush method.
        """
        MemoryHandler.__init__(self, capacity, flushLevel, target, flushOnClose)
        self.flushTarget = flushTarget


    def flush(self):
        """
        For a MemoryHandler, flushing means just sending the buffered
        records to the target, if there is one. Override if you want
        different behaviour.
        Invoke the target flush method if requested.
        The record buffer is also cleared by this operation.
        """
        self.acquire()
        try:
            if self.target:
                for record in self.buffer:
                    self.target.handle(record)
                if self.flushTarget:
                    self.target.flush()
                self.buffer.clear()
        finally:
            self.release()
