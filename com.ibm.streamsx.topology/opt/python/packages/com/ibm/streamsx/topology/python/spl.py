# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

from enum import Enum
import functools
import inspect

OperatorType = Enum('OperatorType', 'Ignore Source Sink Pipe')
OperatorType.Pipe.spl_template = 'PythonFunctionPipe'
OperatorType.Sink.spl_template = 'PythonFunctionSink'

def pipe(wrapped):
    @functools.wraps(wrapped)
    def _pipe(*args, **kwargs):
        return wrapped(*args, **kwargs)
    _pipe.__splpy_optype = OperatorType.Pipe
    _pipe.__splpy_file = inspect.getsourcefile(wrapped)
    return _pipe

# Allows functions in any module in opt/python/streams to be explicitly ignored.
def ignore(wrapped):
    @functools.wraps(wrapped)
    def _ignore(*args, **kwargs):
        return wrapped(*args, **kwargs)
    _ignore.__splpy_optype = OperatorType.Ignore
    _ignore.__splpy_file = inspect.getsourcefile(wrapped)
    return _ignore

# Defines a function as a sink operator
def sink(wrapped):
    @functools.wraps(wrapped)
    def _sink(*args, **kwargs):
        ret = wrapped(*args, **kwargs)
        assert ret == None, "SPL @sink function must not return any value, except None"
        return None
    _sink.__splpy_optype = OperatorType.Sink
    _sink.__splpy_file = inspect.getsourcefile(wrapped)
    return _sink
