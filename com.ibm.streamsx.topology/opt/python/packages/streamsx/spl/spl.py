# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

from enum import Enum
import functools
import inspect

OperatorType = Enum('OperatorType', 'Ignore Source Sink Pipe')
OperatorType.Pipe.spl_template = 'PythonFunctionPipe'
OperatorType.Sink.spl_template = 'PythonFunctionSink'

PassBy = Enum('PassBy', 'name position')


def pipe(wrapped):
    """
    Create a SPL operator from a function.
    
    A pipe SPL operator with a single input port and a single
    output port. For each tuple on the input port the
    function is called passing the contents of the tuple.

    SPL attributes from the tuple are passed by position.
    
    The value returned from the function defines what is
    submitted to the output port. If None is returned then
    nothing is submitted. If a tuple is returned then it
    is c
    """
    if not inspect.isfunction(wrapped):
        raise TypeError('A function is required')
          
    @functools.wraps(wrapped)
    def _pipe(*args, **kwargs):
        return wrapped(*args, **kwargs)
    _pipe.__splpy_optype = OperatorType.Pipe
    _pipe.__splpy_callable = 'function'
    _pipe.__splpy_attributes = PassBy.position
    _pipe.__splpy_file = inspect.getsourcefile(wrapped)
    return _pipe

class map:
    """
    Create a SPL operator from a callable class or function.

    A map SPL operator with a single input port and a single
    output port. For each tuple on the input port the
    function is called passing the contents of the tuple.
    """

    def __init__(self, attributes=PassBy.name):
        if attributes is not PassBy.position:
            raise NotImplementedError(attributes)
        self.attributes = attributes

    def __call__(self, wrapped):
        if inspect.isclass(wrapped):
            if not callable(wrapped):
                raise TypeError('Class must be callable')
            class _map_class(object):
                def __init__(self,*args,**kwargs):
                    self.__splpy_instance = wrapped(*args,**kwargs)
                def __call__(self, *args,**kwargs):
                    return self.__splpy_instance.__call__(*args, **kwargs)
            _map_class.__splpy_optype = OperatorType.Pipe
            _map_class.__splpy_callable = 'class'
            _map_class.__splpy_attributes = self.attributes
            _map_class.__splpy_file = inspect.getsourcefile(wrapped)
            return _map_class
        if not inspect.isfunction(wrapped):
            raise TypeError('A function or callable class is required')
          
        @functools.wraps(wrapped)
        def _map_fn(*args, **kwargs):
            return wrapped(*args, **kwargs)
        _map_fn.__splpy_optype = OperatorType.Pipe
        _map_fn.__splpy_callable = 'function'
        _map_fn.__splpy_attributes = self.attributes
        _map_fn.__splpy_file = inspect.getsourcefile(wrapped)
        return _map_fn

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
    _sink.__splpy_callable = 'function'
    _sink.__splpy_file = inspect.getsourcefile(wrapped)
    return _sink
