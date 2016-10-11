# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

from enum import Enum
import functools
import inspect
import sys
if sys.version_info.major == 2:
  import funcsigs

############################################
# setup for function inspection
if sys.version_info.major == 3:
  _inspect = inspect
elif sys.version_info.major == 2:
  _inspect = funcsigs
else:
  raise ValueError("Python version not supported.")
############################################

OperatorType = Enum('OperatorType', 'Ignore Source Sink Pipe')
OperatorType.Pipe.spl_template = 'PythonFunctionPipe'
OperatorType.Sink.spl_template = 'PythonFunctionSink'

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

    return _wrapforsplop(OperatorType.Pipe, wrapped, 'position', False)

#
# Wrap object for an SPL operator, either
# a callable class or function.
#
def _wrapforsplop(optype, wrapped, style, docpy):

    if inspect.isclass(wrapped):
        if not callable(wrapped):
            raise TypeError('Class must be callable')

        class _op_class(object):

            __doc__ = wrapped.__doc__
            @functools.wraps(wrapped.__init__)
            def __init__(self,*args,**kwargs):
                self.__splpy_instance = wrapped(*args,**kwargs)

            @functools.wraps(wrapped.__call__)
            def __call__(self, *args,**kwargs):
                return self.__splpy_instance.__call__(*args, **kwargs)

        _op_class.__wrapped__ = wrapped
        # _op_class.__doc__ = wrapped.__doc__
        _op_class.__splpy_optype = optype
        _op_class.__splpy_callable = 'class'
        _op_class.__splpy_style = _define_style(wrapped, wrapped.__call__, style)
        _op_class.__splpy_file = inspect.getsourcefile(wrapped)
        _op_class.__splpy_docpy = docpy
        return _op_class
    if not inspect.isfunction(wrapped):
        raise TypeError('A function or callable class is required')
      
    @functools.wraps(wrapped)
    def _op_fn(*args, **kwargs):
        return wrapped(*args, **kwargs)
    _op_fn.__splpy_optype = optype
    _op_fn.__splpy_callable = 'function'
    _op_fn.__splpy_style = _define_style(wrapped, wrapped, style)
    _op_fn.__splpy_file = inspect.getsourcefile(wrapped)
    _op_fn.__splpy_docpy = docpy
    return _op_fn

# define the SPL tuple passing style based
# upon the function signature and the decorator
# style parameter
def _define_style(wrapped, fn, style):
    has_args = False
    has_kwargs = False
    has_positional = False
    req_named = False
     
    pmds = _inspect.signature(fn).parameters
    itpmds = iter(pmds)
    # Skip self
    if inspect.isclass(wrapped):
        next(itpmds)

    pc = 0
    for pn in itpmds:
        pmd = pmds[pn]
        if pmd.kind == _inspect.Parameter.POSITIONAL_ONLY:
            raise TypeError('Positional only parameters are not supported:' + pn)
        elif pmd.kind == _inspect.Parameter.VAR_POSITIONAL:
            has_args = True
        elif pmd.kind == _inspect.Parameter.VAR_KEYWORD:
            has_kwargs = True
        elif pmd.kind == _inspect.Parameter.POSITIONAL_OR_KEYWORD:
            has_positional = True
        elif pmd.kind == _inspect.Parameter.KEYWORD_ONLY:
            if pmd.default is _inspect.Parameter.empty:
                req_named = True
        pc +=1
               
    # See if the requested style matches the signature.
    if style == 'position':
        if req_named:
             raise TypeError("style='position' not supported with a required named parameter.")
        elif pc == 1 and has_kwargs:
            raise TypeError("style='position' not supported with single **kwargs parameter.")
        elif pc == 1 and has_args:
            pass
        elif not has_positional:
            raise TypeError("style='position' not supported as no positional parameters exist.")
        # From an implementation point of view the values
        # are passed as a tuple and Python does the correct mapping
        style = 'tuple'

    elif style == 'name':
        if pc == 1 and has_args:
            raise TypeError("style='name' not supported with single *args parameter.")
        elif pc == 1 and has_kwargs:
            raise TypeError("style='name' not supported with single **kwargs parameter.")

    elif style is not None:
        raise TypeError("style=" + style + " unknown.")

    if style is None:
        if pc == 1 and has_kwargs:
            style = 'dictionary'
        elif pc == 1 and has_args:
            style = 'tuple'
        elif pc == 0:
            style = 'tuple'
        else:
            # Default to by name
            style = 'name'

    if style == 'tuple' and has_kwargs:
         raise TypeError("style='position' not implemented with **kwargs parameter.")

    if style == 'name':
         raise TypeError("Not yet implemented!")
    return style

class map:
    """
    Create a SPL operator from a callable class or function.

    A map SPL operator with a single input port and a single
    output port. For each tuple on the input port the
    function is called passing the contents of the tuple.
    """
    def __init__(self, style=None, docpy=True):
        self.style = style
        self.docpy = docpy
    
    def __call__(self, wrapped):
        return _wrapforsplop(OperatorType.Pipe, wrapped, self.style, self.docpy)

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
    if not inspect.isfunction(wrapped):
        raise TypeError('A function is required')

    return _wrapforsplop(OperatorType.Sink, wrapped, 'position', False)

# Defines a function as a sink operator
class for_each:
    """
    Create a SPL operator from a callable class or function.

    A SPL operator with a single input port and no output ports.
    For each tuple on the input port the
    class or function is called passing the contents of the tuple.
    """
    def __init__(self, style=None, docpy=True):
        self.style = style
        self.docpy = docpy

    def __call__(self, wrapped):
        return _wrapforsplop(OperatorType.Sink, wrapped, self.style, self.docpy)
