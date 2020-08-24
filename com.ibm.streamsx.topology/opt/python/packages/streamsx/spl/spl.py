# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2019
"""
SPL Python primitive operators.

********
Overview
********

SPL primitive operators that call a Python function or
class methods are created by decorators provided by this module.

The name of the function or callable class becomes the name of the
operator.

A decorated function is a stateless operator while a decorated class
is an optionally stateful operator.

These are the supported decorators that create an SPL operator:

    * :py:class:`@spl.source <source>` - Creates a source operator that produces tuples.
    * :py:class:`@spl.filter <filter>` - Creates a operator that filters tuples.
    * :py:class:`@spl.map <map>` - Creates a operator that maps input tuples to output tuples.
    * :py:class:`@spl.for_each <for_each>` - Creates a operator that terminates a stream processing each tuple.
    * :py:class:`@spl.primitive_operator <primitive_operator>` - Creates an SPL primitive operator that has an arbitrary number of input and output ports.

Decorated functions and classes must be located in the directory
``opt/python/streams`` in the SPL toolkit. Each module in that directory
will be inspected for operators during extraction. Each module defines
the SPL namespace for its operators by the function ``spl_namespace``,
for example::

    from streamsx.spl import spl

    def spl_namespace():
        return 'com.example.ops'

    @spl.map()
    def Pass(*tuple_):
        return tuple_

creates a pass-through operator ``com.example.ops::Pass``.

SPL primitive operators are created by executing the extraction script :ref:`spl-py-extract` against the SPL toolkit. Once created the operators become part
of the toolkit and may be used like any other SPL operator.

*******************************
Python classes as SPL operators
*******************************

Overview
========

Decorating a Python class creates a stateful SPL operator
where the instance fields of the class are the operator's state. An instance
of the class is created when the SPL operator invocation is initialized
at SPL runtime. The instance of the Python class is private to the SPL
operator and is maintained for the lifetime of the operator.

If the class has instance fields then they are the state of the
operator and are private to each invocation of the operator.

If the `__init__` method has parameters beyond the first
`self` parameter then they are mapped to operator parameters.
Any parameter that has a default value becomes an optional parameter
to the SPL operator. Parameters of the form `\*args` and `\*\*kwargs`
are not supported.

.. warning::
    Parameter names must be valid SPL identifers,
    SPL identifiers start with an ASCII letter or underscore,
    followed by ASCII letters, digits, or underscores.
    The name also must not be an SPL keyword.

    Parameter names ``suppress`` and ``include`` are reserved.

The value of the operator parameters at SPL operator invocation are passed
to the `__init__` method. This is equivalent to creating an instance
of the class passing the operator parameters into the constructor.

For example, with this decorated class producing an SPL source
operator::

    @spl.source()
    class Range(object):
      def __init__(self, stop, start=0):
        self.start = start
        self.stop = stop

      def __iter__(self):
          return zip(range(self.start, self.stop))

The SPL operator `Range` has two parameters, `stop` is mandatory and `start` is optional, defaulting to zero. Thus the SPL operator may be invoked as::

    // Produces the sequence of values from 0 to 99
    //
    // Creates an instance of the Python class
    // Range using Range(100)
    //
    stream<int32 seq> R = Range() {
      param
        stop: 100;
    }

or both operator parameters can be set::

    // Produces the sequence of values from 50 to 74
    //
    // Creates an instance of the Python class
    // Range using Range(75, 50)
    //
    stream<int32 seq> R = Range() {
      param
        start: 50;
        stop: 75;
    }

Operator state
==============

Use of a class allows the operator to be stateful by maintaining state in instance
attributes across invocations (tuple processing).

When the operator is in a consistent region or checkpointing then it is serialized using `dill`. The default serialization may be modified by using the standard Python pickle mechanism of ``__getstate__`` and ``__setstate__``. This is required if the state includes objects that cannot be serialized, for example file descriptors. For details see See https://docs.python.org/3.5/library/pickle.html#handling-stateful-objects .

If the class has ``__enter__`` and ``__exit__`` context manager methods then ``__enter__`` is called after the instance has been deserialized by `dill`. Thus ``__enter__`` is used to recreate runtime objects that cannot be serialized such as open files or sockets.

Operator initialization & shutdown
==================================

Execution of an instance for an operator effectively run in a context manager so that an instance's ``__enter__``
method is called when the processing element containing the operator is initialized
and its ``__exit__`` method called when the processing element is stopped. To take advantage of this
the class must define both ``__enter__`` and ``__exit__`` methods.

.. note::
    Initialization such as opening files should be in ``__enter__``
    in order to support stateful operator restart & checkpointing.

Example of using ``__enter__`` and ``__exit__`` to open and close a file::

    import streamsx.ec as ec

    @spl.map()
    class Sentiment(object):
        def __init__(self, name):
            self.name = name
            self.file = None

        def __enter__(self):
            self.file = open(self.name, 'r')

        def __exit__(self, exc_type, exc_value, traceback):
            if self.file is not None:
                self.file.close()

        def __call__(self):
            pass

When an instance defines a valid ``__exit__`` method then it will be called with an exception when:

 * the instance raises an exception during processing of a tuple
 * a data conversion exception is raised converting a Python value to an SPL tuple or attribute

If ``__exit__`` returns a true value then the exception is suppressed and processing continues, otherwise the enclosing processing element will be terminated.

Application log and trace
=========================

IBM Streams provides application trace and log services which are
accesible through standard Python loggers from the `logging` module.

See :ref:`streams_app_log_trc`.

*********************************
Python functions as SPL operators
*********************************

Decorating a Python function creates a stateless SPL operator.
In SPL terms this is similar to an SPL Custom operator, where
the code in the Python function is the custom code. For
operators with input ports the function is called for each
input tuple, passing a Python representation of the SPL input tuple.
For an SPL source operator the function is called to obtain an iterable
whose contents will be submitted to the output stream as SPL tuples.

Operator parameters are not supported.

An example SPL sink operator that prints each input SPL tuple after
its conversion to a Python tuple::

    @spl.for_each()
    def PrintTuple(*tuple_):
        "Print each tuple to standard out."
         print(tuple_, flush=True)

.. _spl-tuple-to-python:

*******************************
Processing SPL tuples in Python
*******************************

SPL tuples are converted to Python objects and passed to a decorated callable.

Overview
========

For each SPL tuple arriving at an input port a Python function is called with
the SPL tuple converted to Python values suitable for the function call.
How the tuple is passed is defined by the tuple passing style.

Tuple Passing Styles
====================

An input tuple can be passed to Python function using a number of different styles:
 * *dictionary*
 * *tuple*
 * *attributes by name* **not yet implemented**
 * *attributes by position*

Dictionary
----------

Passing the SPL tuple as a Python dictionary is flexible
and makes the operator independent of any schema.
A disadvantage is the reduction in code readability
for Python function by not having formal parameters,
though getters such as ``tuple['id']`` mitigate that to some extent.
If the function is general purpose and can derive meaning
from the keys that are the attribute names then ``**kwargs`` can be useful.

When the only function parameter is ``**kwargs``
(e.g. ``def myfunc(**tuple_):``) then the passing style is *dictionary*.

All of the attributes are passed in the dictionary
using the SPL schema attribute name as the key.

Tuple
-----

Passing the SPL tuple as a Python tuple is flexible
and makes the operator independent of any schema
but is brittle to changes in the SPL schema.
Another disadvantage is the reduction in code readability
for Python function by not having formal parameters.
However if the function is general purpose and independent
of the tuple contents ``*args`` can be useful.

When the only function parameter is ``*args``
(e.g. ``def myfunc(*tuple_):``) then the passing style is *tuple*.

All of the attributes are passed as a Python tuple
with the order of values matching the order of the SPL schema.

Attributes by name
------------------
(**not yet implemented**)

Passing attributes by name can be robust against changes
in the SPL scheme, e.g. additional attributes being added in
the middle of the schema, but does require that the SPL schema
has matching attribute names.

When *attributes by name* is used then SPL tuple attributes
are passed to the function by name for formal parameters.
Order of the attributes and parameters need not match.
This is supported for function parameters of
kind ``POSITIONAL_OR_KEYWORD`` and ``KEYWORD_ONLY``.

If the function signature also contains a parameter of the form
``**kwargs`` (``VAR_KEYWORD``) then any attributes not bound to
formal parameters are passed in its dictionary using the
SPL schema attribute name as the key.

If the function signature also contains an arbitrary argument
list ``*args`` then any attributes not bound to formal parameters
or to ``**kwargs`` are passed in order of the SPL schema.

If there are only formal parameters any non-bound attributes
are not passed into the function.

Attributes by position
----------------------

Passing attributes by position allows the SPL operator to
be independent of the SPL schema but is brittle to
changes in the SPL schema. For example a function expecting
an identifier and a sensor reading as the first two attributes
would break if an attribute representing region was added as
the first SPL attribute.

When *attributes by position* is used then SPL tuple attributes are
passed to the function by position for formal parameters.
The first SPL attribute in the tuple is passed as the first parameter.
This is supported for function parameters of kind `POSITIONAL_OR_KEYWORD`.

If the function signature also contains an arbitrary argument
list `\*args` (`VAR_POSITIONAL`) then any attributes not bound
to formal parameters are passed in order of the SPL schema.

The function signature must not contain a parameter of the form
``**kwargs`` (`VAR_KEYWORD`).

If there are only formal parameters any non-bound attributes
are not passed into the function.

The SPL schema must have at least the number of positional arguments
the function requires.

Selecting the style
===================

For signatures only containing a parameter of the form 
``*args`` or ``**kwargs`` the style is implicitly defined:

 * ``def f(**tuple_)`` - *dictionary* - ``tuple_`` will contain a dictionary of all of the SPL tuple attribute's values with the keys being the attribute names.
 * ``def f(*tuple_)`` - *tuple* - ``tuple_`` will contain all of the SPL tuple attribute's values in order of the SPL schema definition.

Otherwise the style is set by the ``style`` parameter to the decorator,
defaulting to *attributes by name*. The style value can be set to:

  * ``'name'`` - *attributes by name* (the default)
  * ``'position'`` - *attributes by position*

Examples
========

These examples show how an SPL tuple with the schema and value::

    tuple<rstring id, float64 temp, boolean increase>
    {id='battery', temp=23.7, increase=true}

is passed into a variety of functions by showing the effective Python
call and the resulting values of the function's parameters.

*Dictionary* consuming all attributes by ``**kwargs``::

    @spl.map()
    def f(**tuple_)
        pass
    # f({'id':'battery', 'temp':23.7, 'increase': True})
    #     tuple_={'id':'battery', 'temp':23.7, 'increase':True}

*Tuple* consuming all attributes by ``*args``::

    @spl.map()
    def f(*tuple_)
        pass
    # f('battery', 23.7, True)
    #     tuple_=('battery',23.7, True)

*Attributes by name* consuming all attributes::

    @spl.map()
    def f(id, temp, increase)
        pass
    # f(id='battery', temp=23.7, increase=True)
    #     id='battery'
    #     temp=23.7
    #     increase=True

*Attributes by name* consuming a subset of attributes::

    @spl.map()
    def f(id, temp)
        pass
    # f(id='battery', temp=23.7)
    #    id='battery'
    #    temp=23.7

*Attributes by name* consuming a subset of attributes in a different order::

    @spl.map()
    def f(increase, temp)
        pass
    # f(temp=23.7, increase=True)
    #    increase=True
    #    temp=23.7

*Attributes by name* consuming `id` by name and remaining attributes by ``**kwargs``::

    @spl.map()
    def f(id, **tuple_)
        pass
    # f(id='battery', {'temp':23.7, 'increase':True})
    #    id='battery'
    #    tuple_={'temp':23.7, 'increase':True}

*Attributes by name* consuming `id` by name and remaining attributes by ``*args``::

    @spl.map()
    def f(id, *tuple_)
        pass
    # f(id='battery', 23.7, True)
    #    id='battery'
    #    tuple_=(23.7, True)

*Attributes by position* consuming all attributes::

    @spl.map(style='position')
    def f(key, value, up)
         pass
    # f('battery', 23.7, True)
    #    key='battery'
    #    value=23.7
    #    up=True

*Attributes by position* consuming a subset of attributes::

    @spl.map(style='position')
    def f(a, b)
       pass
    # f('battery', 23.7)
    #    a='battery'
    #    b=23.7

*Attributes by position* consuming `id` by position and remaining attributes by ``*args``::

    @spl.map(style='position')
    def f(key, *tuple_)
        pass
    # f('battery', 23.7, True)
    #    key='battery'
    #    tuple_=(23.7, True)

In all cases the SPL tuple must be able to provide all parameters
required by the function. If the SPL schema is insufficient then
an error will result, typically an SPL compile time error.

The SPL schema can provide a subset of the formal parameters if the
remaining attributes are optional (having a default).

*Attributes by name* consuming a subset of attributes with an optional parameter not matched by the schema::

    @spl.map()
    def f(id, temp, pressure=None)
       pass
    # f(id='battery', temp=23.7)
    #     id='battery'
    #     temp=23.7
    #     pressure=None

.. _submit-from-python:

************************************
Submission of SPL tuples from Python
************************************

The return from a decorated callable results in submission of SPL tuples
on the associated outut port.

A Python function must return:
 * ``None``
 * a Python tuple
 * a Python dictionary
 * a list containing any of the above.

None
====

When ``None`` is return then no tuple will be submitted to
the operator output port.

Python tuple
============

When a Python tuple is returned it is converted to an SPL tuple and
submitted to the output port.

The values of a Python tuple are assigned to an output SPL tuple by position,
so the first value in the Python tuple is assigned to the first attribute
in the SPL tuple::

    # SPL input schema: tuple<int32 x, float64 y>
    # SPL output schema: tuple<int32 x, float64 y, float32 z>
    @spl.map(style='position')
    def myfunc(a,b):
       return (a,b,a+b)

    # The SPL output will be:
    # All values explictly set by returned Python tuple
    # based on the x,y values from the input tuple
    # x is set to: x 
    # y is set to: y
    # z is set to: x+y

The returned tuple may be *sparse*, any attribute value in the tuple
that is ``None`` will be set to their SPL default or copied from
a matching attribute in the input tuple
(same name and type,
or same name and same type as the underlying type of an output attribute
with an optional type),
depending on the operator kind::
    
    # SPL input schema: tuple<int32 x, float64 y>
    # SPL output schema: tuple<int32 x, float64 y, float32 z>
    @spl.map(style='position')
    def myfunc(a,b):
       return (a,None,a+b)

    # The SPL output will be:
    # x is set to: x (explictly set by returned Python tuple)
    # y is set to: y (set by matching input SPL attribute)
    # z is set to: x+y

When a returned tuple has fewer values than attributes in the SPL output
schema the attributes not set by the Python function will be set
to their SPL default or copied from
a matching attribute in the input tuple
(same name and type,
or same name and same type as the underlying type of an output attribute
with an optional type),
depending on the operator kind::
    
    # SPL input schema: tuple<int32 x, float64 y>
    # SPL output schema: tuple<int32 x, float64 y, float32 z>
    @spl.map(style='position')
    def myfunc(a,b):
       return a,

    # The SPL output will be:
    # x is set to: x (explictly set by returned Python tuple)
    # y is set to: y (set by matching input SPL attribute)
    # z is set to: 0 (default int32 value)

When a returned tuple has more values than attributes in the SPL output schema then the additional values are ignored::

    # SPL input schema: tuple<int32 x, float64 y>
    # SPL output schema: tuple<int32 x, float64 y, float32 z>
    @spl.map(style='position')
    def myfunc(a,b):
       return (a,b,a+b,a/b)

    # The SPL output will be:
    # All values explictly set by returned Python tuple
    # based on the x,y values from the input tuple
    # x is set to: x
    # y is set to: y
    # z is set to: x+y
    #
    # The fourth value in the tuple a/b = x/y is ignored.

Python dictionary
=================

A Python dictionary is converted to an SPL tuple for submission to
the associated output port. An SPL attribute is set from the
dictionary if the dictionary contains a key equal to the attribute
name. The value is used to set the attribute, unless the value is
``None``.

If the value in the dictionary is ``None``, or no matching key exists,
then the attribute value is set to its SPL default or copied from
a matching attribute in the input tuple (same name and type,
or same name and same type as the underlying type of an output attribute
with an optional type), depending on the operator kind.

Any keys in the dictionary that do not map to SPL attribute names are ignored.
    
Python list
===========

When a list is returned, each value is converted to an SPL tuple and
submitted to the output port, in order of the list starting with the
first element (position 0). If the list contains `None` at an index
then no SPL tuple is submitted for that index.

The list must only contain Python tuples, dictionaries or `None`. The list
can contain a mix of valid values.

The list may be empty resulting in no tuples being submitted.

"""

from enum import Enum

__all__ = ['source', 'map', 'filter', 'for_each', 'PrimitiveOperator', 'input_port', 'primitive_operator', 'extracting', 'ignore']

import functools
import inspect
import re
import sys
import streamsx.ec as ec
import streamsx._streams._runtime
import importlib
import warnings

import streamsx._streams._version
__version__ = streamsx._streams._version.__version__

# Used to recreate instances of decorated operators
# from their module & class name during pickleling (dill)
# See __reduce__ implementation below
def _recreate_op(op_module, op_name):
    module_ = importlib.import_module(op_module)
    class_ = getattr(module_, op_name)
    return class_.__new__(class_)

_OperatorType = Enum('_OperatorType', 'Ignore Source Sink Pipe Filter Primitive')
_OperatorType.Source.spl_template = 'PythonFunctionSource'
_OperatorType.Pipe.spl_template = 'PythonFunctionPipe'
_OperatorType.Sink.spl_template = 'PythonFunctionSink'
_OperatorType.Filter.spl_template = 'PythonFunctionFilter'
_OperatorType.Primitive.spl_template = 'PythonPrimitive'

_SPL_KEYWORDS = {'as', 'attribute', 
                 'blob', 'boolean', 'break',
                 'complex32', 'complex64', 'composite', 'config', 'continue',
                 'decimal128', 'decimal32', 'decimal64', 'do',
                 'else', 'enum', 'expression',
                 'false', 'float32', 'float64', 'for', 'function',
                 'graph',
                 'if', 'in', 'input', 'int', 'int16', 'int32', 'int64', 'int8',
                 'list', 'logic',
                 'map', 'mutable',
                 'namespace', 'null',
                 'onProcess', 'onPunct', 'onTuple', 'operator', 'optional', 'output',
                 'param', 'public',
                 'return', 'rstring',
                 'set', 'state', 'stateful', 'static', 'stream', 'streams',
                 'timestamp', 'true', 'tuple', 'type',
                 'uint16', 'uint32', 'uint64', 'uint8', 'use', 'ustring',
                 'void',
                 'while', 'window',
                 'xml'}

def _is_identifier(id):
    return re.match('^[a-zA-Z_][a-zA-Z_0-9]*$', id) and id not in _SPL_KEYWORDS

def _valid_identifier(id):
    if not _is_identifier(id):
        raise ValueError("{0} is not a valid SPL identifier".format(id))


def _valid_op_parameter(name):
    _valid_identifier(name)
    if name in ['suppress', 'include']:
        raise ValueError("Parameter name {0} is reserved".format(name))

_EXTRACTING=False

def extracting():
    """Is a module being loaded by ``spl-python-extract``.

    This can be used by modules defining SPL primitive operators
    using decorators such as :py:class:`@spl.map <map>`, to avoid
    runtime behavior. Typically not importing modules that are
    not available locally. The extraction script loads the module
    to determine method signatures and thus does not invoke any methods.

    For example if an SPL toolkit with primitive operators requires
    a package ``extras`` and is using ``opt/python/streams/requirements.txt``
    to include it, then loading it at extraction time can be avoided by::

        from streamsx.spl import spl

        def spl_namespace():
            return 'myns.extras'

        if not spl.extracting():
            import extras

        @spl.map():
        def myextras(*tuple_):
            return extras.process(tuple_)
 
    .. versionadded:: 1.11
    """
    return _EXTRACTING


#
# Wrap object for an SPL operator, either
# a callable class or function.
#
def _wrapforsplop(optype, wrapped, style, docpy):

    if inspect.isclass(wrapped):
        if not callable(wrapped):
            raise TypeError('Class must be callable')

        _valid_identifier(wrapped.__name__)

        class _op_class(wrapped):
            __doc__ = wrapped.__doc__

            _splpy_wrapped = wrapped
            _splpy_optype = optype
            _splpy_callable = 'class'
            _streamsx_ec_cls = True
            _streamsx_ec_context = streamsx._streams._runtime._has_context_methods(wrapped)

            @functools.wraps(wrapped.__init__)
            def __init__(self,*args,**kwargs):
                super(_op_class, self).__init__(*args,**kwargs)
                self._streamsx_ec_entered = False

            # Use reduce to save the state of the class and its
            # module and operator name.
            def __reduce__(self):
                if hasattr(self, '__getstate__'):
                    state = self.__getstate__()
                else:
                    state = self.__dict__
                return _recreate_op, (wrapped.__module__, wrapped.__name__), state

        if optype in (_OperatorType.Sink, _OperatorType.Pipe, _OperatorType.Filter):
            _op_class._splpy_style = _define_style(wrapped, wrapped.__call__, style)
            _op_class._splpy_fixed_count = _define_fixed(_op_class, _op_class.__call__)
        else:
            _op_class._splpy_style = ''
            _op_class._splpy_fixed_count = -1
     
        _op_class._splpy_file = inspect.getsourcefile(wrapped)
        _op_class._splpy_docpy = docpy
        return _op_class
    if not inspect.isfunction(wrapped):
        raise TypeError('A function or callable class is required')

    _valid_identifier(wrapped.__name__)

    #fnstyle =

    #if fnstyle == 'tuple':
    #    @functools.wraps(wrapped)
    #    def _op_fn(*args):
    #        return wrapped(args)
    #else:
    #    @functools.wraps(wrapped)
    #    def _op_fn(*args, **kwargs):
    #       return wrapped(*args, **kwargs)
    _op_fn = wrapped

    _op_fn._splpy_optype = optype
    _op_fn._splpy_callable = 'function'
    _op_fn._splpy_style = _define_style(_op_fn, _op_fn, style)
    _op_fn._splpy_fixed_count = _define_fixed(_op_fn, _op_fn)
    _op_fn._splpy_file = inspect.getsourcefile(wrapped)
    _op_fn._splpy_docpy = docpy
    _op_fn._streamsx_ec_cls = False
    _op_fn._streamsx_ec_context = False
    return _op_fn

# define the SPL tuple passing style based
# upon the function signature and the decorator
# style parameter
def _define_style(wrapped, fn, style):
    has_args = False
    has_kwargs = False
    has_positional = False
    req_named = False
     
    pmds = inspect.signature(fn).parameters
    itpmds = iter(pmds)
    # Skip self
    if inspect.isclass(wrapped):
        next(itpmds)

    pc = 0
    for pn in itpmds:
        pmd = pmds[pn]
        if pmd.kind == inspect.Parameter.POSITIONAL_ONLY:
            raise TypeError('Positional only parameters are not supported:' + pn)
        elif pmd.kind == inspect.Parameter.VAR_POSITIONAL:
            has_args = True
        elif pmd.kind == inspect.Parameter.VAR_KEYWORD:
            has_kwargs = True
        elif pmd.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            has_positional = True
        elif pmd.kind == inspect.Parameter.KEYWORD_ONLY:
            if pmd.default is inspect.Parameter.empty:
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

def _define_fixed(wrapped, callable_):
    """For the callable see how many positional parameters are required"""
    is_class = inspect.isclass(wrapped)
    style = callable_._splpy_style if hasattr(callable_, '_splpy_style') else wrapped._splpy_style

    if style == 'dictionary':
        return -1

    fixed_count = 0
    if style == 'tuple':
        sig = inspect.signature(callable_)
        pmds = sig.parameters
        itpmds = iter(pmds)
        # Skip 'self' for classes
        if is_class:
            next(itpmds)

        for pn in itpmds:
            param = pmds[pn]
            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                fixed_count += 1
            if param.kind == inspect.Parameter.VAR_POSITIONAL: # *args
                fixed_count = -1
                break
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                break
    return fixed_count

class source(object):
    """
    Create a source SPL operator from an iterable.
    The resulting SPL operator has a single output port.

    When decorating a class the class must be iterable
    having an ``__iter__`` function. When the SPL operator
    is invoked an instance of the class is created
    and an iteration is created using ``iter(instance)``. 

    When decoratiing a function the function must have no
    parameters and must return an iterable or iteration.
    When the SPL operator is invoked the function is called
    and an iteration is created using ``iter(value)``
    where ``value`` is the return of the function.

    For each value in the iteration SPL zero or more tuples
    are submitted to the output port, derived from the value,
    see :ref:`submit-from-python`.
    
    If the iteration completes then no more tuples
    are submitted and a window punctuation mark followed
    by final punctuation mark are submitted to the output port.

    Example definition::

        @spl.source()
        class Range(object):
            def __init__(self, stop, start=0):
                self.start = start
                self.stop = stop

            def __iter__(self):
                return zip(range(self.start, self.stop))

    Example SPL invocation::

        stream<int32 seq> R = Range() {
            param
                stop: 100;
        }

    If ``__iter__`` or ``__next__`` block then shutdown, checkpointing
    or consistent region processing may be delayed. Having ``__next__``
    return ``None`` (no available tuples) or tuples to submit
    will allow such processing to proceed.

    A shutdown ``threading.Event`` is available through
    :py:func:`streamsx.ec.shutdown` which becomes set when a shutdown
    of the processing element has been requested. This event my be waited
    on to perform a sleep that will terminate upon shutdown.
    
    Args:
       docpy: Copy Python docstrings into SPL operator model for SPLDOC.

    Exceptions raised by ``__iter__`` and ``__next__`` can be suppressed
    when this decorator wraps a class with context manager
    ``__enter__`` and ``__exit__`` methods.

    If ``__exit__`` returns a true value when called with an exception 
    then the exception is suppressed.

    Suppressing an exception raised by ``__iter__`` results in the
    source producing an empty iteration. No tuples will be submitted.

    Suppressing an exception raised by ``__next__`` results in the
    source not producing any tuples for that invocation. Processing
    continues with a call to ``__next__``.

    Data conversion errors of the value returned by ``__next__`` can
    also be suppressed by ``__exit__``.
    If ``__exit__`` returns a true value when called with the exception 
    then the exception is suppressed and the value that caused the
    exception is not submitted as an SPL tuple.
    """
    def __init__(self, docpy=True):
        self.style = None
        self.docpy = docpy
    
    def __call__(self, wrapped):
        decorated = _wrapforsplop(_OperatorType.Source, wrapped, self.style, self.docpy)
        if inspect.isclass(decorated):
            decorated._splpy_decor = str(self)
        return decorated

    def __str__(self):
        s = ''
        if not self.docpy:
            if s:
                 s += ', '
            s += 'docpy=False'
        return '@spl.source(' + s + ')'

class map(object):
    """
    Decorator to create a map SPL operator from a callable class or function.

    Creates an SPL operator with a single input port and a single
    output port. For each tuple on the input port the
    callable is called passing the contents of the tuple.

    The value returned from the callable results in
    zero or more tuples being submitted to the operator output
    port, see :ref:`submit-from-python`.

    Example definition::

        @spl.map()
        class AddSeq(object):
        \"\"\"Add a sequence number as the last attribute.\"\"\"
        def __init__(self):
            self.seq = 0

        def __call__(self, *tuple_):
            id = self.seq
            self.seq += 1
            return tuple_ + (id,)

    Example SPL invocation::

        stream<In, tuple<uint64 seq>> InWithSeq = AddSeq(In) { }

    Args:
       style: How the SPL tuple is passed into Python callable or function, see  :ref:`spl-tuple-to-python`.
       docpy: Copy Python docstrings into SPL operator model for SPLDOC.

    Exceptions raised by ``__call__`` can be suppressed when this decorator
    wraps a class with context manager ``__enter__`` and ``__exit__`` methods.
    If ``__exit__`` returns a true value when called with the exception 
    then the exception is suppressed and the tuple that caused the
    exception is dropped.

    Data conversion errors of the value returned by ``__call__`` can
    also be suppressed by ``__exit__``.
    If ``__exit__`` returns a true value when called with the exception 
    then the exception is suppressed and the value that caused the
    exception is not submitted as an SPL tuple.
    """
    def __init__(self, style=None, docpy=True):
        self.style = style
        self.docpy = docpy
    
    def __call__(self, wrapped):
        decorated =  _wrapforsplop(_OperatorType.Pipe, wrapped, self.style, self.docpy)
        if inspect.isclass(decorated):
            decorated._splpy_decor = str(self)
        return decorated

    def __str__(self):
        s = ''
        if self.style is not None:
            s += 'style=' + str(self.style)
        if not self.docpy:
            if s:
                 s += ', '
            s += 'docpy=False'
        return '@spl.map(' + s + ')'

class filter(object):
    """
    Decorator that creates a filter SPL operator from a callable class or function.

    A filter SPL operator has a single input port and one mandatory
    and one optional output port. The schema of each output port
    must match the input port. For each tuple on the input port the
    callable is called passing the contents of the tuple. if the
    function returns a value that evaluates to True then it is
    submitted to mandatory output port 0. Otherwise it it submitted to
    the second optional output port (1) or discarded if the port is
    not specified in the SPL invocation.

    Args:
       style: How the SPL tuple is passed into Python callable or function, see  :ref:`spl-tuple-to-python`.
       docpy: Copy Python docstrings into SPL operator model for SPLDOC.

    Example definition::

        @spl.filter()
        class AttribThreshold(object):
            \"\"\"
            Filter based upon a single attribute being
            above a threshold.
            \"\"\"
            def __init__(self, attr, threshold):
                self.attr = attr
                self.threshold = threshold
                
            def __call__(self, **tuple_):
                return tuple_[self.attr] > self.threshold:

    Example SPL invocation::

        stream<rstring id, float64 voltage> Sensors = ...
        stream<Sensors> InterestingSensors = AttribThreshold(Sensors) {
            param 
              attr: "voltage";
              threshold: 225.0;
        }

    Exceptions raised by ``__call__`` can be suppressed when this decorator
    wraps a class with context manager ``__enter__`` and ``__exit__`` methods.
    If ``__exit__`` returns a true value when called with the exception 
    then the expression is suppressed and the tuple that caused the
    exception is dropped.
    """
    def __init__(self, style=None, docpy=True):
        self.style = style
        self.docpy = docpy
    
    def __call__(self, wrapped):
        decorated =  _wrapforsplop(_OperatorType.Filter, wrapped, self.style, self.docpy)
        if inspect.isclass(decorated):
            decorated._splpy_decor = str(self)
        return decorated

    def __str__(self):
        s = ''
        if self.style is not None:
            s += 'style=' + str(self.style)
        if not self.docpy:
            if s:
                 s += ', '
            s += 'docpy=False'
        return '@spl.filter(' + s + ')'

def ignore(wrapped):
    """
     Decorator to ignore a Python function.

     If a Python callable is decorated with ``@spl.ignore``
     then function is ignored by ``spl-python-extract.py``.

     Args:
         wrapped: Function that will be ignored.
    """
    @functools.wraps(wrapped)
    def _ignore(*args, **kwargs):
        return wrapped(*args, **kwargs)
    _ignore._splpy_optype = _OperatorType.Ignore
    _ignore._splpy_file = inspect.getsourcefile(wrapped)
    return _ignore


# Defines a function as a sink operator
class for_each(object):
    """
    Creates an SPL operator with a single input port.

    A SPL operator with a single input port and no output ports.
    For each tuple on the input port the decorated callable
    is called passing the contents of the tuple.

    Example definition::

        @spl.for_each()
        def PrintTuple(*tuple_):
        \"\"\"Print each tuple to standard out.\"\"\"
            print(tuple_, flush=True)

    Example SPL invocation::

        () as PT = PrintTuple(SensorReadings) { }
        
    Example definition with handling window punctuations::
    
        @spl.for_each(style='position')
        class PrintPunct(object):
            def __init__(self): 
                pass

            def __call__(self, value):
                assert value > 0
        
            def on_punct(self):
                print('window marker received')

    .. note::
        Punctuation marks are in-band signals that are inserted between tuples in a stream. Window punctuations are inserted into a stream that are related to the semantics of the operator. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.

    Args:
       style: How the SPL tuple is passed into Python callable, see  :ref:`spl-tuple-to-python`.
       docpy: Copy Python docstrings into SPL operator model for SPLDOC.

    Exceptions raised by ``__call__`` can be suppressed when this decorator
    wraps a class with context manager ``__enter__`` and ``__exit__`` methods.
    If ``__exit__`` returns a true value when called with the exception 
    then the expression is suppressed and the tuple that caused the
    exception is ignored.
    
    Supports handling window punctuation markers in the Sink operator in ``on_punct`` method (new in version 1.16).
    
    """
    def __init__(self, style=None, docpy=True):
        self.style = style
        self.docpy = docpy

    def __call__(self, wrapped):
        decorated = _wrapforsplop(_OperatorType.Sink, wrapped, self.style, self.docpy)
        if inspect.isclass(decorated):
            decorated._splpy_decor = str(self)
        return decorated

    def __str__(self):
        s = ''
        if self.style is not None:
            s += 'style=' + str(self.style)
        if not self.docpy:
            if s:
                 s += ', '
            s += 'docpy=False'
        return '@spl.for_each(' + s + ')'

class PrimitiveOperator(object):
    """Primitive operator super class.
    Classes decorated with `@spl.primitive_operator` must extend
    this class if they have one or more output ports. This class
    provides the `submit` method to submit tuples to specified
    otuput port.

    .. versionadded:: 1.8
    """
    def submit(self, port_id, tuple_):
        """Submit a tuple to the output port.

        The value to be submitted (``tuple_``) can be a ``None`` (nothing will be submitted),
        ``tuple``, ``dict` or ``list`` of those types. For details
        on how the ``tuple_`` is mapped to an SPL tuple see :ref:`submit-from-python`.

        Args:
             port_id: Identifier of the port specified in the
                  ``output_ports`` parameter of the ``@spl.primitive_operator``
                  decorator.
             tuple_: Tuple (or tuples) to be submitted to the output port.
        """
        port_index = self._splpy_output_ports[port_id]
        ec._submit(self, port_index, tuple_)

    def submit_punct(self, port_id):
        """Submit a window punctuation marker to the output port.
        
        .. note::
            Punctuation marks are in-band signals that are inserted between tuples in a stream. Window punctuations are inserted into a stream that are related to the semantics of the operator. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.

        Args:
             port_id: Identifier of the port specified in the
                  ``output_ports`` parameter of the ``@spl.primitive_operator``
                  decorator.

        .. versionadded:: 1.16
        """
        port_index = self._splpy_output_ports[port_id]
        ec._submit_punct(self, port_index)

    def all_ports_ready(self):
        """Notifcation that the operator can submit tuples.

        Called when the primitive operator can submit tuples
        using :py:meth:`submit`. An operator must not submit
        tuples until this method is called or until a port
        processing method is called.

        Any implementation must not block. A typical use
        is to start threads that submit tuples.

        An implementation must return a value that allows
        the SPL runtime to determine when an operator completes.
        An operator completes, and finalizes its output ports
        when:

            * All input ports (if any) have been finalized.
            * All background processing is complete.

        The return from ``all_ports_ready`` defines when
        background processing, such as threads started by
        ``all_ports_ready``, is complete. The value is one of:

            * A value that evaluates to `False` - No background processing exists.
            * A value that evaluates to `True` - Background processing exists and never completes. E.g. a source operator that processes real time events.
            * A callable - Background processing is complete when the callable returns. The SPL runtime invokes the callable once (passing no arguments) when the method returns background processing is assumed to be complete.

        For example if an implementation starts a single thread then `Thread.join` is returned to complete the operator when the thread completes::

            def all_ports_ready(self):
                submitter = threading.Thread(target=self._find_and_submit_data)
                submitter.start()
                return submitter.join

            def _find_and_submit_data(self):
                ...

        Returns:
            Value indicating active background processing.
        

        This method implementation does nothing and returns ``None``.
        """
        return None


class input_port(object):
    """Declare an input port and its processor method.

    Instance methods within a class decorated by
    :py:class:`spl.primitive_operator <primitive_operator>` declare
    input ports by decorating methods with this decorator.

    Each tuple arriving on the input port will result in a call
    to the processor method passing the stream tuple converted to
    a Python representation depending on the style. The style is
    determined by the method signature or the `style` parameter,
    see  :ref:`spl-tuple-to-python`.

    The order of the methods within the class define
    the order of the ports, so the first port is
    the first method decorated with `input_port`.

    Args:
        style: How the SPL tuple is passed into the method, see  :ref:`spl-tuple-to-python`.

    .. versionadded:: 1.8
    """
    _count = 0
    def __init__(self, style=None):
        self._style = style

    def __call__(self, wrapped):
        wrapped._splpy_input_port_seq = input_port._count
        wrapped._splpy_input_port_config = self
        wrapped._splpy_style = self._style
        input_port._count += 1
        return wrapped


class primitive_operator(object):
    """Creates an SPL primitive operator with an arbitrary number of input ports and
    output ports.

    Input ports are declared by decorating an instance method
    with :py:meth:`input_port`. The method is the process method
    for the input port and is called for each tuple that arrives
    at the port. The order of the decorated process methods defines
    the order of the ports in the SPL operator, with the first
    process method being the first port at index zero.

    Output ports are declared by the ``output_ports`` parameter which
    is set to a ``list`` of port identifiers. The port identifiers are
    arbitrary but must be hashable. Port identifiers allow the ability
    to submit tuples "logically' rather than through a port index. Typically
    a port identifier will be a `str` or an `enum`. The size of the list
    defines the number of output ports with the first identifier in the list
    coresponding to the first output port of the operator at index zero.
    If the list is empty or not set then the operator has no output ports.

    Tuples are submitted to an output port using :py:meth:`~PrimitiveOperator.submit`.

    When an operator has output ports it must be a sub-class of
    :py:class:`PrimitiveOperator` which provides the
    :py:meth:`~PrimitiveOperator.submit` method and the ports
    ready notification mechanism :py:meth:`~PrimitiveOperator.all_ports_ready`.

    Example definition of an operator with a single input port and two output ports::

        @spl.primitive_operator(output_ports=['MATCH', 'NEAR_MATCH'])
        class SelectCustomers(spl.PrimitiveOperator):
            \"\"\" Score customers using a model.
            Customers that are a good match are submitted to port 0 ('MATCH')
            while customers that are a near match are submitted to port 1 ('NEAR_MATCH').

            Customers that are not a good or near match are not submitted to any port.
            \"\"\"
            def __init__(self, match, near_match):
                self.match = match
                self.near_match = near_match

            @spl.input_port()
            def customers(self, **tuple_):
                 customer_score = self.score(tuple_)
                 if customer_score >= self.match:
                     self.submit('MATCH', tuple_)
                 elif customer_score >= self.near_match:
                     self.submit('NEAR_MATCH', tuple_)

            def score(self, **customer):
                # Actual model scoring omitted
                score = ...
                return score

    Example SPL invocation::

        (stream<Customers> MakeOffer; stream<Customers> ImproveOffer>) = SelectCustomers(Customers) {
            param
                match: 0.9;
                near_match: 0.8;
        }

    Example definition of an operator with punctuation handling::
        
        @spl.primitive_operator(output_ports=['A'])
        class SimpleForwarder(spl.PrimitiveOperator):
        def __init__(self):
            pass

        @spl.input_port()
        def port0(self, *t):
            self.submit('A', t)

        def on_punct(self):
            self.submit_punct('A')
            
    Supports handling window punctuation markers in the primitive operator in ``on_punct`` method (new in version 1.16).
    
    .. note::
        Punctuation marks are in-band signals that are inserted between tuples in a stream. Window punctuations are inserted into a stream that are related to the semantics of the operator. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.

    Args:
       output_ports(list): List of identifiers for output ports.
       docpy: Copy Python docstrings into SPL operator model for SPLDOC.

    .. versionadded:: 1.8
    """

    def __init__(self, output_ports=None,docpy=True):
        self._docpy = docpy
        self._output_ports = output_ports

    def __call__(self, wrapped):
        if not inspect.isclass(wrapped):
            raise TypeError('A class is required:' + str(wrapped))

        _valid_identifier(wrapped.__name__)

        cls = _wrapforsplop(_OperatorType.Primitive, wrapped, None, self._docpy)

        inputs = dict()
        for fname, fn in inspect.getmembers(wrapped):
            if hasattr(fn, '_splpy_input_port_seq'):
                inputs[fn._splpy_input_port_seq] = fn

        cls._splpy_input_ports = []
        cls._splpy_style = []
        cls._splpy_fixed_count = []
        for seq in sorted(inputs.keys()):
            fn = inputs[seq]
            fn._splpy_input_port_id = len(cls._splpy_input_ports)
            fn._splpy_style = _define_style(wrapped, fn, fn._splpy_style)
            fn._splpy_fixed_count = _define_fixed(cls, fn)

            cls._splpy_input_ports.append(fn)
            cls._splpy_style.append(fn._splpy_style)
            cls._splpy_fixed_count.append(fn._splpy_fixed_count)

        cls._splpy_output_ports = dict()
        if self._output_ports:
            for i in range(len(self._output_ports)):
                cls._splpy_output_ports[self._output_ports[i]] = i

        cls._splpy_decor = str(self)
        return cls

    def __str__(self):
        s = ''
        if self._output_ports:
            s += 'output_ports=' + str(self._output_ports)
             
        if not self._docpy:
            if s:
                 s += ', '
            s += 'docpy=False'
        return '@spl.primitive(' + s + ')'
