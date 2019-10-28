# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import collections
import inspect
import typing
import sys
import streamsx.topology.schema

_SchemaHints = collections.namedtuple('_SchemaHints', ['type_', 'schema'])

def schema_iterable(fn, topo):
    try:
        return _schema_iterable(fn, topo)
    except TypeError:
        raise
    except:
        return

def _schema_iterable(fn, topo):
    if inspect.isroutine(fn):
        _check_arg_count(fn, 0)
    elif callable(fn):
        fn = fn.__call__
        _check_arg_count(fn, 0)
    elif hasattr(fn, '__iter__'):
        fn = fn.__iter__
    else:
        return

    if not topo.type_checking:
        return

    type_ = _fn_return_hint(fn)
    if type_ is not None:
        if hasattr(type_, '__origin__') and hasattr(type_, '__args__'):
            if len(type_.__args__) == 1:
                et = type_.__args__[0]
                if typing.Iterable[et] == type_:
                    return _schema_from_type(et)
                if typing.Iterator[et] == type_:
                    return _schema_from_type(et)

def _fn_return_hint(fn):
    hints = typing.get_type_hints(fn)
    if hints and 'return' in hints:
        return hints['return']
        
# Check the callable for a filter
#
# - Can be passed a single argument
# - Has an argument type that is compatible the the schema type.
# - TODO? = Check the return is convertable to a truth value

def check_filter(fn, stream):
    try:
        _check_filter(fn, stream)
    except TypeError:
        raise
    except:
        import traceback
        traceback.print_exc()
        pass

def _check_filter(fn, stream):
    _check_arg_matching_schema(fn, stream)

# Check the callable for a split
#
# - Can be passed a single argument
# - Has an argument type that is compatible the the schema type.
# - TODO? = Check the return is convertable to a int value

def check_split(fn, stream):
    try:
        _check_split(fn, stream)
    except TypeError:
        raise
    except:
        import traceback
        traceback.print_exc()
        pass

def _check_split(fn, stream):
    _check_arg_matching_schema(fn, stream)

# Check the callable for for_each
#
# - Can be passed a single argument
# - Has an argument type that is compatible the the schema type.
# - TODO? = Check the return is nothing?

def check_for_each(fn, stream):
    try:
        _check_for_each(fn, stream)
    except TypeError:
        raise
    except:
        import traceback
        traceback.print_exc()
        pass

def _check_for_each(fn, stream):
    _check_arg_matching_schema(fn, stream)

# Check the callable for map
#
# - Can be passed a single argument
# - Has an argument type that is compatible the the schema type.
# - Infer output schema from type hint

def check_map(fn, stream):
    try:
        return _check_map(fn, stream)
    except TypeError:
        raise
    except:
        import traceback
        traceback.print_exc()
        pass

def _check_map(fn, stream):
    rt_hint = _check_arg_matching_schema(fn, stream)
    if rt_hint:
        return _schema_from_type(rt_hint)

# Check the callable for flat_map
#
# - Can be passed a single argument
# - Has an argument type that is compatible the the schema type.
# - TODO: Infer output schema from type hint

def check_flat_map(fn, stream):
    try:
        _check_flat_map(fn, stream)
    except TypeError:
        raise
    except:
        import traceback
        traceback.print_exc()
        pass

def _check_flat_map(fn, stream):
    _check_arg_matching_schema(fn, stream)

# Check the hint for paramter the tuple will be passed as
#  matches the schema.
#
# Returns the type hint of the function's return if
# it has one AND type_checking is true. Otherwiae return None
def _check_arg_matching_schema(fn, stream):

    if inspect.isroutine(fn):
        n = 1
    elif callable(fn):
        fn = fn.__call__
        n = 2 # includes self

    _check_arg_count(fn, 1)

    if stream.topology.type_checking:
        if stream._hints:
            hint, param = _get_arg_hint(fn, n-1)
            if hint:
                _check_matching(fn, stream._hints.type_, hint, param)

        return _fn_return_hint(fn)

def _check_arg_count(fn, n):

    sig = inspect.signature(fn)
    if len(sig.parameters) == n:
        return

    # Look for default values
    if len(sig.parameters) > n:
        i = 0
        ok = True
        for pn in sig.parameters:
            if i >= n:
                if sig.parameters[pn].default == inspect.Parameter.empty:
                    ok = False
                    break
            i += 1
        if ok:
            return

    raise TypeError('Callable {} takes {} arguments, {} expected' .format(fn, len(sig.parameters), n))


def _schema_from_type(type_):
    try:
        schema = streamsx.topology.schema._normalize(schema=type_, allow_none=False)
    except ValueError:
        schema = None
    return _SchemaHints(type_, schema)

def _get_arg_hint(fn, pos):
    hints = typing.get_type_hints(fn)
    if not hints:
        return None,None

    sig = inspect.signature(fn)
    i = 0
    for pn in sig.parameters:
        i += 1
        if i > pos:
            break
    return hints.get(pn), sig.parameters[pn]

def _check_matching(fn, source, target, param):

    if target == typing.Any or source == typing.Any:
        return

    if source == target:
        return

    try:
        if issubclass(source, target):
            return
    except TypeError:
        if typing.Optional[source] == target:
            return
        return

    raise TypeError('Callable {} expects {} for parameter {}, being passed {}' .format(fn.__name__, target.__name__, param.name, source.__name__))
