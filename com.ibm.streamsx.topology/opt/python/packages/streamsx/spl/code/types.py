# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

class InTuple(object):
    def __init__(self, port, name):
        self.port = port
        self.name = name

class Attribute(object):
    def __init__(self, code_type, name):
        self.code_type = code_type
        self.name = name

def attributes(schema):
    attrs_by_pos = list(Attribute(a[0], a[1]) for a in schema._types)
    attrs_by_name = dict()
    for a in attrs_by_pos:
        attrs_by_name[a.name] = a
    return (attrs_by_pos, attrs_by_name)


class CodeValue(object):
    def __init__(self, code_type, expr):
        self.code_type = code_type
        self.expr = expr

    def __str__(self):
        return str(self.expr)


class ReadAttribute(CodeValue):
    def __init__(self, tuple_, attr):
        self.tuple_ = tuple_
        self.attr = attr
        super(ReadAttribute, self).__init__(attr.code_type, self.tuple_.name + '.' + self.attr.name)


class CodeTuple(object):
    def __init__(self, args):
        self.values = args

CT_BUILTINS={int,float,bool}

CT_INTS=['int8', 'int16', 'int32', 'int64']
CT_FLOATS=['float32', 'float64']

def value_type(value):
     t = None
     if type(value) in CT_BUILTINS:
        t = type(value)
     elif hasattr(value, 'code_type'):
         t = value.code_type
     return t

def same_type(value, target_type):
    return hasattr(value, 'code_type') and value.code_type == target_type

def code_cast(value, target_type):
    if same_type(value, target_type):
         return value
    cv = '((' + target_type + ') ' + str(value) + ')'
    return CodeValue(target_type, cv)

def is_boolean(cv):
    return cv.code_type  == 'boolean'

def is_int(cv):
    return cv.code_type in CT_INTS

def is_float(cv):
    return cv.code_type in CT_FLOATS


def binary(lhs, op, rhs):
    return '(' + str(lhs) + ' ' + op + ' ' + str(rhs) + ')'

_I64_ZERO = code_cast(0, 'int64')


def as_int64(value):
    if isinstance(value, int) or is_int(value):
        return code_cast(value, 'int64')
    _cannot_translate()

def as_float64(value):
    if isinstance(value, float) or is_float(value) or isinstance(value, int) or is_int(value):
        return code_cast(value, 'float64')
    _cannot_translate()

def is_signed(value):
    return is_int(value) or is_float(value)


def as_boolean(value):
    if isinstance(value, int) or isinstance(value, float):
        value = bool(value)

    if value is True:
        expr = 'true'
    elif value is False:
        expr = 'false'
    elif isinstance(value, CodeValue):
        if is_boolean(value):
            return value
        elif is_int(value):
            expr = binary(as_int64(value), '!=', _I64_ZERO)
    else:
        _cannot_translate()
    return CodeValue('boolean', expr)


def arith_upcast(lhs, rhs):
    if isinstance(lhs, int):
        return arith_upcast(as_int64(lhs), rhs)
    if isinstance(lhs, float):
        return arith_upcast(as_float64(lhs), rhs)
    if isinstance(rhs, int):
        return arith_upcast(lhs, as_int64(rhs))
    if isinstance(rhs, float):
        return arith_upcast(lhs, as_float64(rhs))

    if isinstance(lhs, CodeValue) and isinstance(rhs, CodeValue):

        if lhs.code_type == rhs.code_type:
            return lhs, rhs

        if is_signed(lhs) and is_signed(rhs):
            if is_float(lhs) or is_float(rhs):
                 return as_float64(lhs), as_float64(rhs)

            return as_int64(lhs), as_int64(rhs)

    _cannot_translate()

UNARY_CASTS = dict()
UNARY_CASTS['boolean'] = as_boolean
UNARY_CASTS['int64'] = as_int64

class CannotTranslate(Exception):
     def __init__(self):
         pass

def _cannot_translate(*args):
    raise CannotTranslate()

