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


class ReadAttribute(object):
    def __init__(self, tuple_, attr):
        self.tuple_ = tuple_
        self.attr = attr
        self.code_type = attr.code_type

    def __str__(self):
        return self.tuple_.name + '.' + self.attr.name

class CodeValue(object):
    def __init__(self, code_type, expr):
        self.code_type_ = code_type
        self.expr = expr

    def __str__(self):
        return str(self.expr)

CT_BUILTINS={int,float,bool}

CT_INTS={'int8', 'int16', 'int32', 'int64'}
CT_FLOATS={'float32', 'float64'}
CT_BOOLEANS={'boolean', bool}

def value_type(value):
     t = None
     if type(value) in CT_BUILTINS:
        t = type(value)
     elif hasattr(value, 'code_type'):
         t = value.code_type
     return t

def code_cast(value, code_type):
    if hasattr(value, 'code_type') and value.code_type == code_type:
         return value
    cv = '((' + code_type + ') ' + str(value) + ')'
    return CodeValue(code_type, cv)

def binary_upcast(lhs, rhs):
    lt = streamsx.spl.code.types.value_type(lhs)
    rt = streamsx.spl.code.types.value_type(rhs)
    if lt == rt:
        pass
    elif lt in streamsx.spl.code.types.CT_INTS:
        lhs,rhs = lhs_int_upcast(lhs, lt, rhs, rt)
    elif rt in streamsx.spl.code.types.CT_INTS:
        # yes args are swapped as the rhs is driving the type
        rhs,lhs = lhs_int_upcast(rhs, rt, lhs, lt)
    else:
        _cannot_translate()
    return (lhs, rhs)

def int_upcast(v, t, ov, ot):
    if ot is int:
        ov = streamsx.spl.code.types.code_cast(ov, t)
    elif ot in streamsx.spl.code.types.CT_INTS:
        t = streamsx.spl.code.types.CT_INTS[max(streamsx.spl.code.types.CT_INTS.index(t), streamsx.spl.code.types.CT_INTS.index(ot))]
        v = streamsx.spl.code.types.code_cast(v, t)
        ov = streamsx.spl.code.types.code_cast(ov, t)
    else:
        _cannot_translate()
    return (v, ov)

class CannotTranslate(Exception):
     def __init__(self):
         pass

def _cannot_translate(*args):
    raise CannotTranslate()

