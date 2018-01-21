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
        self.code_type = code_type
        self.expr = expr

    def __str__(self):
        return str(self.expr)

class CodeTuple(object):
    def __init__(self, args):
        self.values = args

CT_BUILTINS={int,float,bool}

CT_INTS=['int8', 'int16', 'int32', 'int64']
CT_FLOATS=['float32', 'float64']
CT_BOOLEANS={'boolean', bool}

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

def binary_upcast(lhs, rhs):
    lt = value_type(lhs)
    rt = value_type(rhs)
    print("BUT:", lt, rt)
    if lt == rt:
        pass
    elif lt in CT_INTS:
        lhs,rhs = int_upcast(lhs, lt, rhs, rt)
    elif rt in CT_INTS:
        # yes args are swapped as the rhs is driving the type
        rhs,lhs = int_upcast(rhs, rt, lhs, lt)
    else:
        _cannot_translate()
    return (lhs, rhs)

def unary_cast(value, target_type):
    if same_type(value, target_type):
         return value
    print(type(value))
    vt = value_type(value)
    print("UNARY", vt, target_type)
    if vt in CT_BUILTINS:
        if target_type in CT_INTS:
            return code_cast(int(value), target_type)
    elif vt in CT_INTS:
        return code_cast(value, target_type)
    _cannot_translate()

def int_upcast(v, t, ov, ot):
    print("IUT:", t, ot)
    if ot is int:
        ov = code_cast(ov, t)
    elif ot in CT_INTS:
        print("IUT:", CT_INTS.index(t), CT_INTS.index(ot))
        t = CT_INTS[max(CT_INTS.index(t), CT_INTS.index(ot))]
        v = code_cast(v, t)
        ov = code_cast(ov, t)
    else:
        _cannot_translate()
    return (v, ov)

class CannotTranslate(Exception):
     def __init__(self):
         pass

def _cannot_translate(*args):
    raise CannotTranslate()

