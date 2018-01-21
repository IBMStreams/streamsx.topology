# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import copy
import dis
import streamsx.spl.code.opcodes
import streamsx.spl.code.types
import streamsx.topology.schema 
import streamsx.spl.op

    
class _SPLCtx(object):
    def __init__(self, code, in_schema, out_schema=None):
        self.code = code
        self._tuple = streamsx.spl.code.types.InTuple(0, code.co_varnames[0])
        self.in_schema = in_schema
        # Attributes by position and name
        attrs = streamsx.spl.code.types.attributes(in_schema)
        self.in_attrs_pos = attrs[0]
        self.in_attrs_name = attrs[1]

        if out_schema:
            self.out_schema = out_schema
            attrs = streamsx.spl.code.types.attributes(out_schema)
            self.out_attrs_pos = attrs[0]
            self.out_attrs_name = attrs[1]

        self._seen_return = False
        self._return = None

    def _calculate(self):
        stack = list()
        try:
            for ins in dis.get_instructions(self.code):
                if self._seen_return:
                    raise streamsx.spl.code.types.CannotTranslate()
                act = streamsx.spl.code.opcodes.OA.get(ins.opname)
                if act is not None:
                    act(self, stack, self.code, ins)
                else:
                    raise streamsx.spl.code.types.CannotTranslate()
        except streamsx.spl.code.types.CannotTranslate as e:
            print("!!!!!!!")
            import traceback
            traceback.print_exc()
            return False
        return True

    def _tuple_spl(ctx, schema, tuple_):
        st = '{'
        attr_names = ['a', 'b', 'c']
        for i in range(len(attr_names)):
             if i:
                 st += ', '
             st += attr_names[i]
             st += '=';
             st += tuple_[i]
        
        st += '}'
        return st

def _translatable_schema(schema):
    if streamsx.topology.schema.is_common(schema):
        return False
    if not hasattr(schema, '_types'):
        return False
    return True

def translate_filter(stream, fn, name):
    """Translate a Python filter to an SPL Filter if possible."""
    schema = stream.oport.schema
    if not _translatable_schema(schema):
        return None

    if hasattr(fn, '__code__'):
        ctx = _FilterCtx(fn.__code__, schema)
        if ctx.translate():
            return ctx.add_translated(stream, name)
    return None

class _FilterCtx(_SPLCtx):
    def __init__(self, code, in_schema):
        super(_FilterCtx, self).__init__(code, in_schema)

    def translate(self):
        return self._calculate()

    def add_translated(self, stream, name):
        if type(self._return) in streamsx.spl.code.types.CT_BUILTINS:
            self._return = bool(self._return)
        if isinstance(self._return, bool):
             self._return = 'true' if self._return else 'false'

        stream = stream.aliased_as(self._tuple.name)

        params = {'filter': streamsx.spl.op.Expression.expression(str(self._return))}
        _op = streamsx.spl.op.Map('spl.relational::Filter', stream, params=params, name=name)
        return _op.stream

def translate_map(stream, fn, out_schema, name):
    """Translate a Python map to SPL if possible."""
    if not _translatable_schema(out_schema):
        return None
    in_schema = stream.oport.schema
    if not _translatable_schema(in_schema):
        return None

    if hasattr(fn, '__code__'):
        ctx = _MapCtx(fn.__code__, in_schema, out_schema)
        if ctx.translate():
            return ctx.add_translated(stream, name)
    return None

class _MapCtx(_SPLCtx):
    def __init__(self, code, in_schema, out_schema):
        super(_MapCtx, self).__init__(code, in_schema, out_schema)


    def translate(self):
        if not self._calculate():
            return False

        if not isinstance(self._return, streamsx.spl.code.types.CodeTuple):
            return False

        if len(self._return.values) != len(self.out_attrs_pos):
            return False

        try:
            values = self._return.values
            assignments = []
            for i in range(len(self.out_attrs_pos)):
                attr = self.out_attrs_pos[i]
                assignments.append(streamsx.spl.code.types.unary_cast(values[i], attr.code_type))
            self.assignments = assignments
            return True
        except streamsx.spl.code.types.CannotTranslate:
            print("XXXXXXXXXXXXXXX")
            import traceback
            traceback.print_exc()
            return False

    def add_translated(self, stream, name):
        stream = stream.aliased_as(self._tuple.name)

        params = {}
        _op = streamsx.spl.op.Map('spl.relational::Functor', stream, schema=self.out_schema, params=params, name=name)
        for i in range(len(self.out_attrs_pos)):
            attr = self.out_attrs_pos[i]
            setattr(_op, attr.name, _op.output(str(self.assignments[i])))
        return _op.stream

