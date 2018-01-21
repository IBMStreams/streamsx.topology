# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import streamsx.spl.code.types
import dis

def _cannot_translate(*args):
    raise streamsx.spl.code.types.CannotTranslate()

def _load_fast(ctx, stack, code, ins):
    if ins.arg == 0:
        stack.append(ctx._tuple)
        return
    stack.append(code.co_varnames[ins.arg])

def _load_const(ctx, stack, code, ins):
    stack.append(code.co_consts[ins.arg])

def _load_attr(ctx, stack, code, ins):
    tos = stack.pop()
    attr_name = code.co_names[ins.arg]

    if tos is ctx._tuple:
        # TODO - assert is a named tuple
        if attr_name in ctx.in_attrs_name:
            stack.append(streamsx.spl.code.types.ReadAttribute(tos, ctx.in_attrs_name[attr_name]))
            return
    _cannot_translate()

def _binary_op(ctx, stack, code, ins, op):
    tos = stack.pop()
    tos1 = stack.pop()
    tos, tos1 = streamsx.spl.code.types.binary_upcast(tos, tos1)
    expr = '(' + str(tos1) + ' ' + op + ' ' + str(tos) + ')'
    et = streamsx.spl.code.types.value_type(tos)
    val = streamsx.spl.code.types.CodeValue(et, expr)
    stack.append(val)

def _binary_add(ctx, stack, code, ins):
    _binary_op(ctx, stack, code, ins, '+')

def _binary_subtract(ctx, stack, code, ins):
    _binary_op(ctx, stack, code, ins, '-')

def _binary_subscr(ctx, stack, code, ins):
    """ supports tuple_[int|str]"""
    tos = stack.pop()
    tos1 = stack.pop()
    if tos1 is ctx._tuple:
        if isinstance(tos, str):
            if not ctx.in_schema.style is dict:
                # TODO: Actually an attribute error
                _cannot_translate()
            if tos in ctx.in_attrs_name:
                stack.append(streamsx.spl.code.types.ReadAttribute(tos1, ctx.in_attrs_name[tos]))
                return
        if isinstance(tos, int) and tos >=0 and tos < len(ctx.in_attrs_pos):
             if ctx.in_schema.style is dict:
                # TODO: Actually an attribute error
                _cannot_translate()
             stack.append(streamsx.spl.code.types.ReadAttribute(tos1, ctx.in_attrs_pos[tos]))
             return

    _cannot_translate()

def _compare_op(ctx, stack, code, ins):
    rhs = stack.pop()
    lhs = stack.pop()

    lt = streamsx.spl.code.types.value_type(lhs)
    rt = streamsx.spl.code.types.value_type(rhs)
    if lt == rt:
        pass
    elif lt in streamsx.spl.code.types.CT_INTS:
        if rt is int:
            rhs = streamsx.spl.code.types.code_cast(rhs, lt)
        elif rt in streamsx.spl.code.types.CT_INTS:
            t = streamsx.spl.code.types.CT_INTS[max(streamsx.spl.code.types.CT_INTS.index(lt), streamsx.spl.code.types.CT_INTS.index(rt))]
            lhs = streamsx.spl.code.types.code_cast(lhs, t)
            rhs = streamsx.spl.code.types.code_cast(rhs, t)
        else:
            _cannot_translate()
    else:
        _cannot_translate()
    
    cv = '(' + str(lhs) + ' ' + dis.cmp_op[ins.arg] + ' ' + str(rhs) + ') '
    stack.append(streamsx.spl.code.types.CodeValue(bool, cv))

def _build_tuple(ctx, stack, code, ins):
    items = streamsx.spl.code.types.CodeTuple(tuple(stack[len(stack)-ins.arg:]))
    for _ in range(ins.arg):
        stack.pop()
    stack.append(items)

def _return_value(ctx, stack, code, ins):
    ctx._seen_return = True
    ctx._return = stack.pop()
    if stack:
        raise streamsx.spl.code.types.CannotTranslate()


OPCODE_ACTION_TUPLE = dict()

OA = OPCODE_ACTION_TUPLE

OA['LOAD_FAST'] = _load_fast
OA['LOAD_CONST'] = _load_const
OA['LOAD_ATTR'] = _load_attr

OA['BINARY_ADD'] = _binary_add
OA['BINARY_SUBTRACT'] = _binary_subtract
OA['BINARY_SUBSCR'] = _binary_subscr

OA['COMPARE_OP'] = _compare_op

OA['BUILD_TUPLE'] = _build_tuple

OA['RETURN_VALUE'] = _return_value


