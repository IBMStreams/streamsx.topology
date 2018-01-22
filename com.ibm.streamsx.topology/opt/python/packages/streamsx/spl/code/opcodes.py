# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import streamsx.spl.code.types as code_types
import dis

def _cannot_translate(*args):
    raise code_types.CannotTranslate()

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
            stack.append(ctx.read_attribute(name=attr_name))
            return
    _cannot_translate()

def _binary_op(ctx, stack, code, ins, op):
    tos = stack.pop()
    tos1 = stack.pop()
    tos, tos1 = code_types.arith_upcast(tos, tos1)
    expr = '(' + str(tos1) + ' ' + op + ' ' + str(tos) + ')'
    et = code_types.value_type(tos)
    val = code_types.CodeValue(et, expr)
    stack.append(val)

def _binary_add(ctx, stack, code, ins):
    _binary_op(ctx, stack, code, ins, '+')

def _binary_subtract(ctx, stack, code, ins):
    _binary_op(ctx, stack, code, ins, '-')

def _binary_multiply(ctx, stack, code, ins):
    _binary_op(ctx, stack, code, ins, '*')

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
                stack.append(ctx.read_attribute(name=tos))
                return
        if isinstance(tos, int) and tos >=0 and tos < len(ctx.in_attrs_pos):
             if ctx.in_schema.style is dict:
                # TODO: Actually an attribute error
                _cannot_translate()
             stack.append(code_types.ReadAttribute(tos1, ctx.in_attrs_pos[tos]))
             return

    _cannot_translate()

def _compare_op(ctx, stack, code, ins):
    rhs = stack.pop()
    lhs = stack.pop()

    lhs, rhs = code_types.arith_upcast(lhs, rhs)

    stack.append(code_types.CodeValue('boolean', code_types.binary(lhs, dis.cmp_op[ins.arg], rhs)))

def _build_tuple(ctx, stack, code, ins):
    items = code_types.CodeTuple(tuple(stack[len(stack)-ins.arg:]))
    for _ in range(ins.arg):
        stack.pop()
    stack.append(items)

def _return_value(ctx, stack, code, ins):
    ctx._seen_return = True
    ctx._return = stack.pop()
    if stack:
        raise code_types.CannotTranslate()


def _cond_jump(ctx, stack, code, ins):
    if ins.arg < ins.offset:
        raise code_types.CannotTranslate()
    tos =  stack.pop()
    if ins.arg in ctx.jumps:
        items = ctx.jumps[ins.arg]
    else:
        items = []
        ctx.jumps[ins.arg] = items
    items.append((code_types.as_boolean(tos), ins))


OPCODE_ACTION_TUPLE = dict()

OA = OPCODE_ACTION_TUPLE

OA['LOAD_FAST'] = _load_fast
OA['LOAD_CONST'] = _load_const
OA['LOAD_ATTR'] = _load_attr

OA['BINARY_ADD'] = _binary_add
OA['BINARY_SUBTRACT'] = _binary_subtract

OA['BINARY_MULTIPLY'] = _binary_multiply

OA['BINARY_SUBSCR'] = _binary_subscr

OA['COMPARE_OP'] = _compare_op

OA['BUILD_TUPLE'] = _build_tuple

# OA['POP_JUMP_IF_FALSE'] = _cond_jump
# OA['POP_JUMP_IF_TRUE'] = _cond_jump
OA['JUMP_IF_TRUE_OR_POP'] = _cond_jump
OA['JUMP_IF_FALSE_OR_POP'] = _cond_jump


OA['RETURN_VALUE'] = _return_value


