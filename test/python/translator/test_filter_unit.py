import unittest

import streamsx.topology.schema as sts
import streamsx.spl.code.translator as translator
import streamsx.spl.code.types as code_types

def my_filter(v):
    return v.a != 9

class TestFilterTranslator(unittest.TestCase):

    def check_predicate(self, fn, schema, alias='val'):
        ctx = translator.FilterCtx(fn.__code__, schema)
        self.assertTrue(ctx.translate())
        self.assertTrue(isinstance(ctx._return, code_types.CodeValue))
        self.assertEqual('boolean', ctx._return.code_type)

        self.assertEqual(alias, ctx.alias())
        return ctx

    def check_expr(self, ctx, *args):
        pos = -1
        expr = ctx.filter_expression()

        for arg in args:
            idx = expr.find(arg)
            self.assertTrue(idx >= 0)
            self.assertTrue(idx > pos)
            pos = idx

    def test_filter_tuple(self):
        schema = sts.StreamSchema('tuple<int32 a, boolean b, int64 c>').as_tuple()

        ctx = self.check_predicate(lambda val : True, schema)
        self.assertEqual('true', ctx.filter_expression())

        ctx = self.check_predicate(lambda val : 8, schema)
        self.assertEqual('true', ctx.filter_expression())

        ctx = self.check_predicate(lambda val : 0.0, schema)
        self.assertEqual('false', ctx.filter_expression())

        ctx = self.check_predicate(lambda val : val[0] < 20, schema)
        self.check_expr(ctx, 'val.a', ' < ', '20')

        ctx = self.check_predicate(lambda val : val[0] <= 20, schema)
        self.check_expr(ctx, 'val.a', ' <= ', '20')

        ctx = self.check_predicate(lambda val : val[0] == 20, schema)
        self.check_expr(ctx, 'val.a', ' == ', '20')

        ctx = self.check_predicate(lambda val : val[0] != 20, schema)
        self.check_expr(ctx, 'val.a', ' != ', '20')

        ctx = self.check_predicate(lambda val : val[0] >= 20, schema)
        self.check_expr(ctx, 'val.a', ' >= ', '20')

        ctx = self.check_predicate(lambda val : val[0] > 20, schema)
        self.check_expr(ctx, 'val.a', ' > ', '20')

        ctx = self.check_predicate(lambda val : val[1], schema)
        self.assertEqual('val.b', ctx.filter_expression())

        ctx = self.check_predicate(lambda val : val[0], schema)
        self.check_expr(ctx, 'val.a', ' != ', '0')

        ctx = self.check_predicate(lambda v : v[2], schema, 'v')
        self.check_expr(ctx, 'v.c', ' != ', '0')
        
    def test_filter_dict(self):
        schema = sts.StreamSchema('tuple<int32 a, boolean b, int64 c>')

        ctx = self.check_predicate(lambda v : v['b'], schema, 'v')
        self.assertEqual('v.b', ctx.filter_expression())

        ctx = self.check_predicate(lambda v : v['c'], schema, 'v')
        self.check_expr(ctx, 'v.c', ' != ', '0')

    def test_filter_namedtuple(self):
        schema = sts.StreamSchema('tuple<int32 a, boolean b, int64 c>').as_tuple(named=True)

        ctx = self.check_predicate(lambda v : v[1], schema, 'v')
        self.assertEqual('v.b', ctx.filter_expression())
        ctx = self.check_predicate(lambda tuple_ : tuple_.b, schema, 'tuple_')
        self.assertEqual('tuple_.b', ctx.filter_expression())

        ctx = self.check_predicate(lambda v : v[0], schema, 'v')
        self.check_expr(ctx, 'v.a', ' != ', '0')
        ctx = self.check_predicate(lambda v : v.a, schema, 'v')
        self.check_expr(ctx, 'v.a', ' != ', '0')

        ctx = self.check_predicate(my_filter, schema, 'v')
        self.check_expr(ctx, 'v.a', ' != ', '9')

    def test_logic(self):
        schema = sts.StreamSchema('tuple<boolean a, boolean b, boolean c, boolean d>').as_tuple(named=True)

        ctx = self.check_predicate(lambda v : v.a and v.b, schema, 'v')
        self.assertEqual('(v.a && v.b)', ctx.filter_expression())

        ctx = self.check_predicate(lambda v : v.a or v.b, schema, 'v')
        self.assertEqual('(v.a || v.b)', ctx.filter_expression())

        ctx = self.check_predicate(lambda v : v.a or v.b or v.c, schema, 'v')
        self.assertEqual('(v.a || (v.b || v.c))', ctx.filter_expression())

        ctx = self.check_predicate(lambda v : v.a and v.b and v.c, schema, 'v')
        self.assertEqual('(v.a && (v.b && v.c))', ctx.filter_expression())

    def test_string(self):
        schema = sts.StreamSchema('tuple<rstring a, ustring b>').as_tuple(named=True)
        ctx = self.check_predicate(lambda s : s.a, schema, 's')
        self.assertEqual('(length(s.a) != 0)', ctx.filter_expression())
        ctx = self.check_predicate(lambda s : s.b, schema, 's')
        self.assertEqual('(length(s.b) != 0)', ctx.filter_expression())
