# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

# Import the SPL decorators
from streamsx.spl import spl

import random

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def spl_namespace():
    return "com.ibm.streamsx.topology.pysamples.primitives"

@spl.primitive_operator(output_ports=['MATCH', 'NEAR_MATCH'])
class SelectCustomers(spl.PrimitiveOperator):
    """Score customers using a model.
    Customers that are a good match are submitted to port 0 ('MATCH')
    while customers that are a near match are submitted to port 1 ('NEAR_MATCH').
            
    Customers that are not a good or near match are not submitted to any port. 
    """
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
        # Real scoring omitted
        score = random.random()
        return score
