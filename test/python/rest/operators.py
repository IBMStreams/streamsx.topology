# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import time

class DelayedTupleSourceWithLastTuple:
    """
    The delay is needed, since it can take longer than ten seconds for the REST API to find a particular view, by
    which time the tuple has already passed. In addition, the source will wait two seconds before sending a last tuple.
    This is a workaround for the current REST API for getting data from views.
    """
    def __init__(self, tuples, delay):
        # Ensure there is at least one tuple in the list
        if len(tuples) < 1:
            raise ValueError("Must supply a list of at least one tuple to DelayedTupleSourceWithLastTuple constructor.")
        self.tuples = tuples
        self.delay = delay

    def __call__(self):
        time.sleep(self.delay)
        for tup in self.tuples:
            yield tup

        # send a last tuple as part of the view REST API workaround
        # TODO: remove when fix is delivered.
        time.sleep(2)
        yield self.tuples[0]
