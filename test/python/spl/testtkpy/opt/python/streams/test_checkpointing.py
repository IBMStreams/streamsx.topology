import time

# Import the SPL decorators
from streamsx.spl import spl
from streamsx.spl.types import Timestamp

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.checkpoint"

# Class defining a source of integers from 0 to the limit, including 0 but
# excluding the limit.  Between each tuple, it sleeps for a period.
# An instance of this class can be dilled.
@spl.source()
class TimeCounter(object):
    """Count up from zero every `period` seconds for a given number of 
    iterations."""
    def __init__(self, period=None, iterations=None):
        if period is None:
            period = 1.0

        self.period = period
        self.iterations = iterations
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        # If the number of iterations has been met, stop iterating.
        if self.iterations is not None and self.count >= self.iterations:
            raise StopIteration

        # Otherwise increment, sleep, and return.
        to_return = self.count
        self.count += 1
        time.sleep(self.period)
        return (to_return,)

    def next(self):
        return self.__next__()

# Pass only even integers.  This is a class, therefore is checkpointed
# even though it does not have meaningful state.
@spl.filter(style='position')
class StatefulEvenFilter(object):
    def __init__(self): pass
    def __call__(self, x):
        return x % 2 == 0

# Map x -> x / 2 + 1.  Again this is stateful although it does not have
# meaningful state.
@spl.map(style='position')
class StatefulHalfPlusOne(object):
    def __init__(self): pass
    def __call__(self, x):
        return (x / 2 + 1,)

# Receive tuples containing integers, and add an rstring attribute
# 'fizz', 'buzz', 'fizzbuzz', or '', depending on whether the integer
# is a multiple of 3, 5, both, or neither.
@spl.primitive_operator(output_ports=['FIZZBUZZ'])
class FizzBuzz(spl.PrimitiveOperator):
    # This must be present, or sc will fail because of missing required
    # parameters args and kwargs.
    def __init__(self): pass

    @spl.input_port(style='position')
    def fizz_buzz(self, value): 
        classification = ''
        if (value % 3 == 0):
            classification = classification + "fizz"
        if (value % 5 == 0):
            classification = classification + 'buzz'

        self.submit('FIZZBUZZ', (value, classification))

# Receive a streams of tuples <int32, rstring>.  The rstring value
# is the fizzbuzz classification of the int32.  This verifies
# the classification.
@spl.for_each(style='position')
class Verify(object):
    def __init__(self): pass
    # verify that all receieved tuples have been correctly fizzbuzzed
    def __call__(self, value, classification):
        if value % 3 == 0 and value % 5 == 0:
            assert classification == 'fizzbuzz'
        elif value % 3 == 0:
            assert classification == 'fizz'
        elif value % 5 == 0:
            assert classification == 'buzz'
        else:
            assert classification == ''
