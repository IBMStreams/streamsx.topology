import math

__author__ = 'wcmarsha'

class ComplexNumber(object):
    """This is an example of a user-provided class that might be passed as a tuple through a stream"""
    def __init__(self, real = 0.0, complex = 0.0):
        self.real = real
        self.complex = complex

    def complexConjugate(self):
        """Returns the magnitude of the vector on the complex plane"""
        return math.sqrt(math.pow(self.real, 2) + math.pow(self.complex, 2))
