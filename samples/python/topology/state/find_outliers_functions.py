# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import random
import math

def readings():
    """
    A generator function that returns a random float
    with each iteration.
    """
    for count in range(100000):
        yield random.gauss(0.0, 1.0)
        
class IsOutlier:
    """
    A callable class that determines if a tuple is an outlier.
    An outlier is defined as more than (threshold * standard deviation)
    from the mean.
    
    Args:
        threshold (number): threshold
    """
    def __init__(self, threshold):
        self.threshold = threshold
        self.sum_x = 0
        self.sum_x_squared = 0
        self.count = 0
    def __call__(self, tuple):
        """
        Args:
            tuple
        Returns:
            True if the tuple is an outlier, False otherwise
        """
        self.count += 1
        self.sum_x += tuple
        self.sum_x_squared += math.pow(tuple, 2)
        
        # sample standard deviation requires at least two values
        if self.count < 2:
            return False
            
        mean = self.sum_x / self.count;
        stddev = math.sqrt((self.sum_x_squared - (self.sum_x * self.sum_x) / self.count) / (self.count - 1))
        return abs(tuple) > (abs(mean) + (self.threshold * stddev))
