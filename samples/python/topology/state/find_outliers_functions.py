import random
import statistics

def readings():
    """
    A generator function that returns a random float
    with each iteration.
    """
    for count in range(100000):
        yield random.gauss(0.0, 1.0)
        
class IsOutlier:
    """
    A callable class that determines if a tuple is an outlier
    in a list of tuples.
    An outlier is defined as more than (threshold*standard deviation)
    from the mean.
    
    Args:
        threshold (number): threshold
    """
    def __init__(self, threshold):
        self.threshold = threshold
        self.tuples = []
    def __call__(self, tuple):
        """
        Args:
            tuple
        Returns:
            True if the tuple is an outlier, False otherwise
        """
        self.tuples.append(tuple)
        
        # sample standard deviation requires at least two values
        if len(self.tuples) < 2:
            return False
            
        mean = statistics.mean(self.tuples)
        stddev = statistics.stdev(self.tuples, mean)
        return abs(tuple) > (abs(mean) + (self.threshold * stddev))
