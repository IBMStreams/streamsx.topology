# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import re
import logging

logging.basicConfig(level = logging.INFO, format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
                
class FilterLine:
    """
    Callable class that returns True if the tuple matches the pattern,
    False otherwise
    Args:
        term (string): search term
    Returns:
        True if the tuple matches the pattern, False otherwise
    """
    def __init__(self, pattern):
        self.pattern = pattern
        self.count = 0
    def __call__(self, tuple):
        self.count += 1
        logger.info("FilterLine@{0} has received {1} lines on this parallel channel.".format(id(self), self.count))
        return re.search(self.pattern,  tuple)

class LineCounter:
    """
    Callable class that counts the number of tuples seen
    """
    def __init__(self):
        self.count = 0
    def __call__(self, tuple):
        self.count += 1
        logger.info("LineCounter@{0} has sent {1} lines to be filtered.".format(id(self), self.count))
        return tuple


        
