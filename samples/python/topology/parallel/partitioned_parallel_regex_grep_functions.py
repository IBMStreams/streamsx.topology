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
    def __call__(self, tuple):
        logger.info("FilterLine@{0} testing string \"{1}\" for the pattern.".format(id(self), tuple))
        return re.search(self.pattern,  tuple)


        
