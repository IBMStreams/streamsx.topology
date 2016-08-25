# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
            
class FilterLine:
    """
    Callable class that returns True if the tuple contains the search term,
    False otherwise
    Args:
        term (string): search term
    Returns:
        True if the tuple contains the search term, False otherwise
    """
    def __init__(self, term):
        self.term = term
    def __call__(self, tuple):
        return self.term in tuple

        
