# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
def int_strings_transform():
    """
    Returns an iterable of strings
    """
    return ["325", "457", "9325"]
    
def string_to_int(t):
    """
    Converts a string to integer
    Args:
        t (string): tuple
    Returns:
        int        
    """
    return int(t)

class AddNum:
    """
    A callable class that performs addition
    Args:
        increment: value to add
    """
    def __init__(self, increment):
        self.increment = increment  
    def __call__(self, tuple):
        """
        Performs addition on the input value
        Args:
            tuple: tuple
        Returns:
            result of addition
        """
        return tuple + self.increment
