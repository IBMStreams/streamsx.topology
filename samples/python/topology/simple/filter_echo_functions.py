# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

class SysArgv:
    """
    Callable class that saves the command line arguments
    Args:
        argv: list of command line arguments (sys.argv[1:])
    """
    def __init__(self, argv):
        self.argv = argv
    def __call__(self):
        return self.argv

def starts_with_d(tuple):
    """
    Returns True if the tuple starts with "d", otherwise False
    """
    return tuple.startswith("d")
