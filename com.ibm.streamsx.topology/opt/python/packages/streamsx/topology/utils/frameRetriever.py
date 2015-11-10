__author__ = 'wcmarsha'

import sys

def functionId(obj, nFramesUp):
    """
    Create a string naming the function n frames up on the stack.
    """
    fr = sys._getframe(nFramesUp+1)
    co = fr.f_code
    return "%s.%s" % (obj.__class__, co.co_name)
