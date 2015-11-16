import sys
import os

# This file is in opt/python/packages/streamsx
# in the com.ibm.streamsx.topology toolkit
dir = os.path.dirname(__file__)
optpkgs = os.path.dirname(dir)

if optpkgs not in sys.path:
    sys.path.append(optpkgs)

