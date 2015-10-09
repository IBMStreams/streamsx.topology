import os
import sys
import inspect
import time

# Add lib, opt/python/packages and opt/python/modules for this toolkit
# to the current Python path. Assumption is that this script
# is copied into the operator directory for any operator
# that is mapped to a Python function.

# This is executed at runtime by the initialization
# of a Python operator

# This file is contained in
# toolkit_root/opt/.__splpy/common

commonDir = os.path.dirname(os.path.realpath(__file__))
splpyDir = os.path.dirname(commonDir)
optDir = os.path.dirname(splpyDir)
pythonDir = os.path.join(optDir, 'python')

addPath = os.path.join(splpyDir, 'packages')
if addPath not in sys.path:
    sys.path.append(addPath)

addPath = os.path.join(pythonDir, 'streams')
if addPath not in sys.path:
    sys.path.append(addPath)

addPath = os.path.join(pythonDir, 'packages')
if addPath not in sys.path:
    sys.path.append(addPath)

addPath = os.path.join(pythonDir, 'modules')
if addPath not in sys.path:
    sys.path.append(addPath)
