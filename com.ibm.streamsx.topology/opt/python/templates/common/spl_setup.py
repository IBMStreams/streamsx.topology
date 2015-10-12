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

def __splpy_addDirToPath(dir):
    if os.path.isdir(dir):
        if dir not in sys.path:
            sys.path.append(dir)
        
commonDir = os.path.dirname(os.path.realpath(__file__))
splpyDir = os.path.dirname(commonDir)
optDir = os.path.dirname(splpyDir)
pythonDir = os.path.join(optDir, 'python')

__splpy_addDirToPath(os.path.join(splpyDir, 'packages'))
__splpy_addDirToPath(os.path.join(pythonDir, 'streams'))
__splpy_addDirToPath(os.path.join(pythonDir, 'packages'))
__splpy_addDirToPath(os.path.join(pythonDir, 'modules'))
