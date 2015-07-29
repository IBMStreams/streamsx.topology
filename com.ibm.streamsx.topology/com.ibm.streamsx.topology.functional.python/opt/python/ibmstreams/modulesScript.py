import os
import sys
import inspect

# Add lib, opt/python/packages and opt/python/modules for this toolkit
# to the current Python path.

# This module is imported and executed at runtime by the initialization
# of a Python operator

def addToolkitModulesToPythonPath(toolkitDir):
    pyl = os.path.join(toolkitDir, 'opt', 'python', 'streams')
    if pyl not in sys.path:
        sys.path.append(pyl)
    pyp = os.path.join(toolkitDir, 'opt', 'python', 'packages')
    if pyp not in sys.path:
        sys.path.append(pyp)
    pym = os.path.join(toolkitDir, 'opt', 'python', 'modules')
    if pym not in sys.path:
        sys.path.append(pym)

    
    
    
