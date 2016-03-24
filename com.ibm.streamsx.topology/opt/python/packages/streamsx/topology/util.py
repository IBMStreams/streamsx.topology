import os
import sys
import site
import inspect
import types

# Gets the package name given a module object
# Returns None or '' if the module does not belong to a package            
def get_package_name(module):
    try:
        # if __package__ is defined, use it
        package_name = module.__package__
    except AttributeError:
        package_name = None  
        
    if package_name is None:
        # if __path__ is defined, the package name is the module name
        package_name = module.__name__
        if not hasattr(module, '__path__'):
            # if __path__ is not defined, the package name is the
            # string before the last "." of the fully-qualified module name
            package_name = package_name.rpartition('.')[0]
           
    return package_name
    
# Gets the function's module name
# Resolves the __main__ module to an actual module name
def get_module_name(function):
    module_name = function.__module__
    if module_name == '__main__':
        # special handling to allow importing functions from __main__ module
        # get the main module object of the function
        main_module = inspect.getmodule(function)
        # get the module name from __file__ by getting the base name and removing the .py extension
        # e.g. test1.py => test1
        module_name = os.path.splitext(os.path.basename(main_module.__file__))[0]
    return module_name

# Gets imported modules for a given module
# The following modules are excluded: 
# * built-in modules
# * modules that have "com.ibm.streamsx.topology" in the path
# * other system modules whose paths start with sys.prefix or sys.exec_prefix 
#   that are not inside a site package
def get_imported_modules(module):
    imported_modules = {}
    #print ("vars(module)", vars(module))
    for alias, val in vars(module).items():
        if isinstance(val, types.ModuleType):
            if not _is_builtin_module(val) and \
               not _is_streamsx_module(val) and \
               not _is_system_module(val):
                imported_modules[val.__name__] = val
    return imported_modules

def _is_builtin_module(module):
    return module.__name__ in sys.builtin_module_names

def _is_streamsx_module(module):
    if hasattr(module, '__file__'):
        module_path = module.__file__
    elif hasattr(module, '__path__'):
        module_path = list(module.__path__)[0]
    else:
        return False
    return "com.ibm.streamsx.topology" in module_path

# returns True if the given path starts with a path of a site package, False otherwise
def _inside_site_package(path):
    for site_package in site.getsitepackages():
        if path.startswith(site_package):
            return True
    return False
          
def _is_system_module(module):
    if hasattr(module, '__file__'):
        module_path = module.__file__
    elif hasattr(module, '__path__'):
        module_path = list(module.__path__)[0]
    else:
        return False
    return not _inside_site_package(module_path) and \
           (module_path.startswith(sys.prefix) or module_path.startswith(sys.exec_prefix))
              