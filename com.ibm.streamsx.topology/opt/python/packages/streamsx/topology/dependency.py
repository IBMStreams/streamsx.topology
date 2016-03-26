import os.path
import sys
import site
import inspect
import types
import zipfile

class DependencyResolver(object):

    def __init__(self):
        self.modules = set()
        self.packages = set()
        self.processed_modules = set()
        
    # adds a module and its dependencies to the list of dependencies
    def add_dependencies(self, module):
        # add the module as a dependency
        self._add_dependency(module)
        # recursively get the module's imports and add those as dependencies
        imported_modules = get_imported_modules(module)
        #print ("get_imported_modules for {0}: {1}".format(module.__name__, imported_modules))
        for imported_module_name,imported_module in imported_modules.items():
            if imported_module not in self.processed_modules:
                self.add_dependencies(imported_module)
    
    # get the list of module dependencies
    def get_module_dependencies(self):
        return frozenset(self.modules)
    
    # gets the list of package dependencies
    def get_package_dependencies(self):
        return frozenset(self.packages)
    
    # adds a module to the list of dependencies
    def _add_dependency(self, module):
        package_name = get_package_name(module)
        if package_name:
            # module is part of a package
            # get the top-level package
            top_package_name = module.__name__.split('.')[0]
            top_package = sys.modules[top_package_name]
            # for regular packages, there is one top-level directory
            # for namespace packages, there can be more than one.
            # they will be merged in the bundle.  file name collisions are not allowed
            for top_package_path in list(top_package.__path__):
                top_package_path = os.path.abspath(top_package_path)
                if is_parentdir_zip(top_package_path):
                   # special handling if package is in a zip file
                   top_package_path = os.path.dirname(top_package_path)
                self._add_package(top_package_path)
        else:
            # individual Python module
            module_path = os.path.abspath(module.__file__)
            if is_parentdir_zip(module_path):
                # special handling if module is in a zip file
                self._add_package(os.path.dirname(module_path))
            else:
                # module is not in a zip file
                self._add_module(module_path)
            
        self.processed_modules.add(module)

    def _add_package(self, path):
        #print ("Adding external package", path)
        self.packages.add(path)
    
    def _add_module(self, path):
        #print ("Adding external module", path)
        self.modules.add(path)

#####################
# Utility functions #
#####################
    
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
            if not is_builtin_module(val) and \
               not is_streamsx_module(val) and \
               not is_system_module(val):
                imported_modules[val.__name__] = val
    return imported_modules

def is_builtin_module(module):
    return module.__name__ in sys.builtin_module_names and \
           not hasattr(module, '__file__') and \
           not hasattr(module, '__path__')

def _is_streamsx_module(module_path):
    return "com.ibm.streamsx.topology" in module_path

def is_streamsx_module(module):
    if hasattr(module, '__file__'):
        # module or regular package
        return _is_streamsx_module(module.__file__)
    elif hasattr(module, '__path__'):
        # namespace package. assume streamsx package if any path evaluates to true
        for module_path in list(module.__path__):
            if _is_streamsx_module(module_path):
                return True
    return False

# returns True if the given path starts with a path of a site package, False otherwise
def _inside_site_package(path):
    for site_package in site.getsitepackages():
        if path.startswith(site_package):
            return True
    return False

def _is_system_module(module_path):
    return not _inside_site_package(module_path) and \
           (module_path.startswith((sys.prefix, sys.exec_prefix)) or \
            (hasattr(sys, 'real_prefix') and module_path.startswith(sys.real_prefix)))
                     
def is_system_module(module):
    if hasattr(module, '__file__'):
        # module or regular package
        return _is_system_module(module.__file__)
    elif hasattr(module, '__path__'):
        # namespace package.  assume system package if any path evaluates to true
        for module_path in list(module.__path__):
            if _is_system_module(module_path):
                return True
    return False

# Returns True if the parent directory in path is a zip file; False otherwise
def is_parentdir_zip(path):
    return os.path.dirname(path) and zipfile.is_zipfile(os.path.dirname(path))
