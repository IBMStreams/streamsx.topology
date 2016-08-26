import sys
import sysconfig
import inspect
import imp
import glob
import os
import shutil
import argparse

############################################
# Argument parsing
cmd_parser = argparse.ArgumentParser(description='Extract SPL operators from Python functions.')
cmd_parser.add_argument('-i', '--directory', required=True,
                   help='Toolkit directory')
cmd_args = cmd_parser.parse_args()

############################################

# Return the root of the com.ibm.streamsx.topology toolkit
def topologyToolkitDir():
    _bin = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    return os.path.dirname(_bin)

# User toolkit (corresponding to -i directory)
def userToolkitDir():
    return cmd_args.directory 

# Directory containing Python packages in com.ibm.streamsx.topology toolkit
def pythonPackagesDir():
    return os.path.join(topologyToolkitDir(), 'opt', 'python', 'packages')  

# Append the packages directory for this toolkit into
# Python path to allow modules in the toolkit being
# extracted to reference the decorators etc.
def setup():
    sys.path.append(pythonPackagesDir())

setup()

from streamsx.spl.spl import OperatorType

def makeNamespaceDir(ns):
     nsdir = os.path.join(userToolkitDir(), ns)
     if os.path.isdir(nsdir):
         return nsdir
     os.mkdir(nsdir)
     return nsdir

def makeOperatorDir(nsdir, name):
    oppath = os.path.join(nsdir, name)
    if (os.path.isdir(oppath)):
        shutil.rmtree(oppath)
    os.mkdir(oppath)
    return oppath

def replaceTokenInFile(file, token, value):
    f = open(file,'r')
    contents = f.read()
    f.close()

    newcontents = contents.replace(token, value)

    f = open(file,'w')
    f.write(newcontents)
    f.close()


# Copy a single file from the templates directory to the newly created operator directory
def copyTemplateDir(dir):
    copyPythonDir(os.path.join("templates", dir))

def copyPythonDir(dir):
    cmn_src = os.path.join(topologyToolkitDir(), "opt", "python", dir);
    cmn_dst = os.path.join(userToolkitDir(), "opt", ".__splpy", os.path.basename(dir))
    if (os.path.isdir(cmn_dst)):
        shutil.rmtree(cmn_dst)
    shutil.copytree(cmn_src, cmn_dst)

def copyCGT(opdir, ns, name, funcTuple):
     cgtbase = funcTuple.__splpy_optype.spl_template
     optemplate = os.path.join(topologyToolkitDir(), "opt", "python", "templates","operators", cgtbase)
     opcgt_cpp = os.path.join(opdir, name + '_cpp.cgt')
     shutil.copy(optemplate + '_cpp.cgt', opcgt_cpp)
     shutil.copy(optemplate + '_h.cgt', os.path.join(opdir, name + '_h.cgt'))
     opmodel_xml = os.path.join(opdir, name + '.xml')
     shutil.copy(optemplate + '.xml', opmodel_xml)
     replaceTokenInFile(opmodel_xml, "__SPLPY__MAJOR_VERSION__SPLPY__", str(sys.version_info[0]));
     replaceTokenInFile(opmodel_xml, "__SPLPY__MINOR_VERSION__SPLPY__", str(sys.version_info[1]));
     _funcdoc_ = funcTuple.__doc__
     if _funcdoc_ is None:
         _funcdoc_ = 'Python function ' + funcTuple.__name__ + ' (module ' + funcTuple.__module__ + ')'
     replaceTokenInFile(opmodel_xml, "__SPLPY__DESCRIPTION__SPLPY__", _funcdoc_);

# Write information about the Python function parameters.
#
def writeParameterInfo(cfgfile, opobj):
        is_class = inspect.isclass(opobj)
        if is_class:
            opobj = opobj.__call__
        
        sig = inspect.signature(opobj)
        fixedCount = 0
        params = sig.parameters
        seenFirst = False
        for pname in params:
             if not seenFirst:
                 # Skip 'self' for classes
                 seenFirst = True
                 if is_class:
                     continue
             param = params[pname]
             if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                 fixedCount += 1
             if param.kind == inspect.Parameter.VAR_POSITIONAL:
                 fixedCount = -1
                 break

        cfgfile.write('sub splpy_FixedParam { \''+ str(fixedCount)   + "\'}\n")
 

# Write out the configuration for the operator
# as a set of Perl functions that return useful values
# for the code generator
def write_config(dynm, opdir, module, opname, opobj):
    cfgpath = os.path.join(opdir, 'splpy_operator.pm')
    cfgfile = open(cfgpath, 'w')
    cfgfile.write('sub splpy_Module { \''+ module   + "\'}\n")
    cfgfile.write('sub splpy_OperatorCallable {\'' + opobj.__splpy_callable + "\'}\n")
    cfgfile.write('sub splpy_FunctionName {\'' + opname + "\'}\n")
    cfgfile.write('sub splpy_OperatorType {\'' + opobj.__splpy_optype.name + "\'}\n")
    writeParameterInfo(cfgfile, opobj)
    cfgfile.write("1;\n")
    cfgfile.close()


#
# module - module for operator
# opname - name of the SPL operator
# opobj - decorated object defining operator
#
def common_tuple_operator(dynm, module, opname, opobj) :        
    ns = getattr(dynm, 'splNamespace')()   
    print(ns + "::" + opname)
    if opobj.__doc__ != None:
        print("  ", opobj.__doc__)
    nsdir = makeNamespaceDir(ns)
    opdir = makeOperatorDir(nsdir, opname)
    copyTemplateDir("common")
    copyTemplateDir("icons")
    copyPythonDir("packages")
    copyPythonDir("include")
    copyCGT(opdir, ns, opname, opobj)
    write_config(dynm, opdir, module, opname, opobj)

# Process python objects in a module looking for SPL operators
# dynm - introspection for the modeul
# module - module name
# ops - list of potential operators (functions)
def process_operators(dynm, module, ops):
    for opname, opobj in ops:
        if inspect.isbuiltin(opobj):
            continue
        if opname.startswith('spl'):
            continue
        if not hasattr(opobj, '__splpy_optype'):
            continue
        if opobj.__splpy_optype == OperatorType.Ignore:
            continue
        if streamsPythonFile != opobj.__splpy_file:
            continue
        common_tuple_operator(dynm, module, opname, opobj)

# Look at all the modules in opt/python/streams (opt/python/streams/*.py) and extract
# any spl decorated function as an operator.

tk_streams = os.path.join(userToolkitDir(), 'opt', 'python', 'streams')
if not os.path.isdir(tk_streams):
    sys.exit(0)

tk_packages = os.path.join(userToolkitDir(), 'opt', 'python', 'packages')
if os.path.isdir(tk_packages):
    sys.path.append(tk_packages)
tk_modules = os.path.join(userToolkitDir(), 'opt', 'python', 'modules')
if os.path.isdir(tk_modules):
    sys.path.append(tk_modules)

for mf in glob.glob(os.path.join(tk_streams, '*.py')):
    print('Checking ', mf, 'for operators')
    (name, suffix, mode, mtype)  = inspect.getmoduleinfo(mf)
    dynm = imp.load_source(name, mf)
    streamsPythonFile = inspect.getsourcefile(dynm)
    process_operators(dynm, name, inspect.getmembers(dynm, inspect.isfunction))
    process_operators(dynm, name, inspect.getmembers(dynm, inspect.isclass))
