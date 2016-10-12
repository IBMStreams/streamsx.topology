from __future__ import print_function
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import sys
import sysconfig
import inspect
if sys.version_info.major == 2:
  import funcsigs
import imp
import glob
import os
import shutil
import argparse
import subprocess

############################################
# Argument parsing
cmd_parser = argparse.ArgumentParser(description='Extract SPL operators from decorated Python classes and functions.')
cmd_parser.add_argument('-i', '--directory', required=True,
                   help='Toolkit directory')
cmd_parser.add_argument('--make-toolkit', action='store_true',
                   help='Index toolkit using spl-make-toolkit')
cmd_args = cmd_parser.parse_args()

############################################

############################################
# setup for function inspection
if sys.version_info.major == 3:
  _inspect = inspect
elif sys.version_info.major == 2:
  _inspect = funcsigs
else:
  raise ValueError("Python version not supported.")
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

# Create SPL operator parameters from the Python class
# (functions cannot have parameters)
# The parameters are taken from the signature of
# the __init__ method. In the spirit of Python
# the default for non-annotated function parameters
# is to map to operator parameters that take any type
# with a cardinality of 1. If the function parameter
# has a default value, then the operator parameter is optional

_OP_PARAM_TEMPLATE ="""
 <parameter>
  <name>__SPLPY__PARAM_NAME__SPLPY__</name>
  <description></description>
  <optional>__SPLPY__PARAM_OPT__SPLPY__</optional>
  <rewriteAllowed>true</rewriteAllowed>
  <expressionMode>AttributeFree</expressionMode>
  <type></type>
  <cardinality>1</cardinality>
 </parameter>"""

def create_op_parameters(opmodel_xml, name, opObj):
    opparam_xml = ''
    if opObj.__splpy_callable == 'class':
        pmds = init_sig = _inspect.signature(opObj.__init__).parameters
        itpmds = iter(pmds)
        # first argument to __init__ is self (instance ref)
        next(itpmds)
        
        for pn in itpmds:
            pmd = pmds[pn]
            px = _OP_PARAM_TEMPLATE
            px = px.replace('__SPLPY__PARAM_NAME__SPLPY__', pn)
            px = px.replace('__SPLPY__PARAM_OPT__SPLPY__', 'false' if pmd.default== _inspect.Parameter.empty else 'true' )
            opparam_xml = opparam_xml + px
    replaceTokenInFile(opmodel_xml, '__SPLPY__PARAMETERS__SPLPY__', opparam_xml)
    
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
     create_op_parameters(opmodel_xml, name, funcTuple)
     create_op_spldoc(opmodel_xml, name, funcTuple)
     create_ip_spldoc(opmodel_xml, name, funcTuple)

## Create SPL doc entries in the Operator model xml file.
##
import html
def create_op_spldoc(opmodel_xml, name, opobj):
     _opdoc = inspect.getdoc(opobj)
     if _opdoc is None:
         _opdoc = 'Callable: ' + name + "\n"

     _opdoc = html.escape(_opdoc)

     # Optionally include the Python source code
     if opobj.__splpy_docpy:
         try:
             _pysrc = inspect.getsource(opobj)
             _opdoc += "\n"
             _opdoc += "# Python\n";

             for _line in str.splitlines(_pysrc):
                 _opdoc += "    "
                 _opdoc += html.escape(_line)
                 _opdoc += "\n"
         except:
             pass
     
     replaceTokenInFile(opmodel_xml, "__SPLPY__DESCRIPTION__SPLPY__", _opdoc);

def create_ip_spldoc(opmodel_xml, name, opobj):
     if (opobj.__splpy_style == 'dictionary'):
       return """
       Tuple attribute values are passed by name to the Python callable using `\*\*kwargs`.
             """
     if (opobj.__splpy_style == 'tuple'):
       return """
       Tuple attribute values are passed by position to the Python callable.
             """
 
     replaceTokenInFile(opmodel_xml, "__SPLPY__INPORT_0_DESCRIPTION__SPLPY__", _p0doc);
   
# Write information about the Python function parameters.
#
def write_style_info(cfgfile, opobj):
        is_class = inspect.isclass(opobj)
        if is_class:
            opfn = opobj.__call__
        else:
            opfn = opobj
        
        sig = _inspect.signature(opfn)
        fixedCount = 0
        if opobj.__splpy_style == 'tuple':
            pmds = sig.parameters
            itpmds = iter(pmds)
            # Skip 'self' for classes
            if is_class:
                next(itpmds)
            
            for pn in itpmds:
                 param = pmds[pn]
                 if param.kind == _inspect.Parameter.POSITIONAL_OR_KEYWORD:
                     fixedCount += 1
                 if param.kind == _inspect.Parameter.VAR_POSITIONAL:
                     fixedCount = -1
                     break
                 if param.kind == _inspect.Parameter.VAR_KEYWORD:
                     break

        cfgfile.write('sub splpy_FixedParam { \''+ str(fixedCount)   + "\'}\n")
        cfgfile.write('sub splpy_ParamStyle { \''+ str(opobj.__splpy_style)   + "\'}\n")
 

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
    write_style_info(cfgfile, opobj)
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
    # Print the summary of the class/function
    _doc = inspect.getdoc(opobj)
    if _doc is not None:
        _doc = str.splitlines(_doc)[0]
        print("  ", _doc)
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

# Now make the toolkit if required
if cmd_args.make_toolkit:
    si = os.environ['STREAMS_INSTALL']
    mktk = os.path.join(si, 'bin', 'spl-make-toolkit')
    mktk_args = [mktk, '--directory', cmd_args.directory, '--make-operator']
    subprocess.check_call(mktk_args)
