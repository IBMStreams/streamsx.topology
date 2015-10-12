import sys
import sysconfig
import inspect
import imp
import glob
import os
import shutil
import xml.etree.ElementTree as etree

def opmns():
    return 'http://www.ibm.com/xmlns/prod/streams/spl/operator'
def cmnns():
    return 'http://www.ibm.com/xmlns/prod/streams/spl/common'

# Return the root of the com.ibm.streamsx.topology toolkit
def topologyToolkitDir():
    _bin = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    return os.path.dirname(_bin)

def userToolkitDir():
    return os.getcwd()

# Directory containing Python packages in com.ibm.streamsx.topology toolkit
def pythonPackagesDir():
    return os.path.join(topologyToolkitDir(), 'opt', 'python', 'packages')  

# Append the packages directory for this toolkit into
# Python path to allow modules in the toolkit being
# extracted to reference the decorators etc.
def setup():
    sys.path.append(pythonPackagesDir())

setup()

from com.ibm.streamsx.topology.python.spl import OperatorType

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
def copyTemplate(ns, name, f, t):
     shutil.copy(topologyToolkitDir() + '/opt/templates/' + f, os.path.join(ns, name, t))

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
     shutil.copy(optemplate + '.xml', os.path.join(opdir, name + '.xml'))

     replaceTokenInFile(opcgt_cpp, "__SPLPY__RELATIVE_TK_DIR__SPLPY__", "../../..");
     #copyTemplate(ns, name, 'python-powered16.gif', 'python-powered16.gif')
     #copyTemplate(ns, name, 'python-powered32.gif', 'python-powered32.gif')
     #copyTemplate(ns, name, 'spl_python.pm', 'spl_python.pm')
     #copyTemplate(ns, name, 'spl_setup.py', 'spl_setup.py')
     #copyTemplate(ns, name, 'py_constructor.cgt', 'py_constructor.cgt')
     #copyTemplate(ns, name, 'py_functionReturnToTuples.cgt', 'py_functionReturnToTuples.cgt')
     #shutil.copytree(topologyToolkitDir() + '/opt/python/packages', os.path.join(opdir, 'packages'))
     

def modifyModel(ns, name, funcTuple):
    cgtbase = funcTuple.__splpy_optype.spl_template
    etree.register_namespace('opm', 'http://www.ibm.com/xmlns/prod/streams/spl/operator')
    etree.register_namespace('opm', opmns())
    etree.register_namespace('cmn', cmnns())
    tree = etree.parse(topologyToolkitDir() + '/opt/templates/' + cgtbase + '.xml')
    root = tree.getroot()
    context = tree.find('.//{' + opmns() + '}context')
    desc = context.find('{' + opmns() + '}description')
    desc.text = funcTuple.__doc__
    libname = context.find('.//{' + cmnns() + '}lib')
    libname.text = 'python' + sysconfig.get_config_var('LDVERSION')
    libpath = context.find('.//{' + cmnns() + '}libPath')
    libpath.text = sysconfig.get_config_var('LIBDIR')
    incpath = context.find('.//{' + cmnns() + '}includePath')
    incpath.text = sysconfig.get_path('include')
    tree.write(os.path.join(ns, name, name + '.xml'), encoding='UTF-8', xml_declaration=True)

def writeParameterInfo(cfgfile, funcTuple):
        sig = inspect.signature(funcTuple)
        fixedCount = 0
        params = sig.parameters
        for pname in params:
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
def writeConfig(dynm, opdir, name, fnname, funcTuple):
    cfgpath = os.path.join(opdir, 'splpy_operator.pm')
    cfgfile = open(cfgpath, 'w')
    cfgfile.write('sub splpy_Module { \''+ name   + "\'}\n")
    cfgfile.write('sub splpy_FunctionName {\'' + fnname + "\'}\n")
    cfgfile.write('sub splpy_OperatorType {\'' + funcTuple.__splpy_optype.name + "\'}\n")
    writeParameterInfo(cfgfile, funcTuple)
    cfgfile.write("1;\n")
    cfgfile.close()


def functionTupleOperator(dynm, name, fnname, funcTuple) :        
    ns = getattr(dynm, 'splNamespace')()   
    print(ns + "::" + fnname)
    if funcTuple.__doc__ != None:
        print("  ", funcTuple.__doc__)
    nsdir = makeNamespaceDir(ns)
    opdir = makeOperatorDir(nsdir, fnname)
    copyTemplateDir("common")
    copyTemplateDir("icons")
    copyPythonDir("packages")
    copyCGT(opdir, ns, fnname, funcTuple)
    #modifyModel_NEW(opdir, ns, fnname, funcTuple)
    writeConfig(dynm, opdir, name, fnname, funcTuple)

# Look at all the modules in opt/python/streams (opt/python/streams/*.py) and extract
# any function as an operator. Type will default to
# a Function operator with input and output.
# Functions may be decorated with spl decorators to
# change the operator type.

sys.path.append(os.path.join('opt', 'python', 'packages'))
sys.path.append(os.path.join('opt', 'python', 'modules'))
    
for mf in glob.glob(os.path.join('opt', 'python', 'streams', '*.py')):
    print('Checking ', mf, 'for operators')
    (name, suffix, mode, mtype)  = inspect.getmoduleinfo(mf)
    dynm = imp.load_source(name, mf)
    streamsPythonFile = inspect.getsourcefile(dynm)
    for fnname, funcOp in inspect.getmembers(dynm, inspect.isfunction):
        if inspect.isbuiltin(funcOp):
            continue
        if fnname.startswith('spl'):
            continue
        if not hasattr(funcOp, '__splpy_optype'):
            continue
        if funcOp.__splpy_optype == OperatorType.Ignore:
            continue
        if streamsPythonFile != funcOp.__splpy_file:
            continue

        functionTupleOperator(dynm, name, fnname, funcOp)
