# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2018
from __future__ import print_function
from __future__ import unicode_literals
from future.builtins import *
import sys
import sysconfig
import inspect
if sys.version_info.major == 2:
  import funcsigs
import imp
import glob
import fcntl
import fnmatch
import os
import shutil
import argparse
import subprocess
import xml.etree.ElementTree as ET
import html
from streamsx.spl.spl import _OperatorType
from streamsx.spl.spl import _valid_op_parameter

############################################
# setup for function inspection
if sys.version_info.major == 3:
    _inspect = inspect
elif sys.version_info.major == 2:
    _inspect = funcsigs
    FileNotFoundError = IOError
else:
    raise ValueError("Python version not supported.")
############################################

# Return the root of the com.ibm.streamsx.topology toolkit
def _topology_tk_dir():
    dir = os.path.dirname(__file__) # streamsx/scripts
    dir = os.path.dirname(dir) # streamsx
 
    # See if we are being run from streamsx package
    pkg_tk = os.path.join(dir, '.toolkit', 'com.ibm.streamsx.topology')
    if os.path.isdir(pkg_tk):
        return pkg_tk

    # Run from the SPL toolkit
    for _ in range(4):
        dir = os.path.dirname(dir)
    return dir

def replaceTokenInFile(file, token, value):
    f = open(file,'r')
    contents = f.read()
    f.close()

    newcontents = contents.replace(token, value)

    f = open(file,'w')
    f.write(newcontents)
    f.close()

def _optype(opobj):
    if hasattr(opobj, '_splpy_optype'):
        return opobj._splpy_optype
    return None

def _opfile(opobj):
    return opobj._splpy_file

def _opcallable(opobj):
    return opobj._splpy_callable

def _opdoc(opobj):
    return opobj._splpy_docpy

_INFO_XML_TEMPLATE="""<?xml version="1.0" encoding="UTF-8"?>
<toolkitInfoModel
  xmlns="http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo"
  xmlns:common="http://www.ibm.com/xmlns/prod/streams/spl/common">
  <identity>
    <name>__SPLPY_TOOLKIT_NAME__</name>
    <description>Automatic generated toolkit description file.</description>
    <version>1.0.0</version>
    <requiredProductVersion>4.2</requiredProductVersion>
  </identity>
  <dependencies/>
  <resources>
    <messageSet name="TopologySplpyResource">
      <lang default="true">en_US/TopologySplpyResource.xlf</lang>
      <lang>de_DE/TopologySplpyResource.xlf</lang>
      <lang>es_ES/TopologySplpyResource.xlf</lang>
      <lang>fr_FR/TopologySplpyResource.xlf</lang>
      <lang>it_IT/TopologySplpyResource.xlf</lang>
      <lang>ja_JP/TopologySplpyResource.xlf</lang>
      <lang>ko_KR/TopologySplpyResource.xlf</lang>
      <lang>pt_BR/TopologySplpyResource.xlf</lang>
      <lang>ru_RU/TopologySplpyResource.xlf</lang>
      <lang>zh_CN/TopologySplpyResource.xlf</lang>
      <lang>zh_TW/TopologySplpyResource.xlf</lang>
    </messageSet>
  </resources>
</toolkitInfoModel>
"""


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

_OP_INPUT_PORT_SET_TEMPLATE ="""
<inputPortSet>
  <description>
                                    __SPLPY__INPORT_DESCRIPTION__SPLPY__
  </description>
  <tupleMutationAllowed>false</tupleMutationAllowed>
  <windowingMode>NonWindowed</windowingMode>
  <windowPunctuationInputMode>Oblivious</windowPunctuationInputMode>
  <cardinality>1</cardinality>
  <optional>false</optional>
</inputPortSet>
"""

_OP_OUTPUT_PORT_SET_TEMPLATE ="""
<outputPortSet>
  <description>
    __SPLPY__OUTPORT_DESCRIPTION__SPLPY__
  </description>
  <expressionMode>Nonexistent</expressionMode>
  <autoAssignment>false</autoAssignment>
  <completeAssignment>false</completeAssignment>
  <rewriteAllowed>true</rewriteAllowed>
  <windowPunctuationOutputMode>Free</windowPunctuationOutputMode>
  <tupleMutationAllowed>false</tupleMutationAllowed>
  <cardinality>1</cardinality>
  <optional>false</optional>
</outputPortSet>
"""





class _Extractor(object):
    def __init__(self, args):
        self._cmd_args = self._parse_cmd_args(args)
        self._tk_dir = self._cmd_args.directory

    def _parse_cmd_args(self, args):
        cmd_parser = argparse.ArgumentParser(description='Extract SPL operators from decorated Python classes and functions.')
        cmd_parser.add_argument('-i', '--directory', required=True,
                   help='Toolkit directory')
        cmd_parser.add_argument('--make-toolkit', action='store_true',
                   help='Index toolkit using spl-make-toolkit')
        cmd_parser.add_argument('-v', '--verbose', action='store_true',
                   help='Print more diagnostics')
        return cmd_parser.parse_args(args)

    def _make_namespace_dir(self, ns):
         nsdir = os.path.join(self._tk_dir, ns)
         if os.path.isdir(nsdir):
             return nsdir
         os.mkdir(nsdir)
         return nsdir

    def _make_operator_dir(self, nsdir, name):
         oppath = os.path.join(nsdir, name)
         if (os.path.isdir(oppath)):
             shutil.rmtree(oppath)
         os.mkdir(oppath)
         return oppath

    def _make_toolkit(self):
        # Now make the toolkit if required
        if self._cmd_args.make_toolkit:
            si = os.environ['STREAMS_INSTALL']
            mktk = os.path.join(si, 'bin', 'spl-make-toolkit')
            mktk_args = [mktk, '--directory', self._cmd_args.directory, '--make-operator']
            subprocess.check_call(mktk_args)

    # Process python objects in a module looking for SPL operators
    # dynm - introspection for the modeul
    # module - module name
    # ops - list of potential operators (functions)
    def _process_operators(self, dynm, module, streams_python_file, ops):
        for opname, opobj in ops:
            if inspect.isbuiltin(opobj):
                continue
            if opname.startswith('spl'):
                continue
            optype = _optype(opobj)
            if optype is None:
                continue
            if optype == _OperatorType.Ignore:
                continue
            if streams_python_file != _opfile(opobj):
                continue
            self._common_tuple_operator(dynm, module, opname, opobj)

    def _copy_globalization_resources(self):
        '''Copy the language resource files for python api functions
    
        This function copies the TopologySplpy Resource files from Topology toolkit directory
        into the impl/nl folder of the project.
        Returns: the list with the copied locale strings'''
        rootDir = os.path.join(_topology_tk_dir(), "impl", "nl")
        languageList = []
        for dirName in os.listdir(rootDir):
            srcDir = os.path.join(_topology_tk_dir(), "impl", "nl", dirName)
            if (os.path.isdir(srcDir)) and (dirName != "include"):
                dstDir = os.path.join(self._tk_dir, "impl", "nl", dirName)
                try:
                    print("Copy globalization resources " + dirName)
                    os.makedirs(dstDir)
                except OSError as e:
                    if (e.errno == 17) and (os.path.isdir(dstDir)):
                        if self._cmd_args.verbose:
                            print("Directory", dstDir, "exists")
                    else:
                        raise
                srcFile = os.path.join(srcDir, "TopologySplpyResource.xlf")
                if os.path.isfile(srcFile):
                    res = shutil.copy2(srcFile, dstDir)
                    languageList.append(dirName)
                    if self._cmd_args.verbose:
                        print("Written: " + res)
        return languageList

    #
    # module - module for operator
    # opname - name of the SPL operator
    # opobj - decorated object defining operator
    #
    def _common_tuple_operator(self, dynm, module, opname, opobj) :        
        if (not hasattr(dynm, 'spl_namespace')) and hasattr(dynm, 'splNamespace'):
            ns = getattr(dynm, 'splNamespace')()
        else:
            ns = getattr(dynm, 'spl_namespace')()
        print(ns + "::" + opname)
        # Print the summary of the class/function
        _doc = inspect.getdoc(opobj)
        if _doc is not None:
            _doc = _doc.splitlines()[0]
            print("  ", _doc)
        nsdir = self._make_namespace_dir(ns)
        opdir = self._make_operator_dir(nsdir, opname)
        self._copy_template_dir("common")
        self._copy_template_dir("icons")
        self._copy_python_dir("packages")
        self._copy_python_dir("include")
        self._copy_CGT(opdir, ns, opname, opobj)
        self._write_config(dynm, opdir, module, opname, opobj)

    def _create_op_parameters(self, opmodel_xml, name, opObj):
        opparam_xml = ''
        if _opcallable(opObj) == 'class':
            pmds = init_sig = _inspect.signature(opObj._splpy_wrapped.__init__).parameters
            itpmds = iter(pmds)
            # first argument to __init__ is self (instance ref)
            next(itpmds)
        
            for pn in itpmds:
                pmd = pmds[pn]
                _valid_op_parameter(pn)
                px = _OP_PARAM_TEMPLATE
                px = px.replace('__SPLPY__PARAM_NAME__SPLPY__', pn)
                px = px.replace('__SPLPY__PARAM_OPT__SPLPY__', 'false' if pmd.default== _inspect.Parameter.empty else 'true' )
                opparam_xml = opparam_xml + px
        replaceTokenInFile(opmodel_xml, '__SPLPY__PARAMETERS__SPLPY__', opparam_xml)
    
    def _copy_CGT(self, opdir, ns, name, funcTuple):
         cgtbase = _optype(funcTuple).spl_template
         optemplate = os.path.join(_topology_tk_dir(), "opt", "python", "templates","operators", cgtbase)
         opcgt_cpp = os.path.join(opdir, name + '_cpp.cgt')
         shutil.copy(optemplate + '_cpp.cgt', opcgt_cpp)
         shutil.copy(optemplate + '_h.cgt', os.path.join(opdir, name + '_h.cgt'))
         opmodel_xml = os.path.join(opdir, name + '.xml')
         shutil.copy(optemplate + '.xml', opmodel_xml)
         replaceTokenInFile(opmodel_xml, "__SPLPY__MAJOR_VERSION__SPLPY__", str(sys.version_info[0]));
         replaceTokenInFile(opmodel_xml, "__SPLPY__MINOR_VERSION__SPLPY__", str(sys.version_info[1]));
         self._create_op_parameters(opmodel_xml, name, funcTuple)
         self._create_op_spldoc(opmodel_xml, name, funcTuple)
         if cgtbase == 'PythonPrimitive':
             self._create_primitive(opmodel_xml, name, funcTuple)
         else:
             self._create_ip_spldoc(opmodel_xml, name, funcTuple)

    ## Create SPL doc entries in the Operator model xml file.
    ##
    def _create_op_spldoc(self, opmodel_xml, name, opobj):
         opdoc = inspect.getdoc(opobj)
         if opdoc is None:
             opdoc = 'Callable: ' + name + "\n"

         opdoc = html.escape(opdoc)

         # Optionally include the Python source code
         if _opdoc(opobj):
             try:
                 _pysrc = inspect.getsource(opobj)
                 opdoc += "\n"
                 opdoc += "# Python\n";

                 for _line in str.splitlines(_pysrc):
                     opdoc += "    "
                     opdoc += html.escape(_line)
                     opdoc += "\n"
             except:
                 pass
         
         replaceTokenInFile(opmodel_xml, "__SPLPY__DESCRIPTION__SPLPY__", opdoc);

    def _create_ip_spldoc(self, opmodel_xml, name, opobj):
         if opobj._splpy_style == 'dictionary':
           _p0doc = """
           Tuple attribute values are passed by name to the Python callable using `\*\*kwargs`.
                 """
         elif opobj._splpy_style == 'tuple':
           _p0doc = """
           Tuple attribute values are passed by position to the Python callable.
                 """
         else:
           _p0doc = ''
     
         replaceTokenInFile(opmodel_xml, "__SPLPY__INPORT_0_DESCRIPTION__SPLPY__", _p0doc);

    def _create_primitive(self, opmodel_xml, name, cls):
        # input ports
        if cls._splpy_input_ports:
            ipd = ''
            for portfn in cls._splpy_input_ports:
                tip = _OP_INPUT_PORT_SET_TEMPLATE
                fndoc = inspect.getdoc(portfn)
                if not fndoc:
                    fndoc = portfn.__name__
                tip = tip.replace('__SPLPY__INPORT_DESCRIPTION__SPLPY__', fndoc)
                ipd += tip
        else:
            ipd = "<!-- no input ports -->"
        replaceTokenInFile(opmodel_xml, "__SPLPY__INPORTS__SPLPY__", ipd);

        # output ports
        if cls._splpy_output_ports:
            opd = ''
            for portfn in cls._splpy_output_ports:
                top = _OP_OUTPUT_PORT_SET_TEMPLATE
                opd += top
        else:
            opd = "<!-- no output ports -->"
        replaceTokenInFile(opmodel_xml, "__SPLPY__OUTPORTS__SPLPY__", opd);
   
    # Write information about the Python function parameters.
    #
    def _write_style_info(self, cfgfile, opobj):
            style = opobj._splpy_style

            if isinstance(style, list):
                pm_style = '(' + ','.join(["'{0}'".format(_) for _ in opobj._splpy_style]) + ')'
            else:
                pm_style = "'" + style + "'"
            cfgfile.write('sub splpy_ParamStyle {'+ pm_style + '}\n')
             
            if isinstance(opobj._splpy_fixed_count, list):
                pm_fixed = '(' + ','.join(["'{0}'".format(_) for _ in opobj._splpy_fixed_count]) + ')'
            else:
                pm_fixed = "'" + str(opobj._splpy_fixed_count) + "'"

            cfgfile.write('sub splpy_FixedParam { '+ pm_fixed + "}\n")
 

    # Write out the configuration for the operator
    # as a set of Perl functions that return useful values
    # for the code generator
    def _write_config(self, dynm, opdir, module, opname, opobj):
        cfgpath = os.path.join(opdir, 'splpy_operator.pm')
        cfgfile = open(cfgpath, 'w')
        cfgfile.write('sub splpy_Module { \''+ module   + "\'}\n")
        cfgfile.write('sub splpy_OperatorCallable {\'' + _opcallable(opobj) + "\'}\n")
        cfgfile.write('sub splpy_FunctionName {\'' + opname + "\'}\n")
        cfgfile.write('sub splpy_OperatorType {\'' + _optype(opobj).name + "\'}\n")
        self._write_style_info(cfgfile, opobj)

        if hasattr(dynm, 'spl_pip_packages'):
            pp = getattr(dynm, 'spl_pip_packages')()
            if not isinstance(pp, list):
                pp = list(pp)
        else:
            pp = []

        cfgfile.write('sub splpy_Packages {(' + ','.join(["'{0}'".format(_) for _ in pp]) + ')}\n')

        cfgfile.write("1;\n")
        cfgfile.close()

    # Copy a single file from the templates directory to the newly created operator directory
    def _copy_template_dir(self, dir):
        self._copy_python_dir(os.path.join("templates", dir))

    def _copy_python_dir(self, dir):
        cmn_src = os.path.join(_topology_tk_dir(), "opt", "python", dir);
        cmn_dst = os.path.join(self._tk_dir, "opt", ".__splpy", os.path.basename(dir))
        if (os.path.isdir(cmn_dst)):
            shutil.rmtree(cmn_dst)
        shutil.copytree(cmn_src, cmn_dst)

    def _setup_info_xml(self, languageList):
        '''Setup the info.xml file
        
        This function prepares or checks the info.xml file in the project directory
        - if the info.xml does not exist in the project directory, it copies the template info.xml into the project directory.
          The project name is obtained from the project directory name
        - If there is a info.xml file, the resource section is inspected. If the resource section has no valid message set
          description for the TopologySplpy Resource a warning message is printed'''
        infoXmlFile = os.path.join(self._tk_dir, 'info.xml')
        print('Check info.xml:', infoXmlFile)
        try:
            TopologySplpyResourceMessageSetFound = False
            TopologySplpyResourceLanguages = []
            tree = ET.parse(infoXmlFile)
            root = tree.getroot()
            for resources in root.findall('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}resources'):
                if self._cmd_args.verbose:
                    print('Resource: ', resources.tag)
                for messageSet in resources.findall('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}messageSet'):
                    if self._cmd_args.verbose:
                        print('Message set:', messageSet.tag, messageSet.attrib)
                    if 'name' in messageSet.attrib:
                        if messageSet.attrib['name'] == 'TopologySplpyResource':
                            TopologySplpyResourceMessageSetFound = True
                            for lang in messageSet.findall('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}lang'):
                                language = os.path.dirname(lang.text)
                                TopologySplpyResourceLanguages.append(language)
            if TopologySplpyResourceMessageSetFound:
                TopologySplpyResourceLanguages.sort()
                languageList.sort()
                copiedLanguagesSet = set(languageList)
                resourceLanguageSet = set(TopologySplpyResourceLanguages)
                if self._cmd_args.verbose:
                    print('copied language resources:\n', languageList)
                    print('TopologySplpyResource from info.xml:\n', TopologySplpyResourceLanguages)
                if copiedLanguagesSet == resourceLanguageSet:
                    print('Resource section of info.xml verified')
                else:
                    errstr = """"ERROR: Message set for the "TopologySplpyResource" is incomplete or invalid. Correct the resource section in info.xml file.
    
                    Sample info xml:\n""" + _INFO_XML_TEMPLATE
                    sys.exit(errstr)
            else:
                errstr = """"ERROR: Message set for the "TopologySplpyResource" is missing. Correct the resource section in info.xml file.
    
                    Sample info xml:\n""" + _INFO_XML_TEMPLATE
                sys.exit(errstr)
        except FileNotFoundError as e:
            print("WARNING: File info.xml not found. Creating info.xml from template")
            #Get default project name from project directory
            projectRootDir = os.path.abspath(self._tk_dir) #os.path.abspath returns the path without trailing /
            projectName = os.path.basename(projectRootDir)
            infoXml=_INFO_XML_TEMPLATE.replace('__SPLPY_TOOLKIT_NAME__', projectName)
            f = open(infoXmlFile, 'w')
            f.write(infoXml)
            f.close()
        except SystemExit as e:
            raise e
        except:
            errstr = """ERROR: File info.xml is invalid or not accessible
    
                Sample info xml:\n""" + _INFO_XML_TEMPLATE
            sys.exit(errstr)

def _extract_from_toolkit(args):
    """
    Look at all the modules in opt/python/streams (opt/python/streams/*.py)
    and extract any spl decorated function as an operator.
    """

    extractor = _Extractor(args)

    tk_dir = extractor._tk_dir

    tk_streams = os.path.join(tk_dir, 'opt', 'python', 'streams')
    if not os.path.isdir(tk_streams) or not fnmatch.filter(os.listdir(tk_streams), '*.py'):
        # Nothing to do for Python extraction
        extractor._make_toolkit()
        return

    lf = os.path.join(tk_streams, '.lockfile')
    with  open(lf, 'w') as lfno:
        fcntl.flock(lfno, fcntl.LOCK_EX)

        tk_idx = os.path.join(tk_dir, 'toolkit.xml')
        tk_time = os.path.getmtime(tk_idx) if os.path.exists(tk_idx) else None
        changed = False if tk_time else True
        if tk_time:
            for mf in glob.glob(os.path.join(tk_streams, '*.py')):
               if os.path.getmtime(mf) >= tk_time:
                   changed = True
                   break

        if changed:
            path_items = _setup_path(tk_dir, tk_streams)

            for mf in glob.glob(os.path.join(tk_streams, '*.py')):
                print('Checking ', mf, 'for operators')
                name  = inspect.getmodulename(mf)
                dynm = imp.load_source(name, mf)
                streams_python_file = inspect.getsourcefile(dynm)
                extractor._process_operators(dynm, name, streams_python_file, inspect.getmembers(dynm, inspect.isfunction))
                extractor._process_operators(dynm, name, streams_python_file, inspect.getmembers(dynm, inspect.isclass))

            langList = extractor._copy_globalization_resources()
            if extractor._cmd_args.verbose:
                print("Available languages for TopologySplpy resource:", langList)
            extractor._setup_info_xml(langList)

            extractor._make_toolkit()

            _reset_path(path_items)
            fcntl.flock(lfno, fcntl.LOCK_UN)

def _setup_path(tk_dir, tk_streams):
    sys.path.insert(1, tk_streams)
    items = [tk_streams]
    tk_packages = os.path.join(tk_dir, 'opt', 'python', 'packages')
    if os.path.isdir(tk_packages):
        sys.path.insert(1, tk_packages)
        items.append(tk_packages)
    tk_modules = os.path.join(tk_dir, 'opt', 'python', 'modules')
    if os.path.isdir(tk_modules):
        sys.path.insert(1, tk_modules)
        items.append(tk_modules)
    return items

def _reset_path(items):
    for p in items:
        sys.path.remove(p)

def main(args=None):
    _extract_from_toolkit(args)

if __name__ == '__main__':
    main()
