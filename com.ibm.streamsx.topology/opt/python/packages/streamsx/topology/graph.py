# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2016

from __future__ import unicode_literals
from future.builtins import *
from past.builtins import basestring

import sys
import uuid
import json
import inspect
import datetime
import pickle
from enum import Enum

import dill

import types
import base64
import re
import streamsx.topology.dependency
import streamsx.topology.functions
import streamsx.topology.param
import streamsx.topology.state
import streamsx.spl.op
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.schema import _stream_schema


def _fix_namespace(ns):
    ns = str(ns)
    sns = ns.split('.')
    if len(sns) == 1:
        return re.sub(r'\W+', '', ns, flags=re.UNICODE)
    for i in range(0,len(sns)):
        sns[i] = re.sub(r'\W+', '', sns[i], flags=re.UNICODE)

    for i in range(len(sns), 0):
        if len(sns[i]) == 0:
            sns.pop(i)

    return '.'.join(sns)

def _as_spl_expr(value):
    """ Return value converted to an SPL expression if
    needed other otherwise value.
    """
    import streamsx._streams._numpy

    if hasattr(value, 'spl_json'):
        return value
      
    if isinstance(value, Enum):
        value = streamsx.spl.op.Expression.expression(value.name)

    npcnv = streamsx._streams._numpy.as_spl_expr(value)
    if npcnv is not None:
        return npcnv
    
    return value

# Return a value suitable for use inthe JSON used to
# create the SPL. Returns `value.spl_json() if value has it otherwise
# fn(value) or value if fn is not supplied.
def _as_spl_json(value, fn=None):
    if hasattr(value, 'spl_json'):
        return value.spl_json()
    if fn:
        return fn(value)
    return value

class SPLGraph(object):

    def __init__(self, topology, name=None, namespace=None):
        if name is None:
            name = str(uuid.uuid1()).replace("-", "")

        if namespace is None:
            namespace = name
        
        # Allows Topology or SPLGraph to be passed to submit
        self.graph = self
        # Remove 'awkward characters' from names
        self.name = re.sub(r'\W+', '', str(name), flags=re.UNICODE)
        self.namespace = _fix_namespace(namespace)
        self.topology = topology
        self.operators = []
        self.resolver = streamsx.topology.dependency._DependencyResolver(self.topology)
        self._views = []
        self._spl_toolkits = []
        self._used_names = {'list', 'tuple', 'int'}
        self._layout_group_id = 0
        self._colocate_tag_mapping = {}
        self._id_gen = 0

    def _unique_id(self, prefix):
        """
        Generate a unique (within the graph) identifer
        internal to graph generation.
        """
        _id = self._id_gen
        self._id_gen += 1
        return prefix + str(_id)

    def get_views(self):
        return self._views

    def add_views(self, view):
        self._views.append(view)

    def _requested_name(self, name, action=None, func=None):
        """Create a unique name for an operator or a stream.
        """
        if name is not None:
            if name in self._used_names:
                # start at 2 for the "second" one of this name
                n = 2
                while True:
                    pn = name + '_' + str(n)
                    if pn not in self._used_names:
                        self._used_names.add(pn)
                        return pn
                    n += 1
            else:
                self._used_names.add(name)
                return name

        if func is not None:
            if hasattr(func, '__name__'):
                name = func.__name__
                if name == '<lambda>':
                    # Avoid use of <> characters in name
                    # as they are converted to unicode
                    # escapes in SPL identifier
                    name = action + '_lambda'
            elif hasattr(func, '__class__'):
                name = func.__class__.__name__

        if name is None:
            if action is not None:
                name = action
            else:
                name = self.name

        # Recurse once to get unique version of name
        return self._requested_name(name)


    def addOperator(self, kind, function=None, name=None, params=None, sl=None, stateful=None):
        if(params is None):
            params = {}

        if name is None:
            name = self._requested_name(None,action="Op", func = function)

        if(kind.startswith("$")):    
            op = Marker(len(self.operators), kind, name, {}, self)                           
        else:
            if function is not None:
                params['toolkitDir'] = streamsx.topology.param.toolkit_dir()
            op = _SPLInvocation(len(self.operators), kind, function, name, params, self, sl=sl, stateful=stateful)
        self.operators.append(op)
        if not function is None:
            dep_instance = function
            if isinstance(function, streamsx._streams._runtime._WrapOpLogic):
                dep_instance = type(function._callable)

            self.add_dependency(dep_instance)

        return op

    def add_dependency(self, obj_):
        if not inspect.isbuiltin(obj_):
            self.resolver.add_dependencies(inspect.getmodule(obj_))
    
    def addPassThruOperator(self):
        name = self.name + "_OP"+str(len(self.operators))
        op = _SPLInvocation(len(self.operators), "spl.relational::Functor", None, name, {}, self)
        self.operators.append(op)
        return op

    def _next_layout_group_id(self):
        lgi = '__spl_lg_' + str(self._layout_group_id)
        self._layout_group_id += 1
        return lgi

    def generateSPLGraph(self):
        self.topology._prepare()
        _graph = {}
        _graph["name"] = self.name
        _graph["namespace"] = self.namespace
        _graph["public"] = True
        _graph["config"] = {}
        self._determine_model(_graph["config"])
        _graph["config"]["includes"] = []
        _graph['config']['spl'] = {}
        _graph['config']['spl']['toolkits'] = self._spl_toolkits
        self._add_parameters(_graph)
        self._add_checkpoint(_graph)
        if self._colocate_tag_mapping:
            _graph['config']['colocateTagMapping'] = self._colocate_tag_mapping
        _ops = []
        self._add_modules(_graph["config"]["includes"])
        self._add_packages(_graph["config"]["includes"])
        self._add_files(_graph["config"]["includes"])
        for op in self.operators:
            _ops.append(op.generateSPLOperator())

        _graph["operators"] = _ops
        return _graph

    def _determine_model(self, graph_cfg):
        # Python can be used to build pure SPL
        # graphs so if that's the case mark the
        # graph model/langauge as spl/spl
        all_spl = True
        for op in self.operators:
            if op.model != 'spl':
                all_spl = False
                break

        graph_cfg['model'] = 'spl' if all_spl else 'functional' 
        graph_cfg['language'] = 'spl' if all_spl else 'python'

    def _add_packages(self, includes):
        for package_path in self.resolver.packages:
           mf = {}
           mf["source"] = package_path
           mf["target"] = "opt/python/packages"
           includes.append(mf)

    def _add_modules(self, includes):
        for module_path in self.resolver.modules:
           mf = {}
           mf["source"] = module_path
           mf["target"] = "opt/python/modules"
           includes.append(mf)

    def _add_files(self, includes):
         fls = self.topology._files
         for location in fls:
             files = fls[location]
             for path in files:
                 if isinstance(path, basestring):
                     # Simple file with a source to copy
                     f = {}
                     f['source'] = path
                     f['target'] = location
                     includes.append(f)
                 else:
                     # Arbitray file description
                     includes.append(path)

    def _add_parameters(self, _graph):
        sps = self.topology._submission_parameters
        if not sps:
            return
        params = dict()
        _graph['parameters'] = params
        for name, sp in sps.items():
            params[name] = sp.spl_json()

    def _add_checkpoint(self, _graph):

        if self.topology.checkpoint_period is None:
            pass
        else:            
            unit = "MICROSECONDS"

            _graph["config"]["checkpoint"] = {}
            _graph["config"]["checkpoint"]["mode"] = "periodic"
            _graph["config"]["checkpoint"]["period"] = self.topology.checkpoint_period * 1000 * 1000 # Seconds to microseconds
            _graph["config"]["checkpoint"]["unit"] = unit


class _SPLInvocation(object):

    def __init__(self, index, kind, function, name, params, graph, view_configs = None, sl=None, stateful=None):
        self.index = index
        self.kind = kind
        self.model = None
        self.language = None
        self.function = function
        self.name = name
        self.category = None
        self.params = {}
        self.setParameters(params)
        self.config = {}
        self.graph = graph
        self.viewable = True
        self.sl = sl
        self._placement = {}
        self._start_op = False
        self._consistent = None
        # Arbitrary JSON for operator
        self._op_def = {}

        if view_configs is None:
            self.view_configs = []
        else:
            self.view_configs = view_configs

        self.inputPorts = []
        self.outputPorts = []
        self._layout_hints = {}
        self._addOperatorFunction(self.function, stateful)

    def addOutputPort(self, oWidth=None, name=None, inputPort=None, schema= CommonSchema.Python,partitioned_keys=None, routing = None):
        if name is None:
            name = self.name + "_OUT"+str(len(self.outputPorts))
        oport = OPort(name, self, len(self.outputPorts), schema, oWidth, partitioned_keys, routing=routing)
        self.outputPorts.append(oport)
        if schema == CommonSchema.Python:
            self.viewable = False

        if not inputPort is None:
            oport.connect(inputPort)
        return oport

    def setParameters(self, params):
        import streamsx.spl.op
        for param in params:
            if params[param] is None:
                # map Python None to SPL null
                params[param] = streamsx.spl.op.Expression.expression("null")
            self.params[param] = params[param]

    def appendParameters(self, params):
        for param in params:
            if self.params.get(param) is None:
                self.params[param] = params[param]
            else:
                for innerParam in param:
                    self.params[param].append(innerParam)

    def getViewConfig(self):
        return self.view_configs

    def addViewConfig(self, view_configs):
        self.view_configs.append(view_configs)

    def addInputPort(self, outputPort=None, window_config=None, alias=None):
        iPortSchema = CommonSchema.Python    
        if not outputPort is None :
            iPortSchema = outputPort.schema        
        iport = IPort(self, len(self.inputPorts),iPortSchema, window_config)
        self.inputPorts.append(iport)

        if not outputPort is None:
            iport.connect(outputPort)
        if alias:
            iport._alias = alias
        return iport


    def generateSPLOperator(self):
        _op = dict(self._op_def)
        _op["name"] = self.name
        if self.category:
            _op["category"] = self.category

        _op["kind"] = self.kind
        if self.model:
            _op["model"] = self.model
        if self.language:
           _op["language"] = self.language

        _op["partitioned"] = False
        if self._start_op:
            _op["startOp"] = True

        _outputs = []
        _inputs = []

        for output in self.outputPorts:
            _outputs.append(output.getSPLOutputPort())

        for input in self.inputPorts:
            _inputs.append(input.getSPLInputPort())
        _op["outputs"] = _outputs
        _op["inputs"] = _inputs
        _op["config"] = self.config
        _op["config"]["streamViewability"] = self.viewable
        _op["config"]["viewConfigs"] = self.view_configs
        if self._placement:
            _op["config"]["placement"] = self._placement
            if 'resourceTags' in self._placement:
                # Convert the set to a list for JSON
                tags = _op['config']['placement']['resourceTags']
                _op['config']['placement']['resourceTags'] = list(tags)

        # Fix up any pending streams for input style
        if 'pyStyle' in self.params and 'pending' == self.params['pyStyle']\
                and self.kind.startswith('com.ibm.streamsx.topology.functional.python'):
            StreamSchema._fnop_style(self.inputPorts[0].schema, self, 'pyStyle')

        # Add parameters as their natural representation
        # unless they value has a spl_json() function,
        # then use that
        _params = {}

        for name in self.params:
            param = _as_spl_expr(self.params[name])
            try:
                _params[name] = param.spl_json()
            except:
                _value = {}
                _value["value"] = param
                _params[name] = _value
        _op["parameters"] = _params

        if self.sl is not None:
           _op['sourcelocation'] = self.sl.spl_json()

        if self._layout_hints:
            _op['layout'] = self._layout_hints

        if self._consistent is not None:
            _op['consistent'] = {}
            consistent = _op['consistent']
            consistent['trigger'] = self._consistent.trigger.name
            if self._consistent.trigger == streamsx.topology.state.ConsistentRegionConfig.Trigger.PERIODIC:
                if isinstance(self._consistent.period, datetime.timedelta):
                    consistent_period = self._consistent.period.total_seconds()
                else:
                    consistent_period = float(self._consistent.period)
                consistent['period'] = str(consistent_period)

            if isinstance(self._consistent.drain_timeout, datetime.timedelta):
                consistent_drain = self._consistent.drain_timeout.total_seconds();
            else:
                consistent_drain = float(self._consistent.drain_timeout)
            consistent['drainTimeout'] = str(consistent_drain)

            if isinstance(self._consistent.reset_timeout, datetime.timedelta):
                consistent_reset = self._consistent.reset_timeout.total_seconds();
            else:
                consistent_reset = float(self._consistent.reset_timeout)
            consistent['resetTimeout'] = str(consistent_reset)

            consistent['maxConsecutiveResetAttempts'] = int(self._consistent.max_consecutive_attempts)

        # Callout to allow a ExtensionOperator
        # to augment the JSON
        if hasattr(self, '_ex_op'):
            self._ex_op._generate(_op)
        return _op

    def _addOperatorFunction(self, function, stateful):
        if (function is None):
            return None
        if not hasattr(function, "__call__"):
            raise "argument to _addOperatorFunction is not callable"

        self.model = 'functional'
        self.language = 'python'

        # Wrap a lambda as a callable class instance
        if isinstance(function, types.LambdaType) and function.__name__ == "<lambda>" :
            function = streamsx.topology.runtime._Callable(function, no_context=True)
        elif function.__module__ == '__main__':
            # Function/Class defined in main, create a callable wrapping its
            # dill'ed form
            function = streamsx.topology.runtime._Callable(function,
                no_context = True if inspect.isroutine(function) else None)
         
        if inspect.isroutine(function):
            # callable is a function
            self.params["pyName"] = function.__name__
        else:
            # callable is a callable class instance
            self.params["pyName"] = function.__class__.__name__
            # dill format is binary; base64 encode so it is json serializable 
            #print('DILL-----------------------------------------------')
            #dddddd = dill.dumps(function)
            #print('DILL-LEN', len(dddddd))
            #print(dddddd)
            #print('END-DILL-----------------------------------------------')
            self.params["pyCallable"] = base64.b64encode(dill.dumps(function)).decode("ascii")

        if stateful is not None:
            self.params['pyStateful'] = bool(stateful)
            if not stateful:
                self.config['noCheckpoint'] = True
                 
        # note: functions in the __main__ module cannot be used as input to operations 
        # function.__module__ will be '__main__', so C++ operators cannot import the module
        self.params["pyModule"] = function.__module__

    def _remap_colocate_tag(self, colocate_id, tag):
        ctm = self.graph._colocate_tag_mapping
        if tag in ctm:
            old_colocate_id = ctm[tag]
            if old_colocate_id != colocate_id:
                ctm[tag] = colocate_id
                self._remap_colocate_tag(colocate_id, old_colocate_id)
        else:
            ctm[tag] = colocate_id

    
    def colocate(self, others, why):
        """
        Colocate this operator with another.
        """
        if isinstance(self, Marker):
            return
        colocate_tag = '__spl_' + why + '$' + str(self.index)
        self._colocate_tag(colocate_tag)
        for op in others:
            op._colocate_tag(colocate_tag)

    def _colocate_tag(self, colocate_tag):
        if 'colocateTags' not in self._placement:
            self._placement['colocateTags'] = []
        self._placement['colocateTags'].append(colocate_tag)

    def consistent(self, consistent_config):
        self._consistent = consistent_config

    def _layout(self, kind=None, hidden=None, name=None, orig_name=None):
        if kind:
           self._layout_hints['kind'] = str(kind)
        if hidden:
           self._layout_hints['hidden'] = True
        if name:
            self._layout_map_name(name, orig_name)


    def _layout_map_name(self, name, orig_name):
        if orig_name and name != orig_name:
            if 'names' not in self._layout_hints:
                self._layout_hints['names'] = dict()
            self._layout_hints['names'][name] = orig_name

    def _layout_group(self,kind, name, group_id=None):
        group = {}
        if group_id is None:
            group_id = self.graph._next_layout_group_id()
        group['id'] = group_id
        group['name'] = name
        group['kind'] = kind
        self._layout_hints['group'] = group
        return group_id

    def _printOperator(self):
        print(self.name+":")
        print("inputs:" + str(len(self.inputPorts)))
        for port in self.inputPorts:
            print(port.name())
        print("outputs:" + str(len(self.outputPorts)))
        for port in self.outputPorts:
            print(port.name)

# Input ports don't have a name in SPL but the code generation
# keys ports by their name so we create a unique internal identifier
# for the name.

class IPort(object):
    def __init__(self, operator, index, schema, window_config):
        self.name = operator.graph._unique_id('$__spl_ip')
        self._alias = None
        self.operator = operator
        self.index = index
        self.schema = schema
        self.window_config = window_config
        self.outputPorts = []

    def connect(self, oport):
        if not oport in self.outputPorts:
            self.outputPorts.append(oport)

        if not self in oport.inputPorts:
            oport.connect(self)

    def getSPLInputPort(self):
        _iport = {}
        _iport["name"] = self.name
        if self._alias:
            _iport['alias'] = self._alias
        _iport["connections"] = [port.name for port in self.outputPorts]
        _iport["type"] = self.schema.schema()
        if self.window_config is not None:
            _iport['window'] = self.window_config
        return _iport

class OPort(object):
    def __init__(self, name, operator, index, schema, width=None, partitioned_keys=None, routing=None):
        self.name = name
        self.operator = operator
        self.schema = _stream_schema(schema)
        self.index = index
        self.width = width
        self.partitioned = partitioned_keys is not None
        self.partitioned_keys = partitioned_keys
        self.routing = routing

        self.inputPorts = []

    def connect(self, iport):
        if not iport in self.inputPorts:
            self.inputPorts.append(iport)
        
        if not self in iport.outputPorts:
            iport.connect(self)

    def getSPLOutputPort(self):
        _oport = {}
        _oport["type"] = self.schema.schema()
        _oport["name"] = self.name
        _oport["connections"] = [port.name for port in self.inputPorts]
        _oport["routing"] = self.routing

        if not self.width is None:
            _oport['width'] = _as_spl_json(self.width, int)
        if not self.partitioned is None:
            _oport["partitioned"] = self.partitioned
        if self.partitioned_keys is not None:
            _oport["partitionedKeys"] = self.partitioned_keys

        return _oport

class Marker(_SPLInvocation):

    def __init__(self, index, kind, name, params, graph):
        self.index = index
        self.kind = kind
        self.model = 'virtual'
        self.name = name
        self.params = {}
        self.setParameters(params)
        self.graph = graph

        self.inputPorts = []
        self.outputPorts = []
                   

    def generateSPLOperator(self):
        _op = {}
        _op["name"] = self.name

        _op["kind"] = self.kind
        _op["partitioned"] = False

        _op["marker"] = True
        _op["model"] = self.model
        _op["language"] = "marker"

        _outputs = []
        _inputs = []

        for output in self.outputPorts:
            _outputs.append(output.getSPLOutputPort())

        for input in self.inputPorts:
            _inputs.append(input.getSPLInputPort())
        _op["outputs"] = _outputs
        _op["inputs"] = _inputs
        _op["config"] = {}

        return _op

