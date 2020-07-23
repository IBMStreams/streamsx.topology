/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
*/

/*
 * Internal header file supporting Python
 * for com.ibm.streamsx.topology.
 *
 * This is not part of any public api for
 * the toolkit or toolkit with decorated
 * SPL Python operators.
 *
 * Functionality related to accessing
 * information from the pe/operator context.
 *
 * Note: When a C function is called from Python, it borrows
 * references to its arguments from the caller. 
 */

#ifndef __SPL__SPLPY_EC_H
#define __SPL__SPLPY_EC_H

#include "splpy_ec_api.h"
#include "splpy_general.h"
#include "splpy_exc.h"

#include <SPL/Runtime/ProcessingElement/ProcessingElement.h>
#include <SPL/Runtime/Operator/OperatorMetrics.h>
#include <SPL/Runtime/Common/Metric.h>
#include <SPL/Runtime/Function/UtilFunctions.h>

namespace streamsx {
  namespace topology {

/**
 * An "interface" for a SPL python primtive operator
 * that can submit tuples from Python code. This allows
 * the python code through __splpy_ec_submit to submit
 * a tuple to an output port directly.
 */
class SplpyPrimitiveOp
{
    public:
        virtual ~SplpyPrimitiveOp() {} 
        virtual void convertAndSubmit(uint32_t port, PyObject *tuple_) = 0;
        virtual void submitPunct(uint32_t port) = 0;
};

}}

extern "C" {

/**
* Utility function to get the operator context
* reference from operator pointer previously
* saved as a Python Long wrapping a pointer.
*/
static SPL::OperatorContext &  __splpy_ec_opcontext(PyObject *opc) {
    void * opptr = PyLong_AsVoidPtr(opc);
    SPL::Operator * op = reinterpret_cast<SPL::Operator *>(opptr);
   
    return op->getContext();
}

static PyObject * __splpy_ec_domain_id(PyObject *self, PyObject *notused) {
   return streamsx::topology::pyUnicode_FromUTF8(SPL::ProcessingElement::pe().getDomainID());
}
static PyObject * __splpy_ec_instance_id(PyObject *self, PyObject *notused) {
   return streamsx::topology::pyUnicode_FromUTF8(SPL::ProcessingElement::pe().getInstanceID());
}

static PyObject * __splpy_ec_job_id(PyObject *self, PyObject *notused) {
   uint64_t id = SPL::ProcessingElement::pe().getJobId();
   return PyLong_FromUnsignedLong(id);
}

static PyObject * __splpy_ec_pe_id(PyObject *self, PyObject *notused) {
   uint64_t id = SPL::ProcessingElement::pe().getPEId();
   return PyLong_FromUnsignedLong(id);
}

static PyObject * __splpy_ec_is_standalone(PyObject *self, PyObject *notused) {
   bool stand = SPL::ProcessingElement::pe().isStandalone();
   return streamsx::topology::SplpyGeneral::getBool(stand);
}

static PyObject * __splpy_ec_get_application_directory(PyObject *self, PyObject *notused) {
   const SPL::rstring adrs(SPL::ProcessingElement::pe().getApplicationDirectory());
   return streamsx::topology::pySplValueToPyObject(adrs);
}

static PyObject * __splpy_ec_get_app_config(PyObject *self, PyObject *pyname) {

   SPL::rstring name;
   streamsx::topology::pySplValueFromPyObject(name, pyname);

   int rc;
   SPL::map<SPL::rstring, SPL::rstring> properties;

   Py_BEGIN_ALLOW_THREADS
   try {

       rc = SPL::Functions::Utility::getApplicationConfiguration(properties, name);
   } catch (const std::exception& e) {
       Py_BLOCK_THREADS
       throw e;
   }
   Py_END_ALLOW_THREADS

   if (rc == 0)
       return streamsx::topology::pySplValueToPyObject(properties);

   return streamsx::topology::SplpyGeneral::getBool(false);
}

static PyObject * __splpy_ec_app_trc(PyObject *self, PyObject *args) {
   PyObject *pylevel = PyTuple_GET_ITEM(args, 0);

   int pylvl = (int) PyLong_AsLong(pylevel);
   int lvl = L_TRACE;
   if (pylvl >= 40)
      lvl = L_ERROR;
   else if (pylvl >= 30)
      lvl = L_WARN;
   else if (pylvl >= 20)
      lvl = L_INFO;
   else if (pylvl >= 10)
      lvl = L_DEBUG;

   int ilvl = Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[lvl];
   if (ilvl <= Distillery::debug::app_trace_level) {
       PyObject *pymsg = PyTuple_GET_ITEM(args, 1);
       PyObject *pyaspects = PyTuple_GET_ITEM(args, 2);
       PyObject *pyfile = PyTuple_GET_ITEM(args, 3);
       PyObject *pyfunc = PyTuple_GET_ITEM(args, 4);
       PyObject *pyline = PyTuple_GET_ITEM(args, 5);

       const SPL::rstring &aspects = streamsx::topology::pyRstringFromPyObject(pyaspects);
       const SPL::rstring &func  = streamsx::topology::pyRstringFromPyObject(pyfunc);
       const SPL::rstring &file  = streamsx::topology::pyRstringFromPyObject(pyfile);
       const SPL::rstring &msg  = streamsx::topology::pyRstringFromPyObject(pymsg);
       int line = (int) PyLong_AsLong(pyline);

       Py_BEGIN_ALLOW_THREADS
       try {
           Distillery::debug::write_appmsg(ilvl,
              SPL::splAppTrcAspect(aspects),
              func, file, line, msg);
       } catch (const std::exception& e) {
           Py_BLOCK_THREADS
           throw e;
       }

       Py_END_ALLOW_THREADS
   }
 
   // Any return is going to be ignored (maybe)
   // so return an existing object with its reference bumped
   Py_INCREF(pylevel);
   return pylevel;
}
static PyObject * __splpy_ec_app_trc_level(PyObject *self, PyObject *notused) {
   int ilvl = Distillery::debug::app_trace_level;
   int pylvl = 0; // NOTSET
   if (ilvl == Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[L_ERROR])
       pylvl = 40; // ERROR 
   else if (ilvl == Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[L_WARN])
       pylvl = 30; // WARNING 
   else if (ilvl == Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[L_INFO])
       pylvl = 20; // INFO 
   else if (ilvl == Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[L_DEBUG])
       pylvl = 10; // DEBUG 
   else if (ilvl == Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[L_TRACE])
       pylvl = 10; // DEBUG 

   return PyLong_FromLong(pylvl);
}

static PyObject * __splpy_ec_app_log(PyObject *self, PyObject *args) {
   PyObject *pylevel = PyTuple_GET_ITEM(args, 0);

   int pylvl = (int) PyLong_AsLong(pylevel);
   int lvl = L_OFF;
   if (pylvl >= 40)
      lvl = L_ERROR;
   else if (pylvl >= 30)
      lvl = L_WARN;
   else if (pylvl >= 20)
      lvl = L_INFO;

   if (lvl != L_OFF) {

     int ilvl = Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[lvl];
     if (ilvl <= Distillery::debug::logger_level) {
       PyObject *pymsg = PyTuple_GET_ITEM(args, 1);
       PyObject *pyaspects = PyTuple_GET_ITEM(args, 2);
       PyObject *pyfile = PyTuple_GET_ITEM(args, 3);
       PyObject *pyfunc = PyTuple_GET_ITEM(args, 4);
       PyObject *pyline = PyTuple_GET_ITEM(args, 5);

       const SPL::rstring &aspects = streamsx::topology::pyRstringFromPyObject(pyaspects);
       const SPL::rstring &func  = streamsx::topology::pyRstringFromPyObject(pyfunc);
       const SPL::rstring &file  = streamsx::topology::pyRstringFromPyObject(pyfile);
       const SPL::rstring &msg  = streamsx::topology::pyRstringFromPyObject(pymsg);
       int line = (int) PyLong_AsLong(pyline);

       Py_BEGIN_ALLOW_THREADS
       try {

           Distillery::debug::write_log(ilvl,
               SPL::splAppLogAspect(aspects),
               func, file, line, msg);
       } catch (const std::exception& e) {
           Py_BLOCK_THREADS
           throw e;
       }

       Py_END_ALLOW_THREADS
     }
   }
 
   // Any return is going to be ignored (maybe)
   // so return an existing object with its reference bumped
   Py_INCREF(pylevel);
   return pylevel;
}
static PyObject * __splpy_ec_app_log_level(PyObject *self, PyObject *notused) {
   int ilvl = Distillery::debug::logger_level;
   int pylvl = 0; // NOTSET
   if (ilvl == Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[L_ERROR])
       pylvl = 40; // ERROR 
   else if (ilvl == Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[L_WARN])
       pylvl = 30; // WARNING 
   else if (ilvl == Distillery::debug::EXTERNAL_DEBUG_LEVEL_MAP_TO_INTERNAL[L_INFO])
       pylvl = 20; // INFO 

   return PyLong_FromLong(pylvl);
}

// Operator functions
static PyObject * __splpy_ec_channel(PyObject *self, PyObject *opc) {
   return PyLong_FromLong(__splpy_ec_opcontext(opc).getChannel());
}
static PyObject * __splpy_ec_local_channel(PyObject *self, PyObject *opc) {
   return PyLong_FromLong(__splpy_ec_opcontext(opc).getLocalChannel());
}
static PyObject * __splpy_ec_max_channels(PyObject *self, PyObject *opc) {
   return PyLong_FromLong(__splpy_ec_opcontext(opc).getMaxChannels());
}
static PyObject * __splpy_ec_local_max_channels(PyObject *self, PyObject *opc) {
   return PyLong_FromLong(__splpy_ec_opcontext(opc).getLocalMaxChannels());
}

static PyObject * __splpy_ec_create_custom_metric(PyObject *self, PyObject *args){

   PyObject *opc = PyTuple_GET_ITEM(args, 0);
   PyObject *pyname = PyTuple_GET_ITEM(args, 1);
   PyObject *pydescription = PyTuple_GET_ITEM(args, 2);
   PyObject *pykind = PyTuple_GET_ITEM(args, 3);
   PyObject *pyvalue = PyTuple_GET_ITEM(args, 4);

   SPL::rstring name;
   streamsx::topology::pySplValueFromPyObject(name, pyname);
   SPL::rstring desc;
   streamsx::topology::pySplValueFromPyObject(desc, pydescription);

   SPL::OperatorMetrics & metrics = __splpy_ec_opcontext(opc).getMetrics();
   
   SPL::Metric::Kind kind = static_cast<SPL::Metric::Kind>(PyLong_AsLong(pykind));

   int64_t value = PyLong_AsLong(pyvalue);

   void * cmptr = NULL;

   Py_BEGIN_ALLOW_THREADS
   try {

       // If the metric already exists with the same name
       // and kind then return that. Do not set the value
       // as the create custom metric is the initial value.
       if (metrics.hasCustomMetric(name)) {
           SPL::Metric &cm = metrics.getCustomMetricByName(name);
           if (cm.getKind() == kind)
               cmptr = reinterpret_cast<void *>(&cm);

           // Otherwise let SPL runtime handle the duplicate
       }

       if (cmptr == NULL) {
           SPL::Metric &cm = metrics.createCustomMetric(name, desc, kind);
           cm.setValue(value);
           cmptr = reinterpret_cast<void *>(&cm);
       }
   } catch (const SPL::SPLRuntimeInvalidMetricException& e) {
       Py_BLOCK_THREADS
       PyErr_SetString(streamsx::topology::SplpyErrors::ValueError, e.getExplanation().c_str());
       return NULL;
   } catch (const std::exception& e) {
       Py_BLOCK_THREADS
       throw e;
   }
   Py_END_ALLOW_THREADS

   return PyLong_FromVoidPtr(cmptr);
}
static PyObject * __splpy_ec_metric_get(PyObject *self, PyObject *pymptr){
   SPL::Metric * cm = reinterpret_cast<SPL::Metric *>(PyLong_AsVoidPtr(pymptr));
   return PyLong_FromLong(cm->getValue());
}
static PyObject * __splpy_ec_metric_inc(PyObject *self, PyObject *args){
   PyObject *pymptr = PyTuple_GET_ITEM(args, 0);
   PyObject *pyvalue = PyTuple_GET_ITEM(args, 1);

   SPL::Metric * cm = reinterpret_cast<SPL::Metric *>(PyLong_AsVoidPtr(pymptr));
   int64_t value = PyLong_AsLong(pyvalue);

   cm->incrementValue(value);

   // Any return is going to be ignored (maybe)
   // so return an existing object with its reference bumped
   Py_INCREF(pyvalue);
   return pyvalue;
}
static PyObject * __splpy_ec_metric_set(PyObject *self, PyObject *args){
   PyObject *pymptr = PyTuple_GET_ITEM(args, 0);
   PyObject *pyvalue = PyTuple_GET_ITEM(args, 1);

   SPL::Metric * cm = reinterpret_cast<SPL::Metric *>(PyLong_AsVoidPtr(pymptr));
   int64_t value = PyLong_AsLong(pyvalue);

   cm->setValue(value);

   // Any return is going to be ignored (maybe)
   // so return an existing object with its reference bumped
   Py_INCREF(pyvalue);
   return pyvalue;
}

// Submit a tuple to the output ports of a primitive operator.
static PyObject * __splpy_ec_submit(PyObject *self, PyObject *args) {
   PyObject *opc = PyTuple_GET_ITEM(args, 0);
   PyObject *pyport = PyTuple_GET_ITEM(args, 1);
   PyObject *pytuple = PyTuple_GET_ITEM(args, 2);

   void * opptr = PyLong_AsVoidPtr(opc);
   SPL::Operator *op = reinterpret_cast<SPL::Operator*>(opptr);
   streamsx::topology::SplpyPrimitiveOp *op2 = dynamic_cast<streamsx::topology::SplpyPrimitiveOp*>(op);

   uint32_t port = (uint32_t) PyLong_AsLong(pyport);

   op2->convertAndSubmit(port, pytuple);

   // Any return is going to be ignored
   // so return an existing object with its reference bumped
   Py_INCREF(pyport);
   return pyport;
}

// Submit a tuple to the output ports of a primitive operator.
static PyObject * __splpy_ec_submit_punct(PyObject *self, PyObject *args) {
   PyObject *opc = PyTuple_GET_ITEM(args, 0);
   PyObject *pyport = PyTuple_GET_ITEM(args, 1);

   void * opptr = PyLong_AsVoidPtr(opc);
   SPL::Operator *op = reinterpret_cast<SPL::Operator*>(opptr);
   streamsx::topology::SplpyPrimitiveOp *op2 = dynamic_cast<streamsx::topology::SplpyPrimitiveOp*>(op);

   uint32_t port = (uint32_t) PyLong_AsLong(pyport);

   op2->submitPunct(port);

   // Any return is going to be ignored
   // so return an existing object with its reference bumped
   Py_INCREF(pyport);
   return pyport;
}

static PyMethodDef __splpy_ec_methods[] = {
    {"domain_id", __splpy_ec_domain_id, METH_NOARGS,
         "Return the domain identifier."},
    {"instance_id", __splpy_ec_instance_id, METH_NOARGS,
         "Return the instance identifier."},
    {"job_id", __splpy_ec_job_id, METH_NOARGS,
         "Return the job identifier of the running application."},
    {"pe_id", __splpy_ec_pe_id, METH_NOARGS,
         "Return the PE identifier hosting this code."},
    {"is_standalone", __splpy_ec_is_standalone, METH_NOARGS,
         "Return if execution context is standalone."},
    {"get_application_configuration", __splpy_ec_get_app_config, METH_O,
         "Get application configuration."},
    {"_app_trc", __splpy_ec_app_trc, METH_O,
         "Application trace."},
    {"_app_trc_level", __splpy_ec_app_trc_level, METH_NOARGS,
         "Application trace level."},
    {"_app_log", __splpy_ec_app_log, METH_O,
         "Application log."},
    {"_app_log_level", __splpy_ec_app_log_level, METH_NOARGS,
         "Application log level."},
    {"channel", __splpy_ec_channel, METH_O,
         "Return the global parallel channel."},
    {"local_channel", __splpy_ec_local_channel, METH_O,
         "Return the local parallel channel."},
    {"max_channels", __splpy_ec_max_channels, METH_O,
         "Return the global max channels."},
    {"local_max_channels", __splpy_ec_local_max_channels, METH_O,
         "Return the local max channels."},
    {"create_custom_metric", __splpy_ec_create_custom_metric, METH_O,
         "Create a custom metric."},
    {"metric_get", __splpy_ec_metric_get, METH_O,
         "Get metric value."},
    {"metric_inc", __splpy_ec_metric_inc, METH_O,
         "Increment metric value."},
    {"metric_set", __splpy_ec_metric_set, METH_O,
         "Set metric value."},
    {"_submit", __splpy_ec_submit, METH_O,
         "Submit tuple."},
    {"_submit_punct", __splpy_ec_submit_punct, METH_O,
         "Submit window punctuation."},
    {"get_application_directory", __splpy_ec_get_application_directory, METH_NOARGS,
         "Get the application directory."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef __splpy_ec_module = {
   PyModuleDef_HEAD_INIT,
   __SPLPY_EC_MODULE_NAME,   /* name of module */
   "Internal module providing access to the Streams execution environment.",
   -1,       /* size of per-interpreter state of the module,
                or -1 if the module keeps state in global variables. */
   __splpy_ec_methods
};

PyMODINIT_FUNC
init_streamsx_ec(void)
{
    return PyModule_Create(&__splpy_ec_module);
}

}

#endif

