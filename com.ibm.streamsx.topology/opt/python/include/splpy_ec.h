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

#if __SPLPY_EC_MODULE_OK

#include <SPL/Runtime/ProcessingElement/ProcessingElement.h>
#include <SPL/Runtime/Operator/OperatorContext.h>
#include <SPL/Runtime/Operator/OperatorMetrics.h>
#include <SPL/Runtime/Common/Metric.h>
#include <SPL/Runtime/Function/UtilFunctions.h>

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

static PyObject * __splpy_ec_get_app_config(PyObject *self, PyObject *pyname) {

   SPL::rstring name;
   streamsx::topology::pySplValueFromPyObject(name, pyname);

   SPL::map<SPL::rstring, SPL::rstring> properties;
   if (SPL::Functions::Utility::getApplicationConfiguration(properties, name) ==  0)
       return streamsx::topology::pySplValueToPyObject(properties);

   return streamsx::topology::SplpyGeneral::getBool(false);
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

   SPL::Metric & cm = metrics.createCustomMetric(name, desc, kind);
   cm.setValue(PyLong_AsLong(pyvalue));

   return PyLong_FromVoidPtr(reinterpret_cast<void *>(&cm));
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
#endif

