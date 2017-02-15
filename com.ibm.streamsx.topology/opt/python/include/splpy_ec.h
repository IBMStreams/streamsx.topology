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

#if __SPLPY_EC_MODULE_OK

#include <SPL/Runtime/ProcessingElement/ProcessingElement.h>

extern "C" {

/**
* Utility function to get the operator context
* reference from the args passed into an extension
* function containing the capsule at position 0.
*/
static SPL::OperatorContext &  __splpy_ec_opcontext(PyObject *args) {
    PyObject *capsule = PyTuple_GET_ITEM(args, 0);
    void * opptr = PyCapsule_GetPointer(capsule, PyCapsule_GetName(capsule));
    SPL::Operator * op = reinterpret_cast<SPL::Operator *>(opptr);
   
    return op->getContext();
}

static PyObject * __splpy_ec_job_id(PyObject *self, PyObject *args) {
   uint64_t id = SPL::ProcessingElement::pe().getJobId();
   return PyLong_FromUnsignedLong(id);
}

static PyObject * __splpy_ec_pe_id(PyObject *self, PyObject *args) {
   uint64_t id = SPL::ProcessingElement::pe().getPEId();
   return PyLong_FromUnsignedLong(id);
}

// Operator functions
static PyObject * __splpy_ec_channel(PyObject *self, PyObject *args) {
   return PyLong_FromLong(__splpy_ec_opcontext(args).getChannel());
}
static PyObject * __splpy_ec_local_channel(PyObject *self, PyObject *args) {
   return PyLong_FromLong(__splpy_ec_opcontext(args).getLocalChannel());
}
static PyObject * __splpy_ec_max_channels(PyObject *self, PyObject *args) {
   return PyLong_FromLong(__splpy_ec_opcontext(args).getMaxChannels());
}
static PyObject * __splpy_ec_local_max_channels(PyObject *self, PyObject *args) {
   return PyLong_FromLong(__splpy_ec_opcontext(args).getLocalMaxChannels());
}

static PyObject * __splpy_ec_create_custom_metric(PyObject *self, PyObject *args){
   SPL::OperatorMetrics & metrics = __splpy_ec_opcontext(args).getMetrics();
   
   PyObject *name = PyTuple_GET_ITEM(args, 1);
   PyObject *description = PyTuple_GET_ITEM(args, 2);
   PyObject *kind = PyTuple_GET_ITEM(args, 3);

   // TODO: Temp fix name and kind
   SPL::Metric & cm = metrics.createCustomMetric("M1", "M1 Description",
          Metric::Kind.Counter)

   return PyCapsule_New(&cm, NULL, NULL);
}

static PyObject * __splpy_ec_job_id(PyObject *self, PyObject *args) {
   uint64_t id = SPL::ProcessingElement::pe().getJobId();
   return PyLong_FromUnsignedLong(id);
}
}

static PyMethodDef __splpy_ec_methods[] = {
    {"job_id", __splpy_ec_job_id, METH_VARARGS,
         "Return the job identifier of the running application."},
    {"pe_id", __splpy_ec_pe_id, METH_VARARGS,
         "Return the PE identifier hosting this code."},
    {"channel", __splpy_ec_channel, METH_VARARGS,
         "Return the global parallel channel."},
    {"local_channel", __splpy_ec_local_channel, METH_VARARGS,
         "Return the local parallel channel."},
    {"max_channels", __splpy_ec_max_channels, METH_VARARGS,
         "Return the global max channels."},
    {"local_max_channels", __splpy_ec_local_max_channels, METH_VARARGS,
         "Return the local max channels."},
    {"create_custom_metric", __splpy_ec_create_custom_metric, METH_VARARGS,
         "Create a custom metric."},
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

