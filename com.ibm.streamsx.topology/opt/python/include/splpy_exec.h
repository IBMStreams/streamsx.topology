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
 */

#ifndef __SPL__SPLPY_EXEC_CONTEXT_H
#define __SPL__SPLPY_EXEC_CONTEXT_H

#if (PY_MAJOR_VERSION == 3) && \
     ((_IBM_STREAMS_VER_ > 4) || \
      ((_IBM_STREAMS_VER_ == 4) && (_IBM_STREAMS_REL_ >= 2)))

#define __SPLPY_EXEC_MODULE_OK 1
#define __SPLPY_EXEC_MODULE_NAME "_streamsx_exec"

#include <SPL/Runtime/ProcessingElement/ProcessingElement.h>

extern "C" {

static PyObject * __splpy_ec_job_id(PyObject *self, PyObject *args) {
   uint64_t id = SPL::ProcessingElement::pe().getJobId();
   return PyLong_FromUnsignedLong(id);
}

static PyObject * __splpy_ec_pe_id(PyObject *self, PyObject *args) {
   uint64_t id = SPL::ProcessingElement::pe().getPEId();
   return PyLong_FromUnsignedLong(id);
}

static PyMethodDef __splpy_ec_methods[] = {
    {"job_id", __splpy_ec_job_id, METH_VARARGS,
         "Return the job identifier of the running application."},
    {"pe_id", __splpy_ec_pe_id, METH_VARARGS,
         "Return the PE identifier hosting this code."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef __splpy_ec_module = {
   PyModuleDef_HEAD_INIT,
   __SPLPY_EXEC_MODULE_NAME,   /* name of module */
   "Internal module providing access to the Streams execution environment.",
   -1,       /* size of per-interpreter state of the module,
                or -1 if the module keeps state in global variables. */
   __splpy_ec_methods
};

PyMODINIT_FUNC
init_streamsx_exec(void)
{
    return PyModule_Create(&__splpy_ec_module);
}

}

#else
#define __SPLPY_EXEC_MODULE_OK 0
#endif
#endif

