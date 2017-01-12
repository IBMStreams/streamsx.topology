/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2016
*/

/*
 * Internal header file supporting Python
 * for com.ibm.streamsx.topology.
 *
 * This is not part of any public api for
 * the toolkit or toolkit with decorated
 * SPL Python operators.
 *
 * Functionality related to setting up
 * Python symbols to point to dynamically
 * loaded symbols.
 */


#ifndef __SPL__SPLPY_SYM_H
#define __SPL__SPLPY_SYM_H

#pragma weak _Py_NoneStruct

#include "Python.h"

/**
 * For a Python C API function symbol PyXXX we create
 * - typedef matching the function
 *
 * - a symbol __spl_fp_PyXXX that is dlsym() resolved to PyXXX from the
 *   dynamically loaded shared library.
 *
 * - a function __spl_fp_PyXXX that invokes the symbol __spl_fp_PyXXX
 *   and thus invokes PyXXX
 *  
 * - A weak mapping from the symbol PyXXX to __spl_fp_PyXXX. This
 *   means when the operator (PE pre-4.2) shared library is opened
 *   by the SPL runtime with RTLD_NOW PyXXX will not be marked as
 *   an unresolved symbol, instead it will resolve to __spl_fp_PyXXX
 *   This allows the code to be written using the standard Python
 *   API calls, but code in this header maps them to the dynamically
 *   loaded library.
 *
 *   This is all to allow the location of the Python dynamic shared
 *   library to be set by PYTHONHOME and loaded at runtime.
 */

/**
 * Generic typedefs potentially shared by more than one function.
 */
typedef PyObject * (*__splpy_p_p_fp)(PyObject *);
typedef PyObject * (*__splpy_p_pp_fp)(PyObject *, PyObject *);
typedef PyObject * (*__splpy_p_ppp_fp)(PyObject *, PyObject *, PyObject *);
typedef PyObject * (*__splpy_p_s_fp)(Py_ssize_t);
typedef char * (*__splpy_c_p_fp)(PyObject *);
typedef int (*__splpy_i_p_fp)(PyObject *);

/*
 * GIL State locks
 */

typedef PyGILState_STATE (*__splpy_gil_v_fp)(void);
typedef void (*__splpy_v_gil_fp)(PyGILState_STATE);

extern "C" {
  static __splpy_gil_v_fp __spl_fp_PyGILState_Ensure;
  static __splpy_v_gil_fp __spl_fp_PyGILState_Release;

  static PyGILState_STATE __spl_fi_PyGILState_Ensure() {
     return __spl_fp_PyGILState_Ensure();
  }
  static void __spl_fi_PyGILState_Release(PyGILState_STATE state) {
     __spl_fp_PyGILState_Release(state);
  }
};
#pragma weak PyGILState_Ensure = __spl_fi_PyGILState_Ensure
#pragma weak PyGILState_Release = __spl_fi_PyGILState_Release

/*
 * String handling
 */
typedef PyObject* (*__splpy_udu_fp)(const char *, Py_ssize_t, const char *);

extern "C" {
  static __splpy_p_p_fp __spl_fp_PyObject_Str;
  static __splpy_udu_fp __spl_fp_PyUnicode_DecodeUTF8;
  static __splpy_c_p_fp __spl_fp_PyBytes_AsString;

  static PyObject * __spl_fi_PyObject_Str(PyObject *v) {
     return __spl_fp_PyObject_Str(v);
  }
  static PyObject * __spl_fi_PyUnicode_DecodeUTF8(const char *s, Py_ssize_t size, const char * errors) {
     return __spl_fp_PyUnicode_DecodeUTF8(s, size, errors);
  }
  static char * __spl_fi_PyBytes_AsString(PyObject * o) {
     return __spl_fp_PyBytes_AsString(o);
  }
}
#pragma weak PyObject_Str = __spl_fi_PyObject_Str
#pragma weak PyUnicode_DecodeUTF8 = __spl_fi_PyUnicode_DecodeUTF8
#pragma weak PyBytes_AsString = __spl_fi_PyBytes_AsString

#if PY_MAJOR_VERSION == 3
typedef char * (*__splpy_uauas_fp)(PyObject *, Py_ssize_t);
typedef PyObject * (*__splpy_mvfm_fp)(char *, Py_ssize_t, int);
extern "C" {
  static __splpy_uauas_fp __spl_fp_PyUnicode_AsUTF8AndSize;
  static __splpy_mvfm_fp __spl_fp_PyMemoryView_FromMemory;
  static char * __spl_fi_PyUnicode_AsUTF8AndSize(PyObject * o, Py_ssize_t size) {
     return __spl_fp_PyUnicode_AsUTF8AndSize(o, size);
  }
  static PyObject * __spl_fi_PyMemoryView_FromMemory(char *mem, Py_ssize_t size, int flags) {
     return __spl_fp_PyMemoryView_FromMemory(mem, size, flags);
  }
}
#pragma weak PyUnicode_AsUTF8AndSize = __spl_fi_PyUnicode_AsUTF8AndSize
#pragma weak PyMemoryView_FromMemory = __spl_fi_PyMemoryView_FromMemory

#else
#endif


/*
 * Loading modules, running code
 */

typedef PyObject* (*__splpy_ogas_fp)(PyObject *, const char *);
typedef int (*__splpy_rssf_fp)(const char *, PyCompilerFlags *);

extern "C" {
  static __splpy_ogas_fp __spl_fp_PyObject_GetAttrString;
  static __splpy_rssf_fp __spl_fp_PyRun_SimpleStringFlags;
  static __splpy_p_ppp_fp __spl_fp_PyObject_Call;
  static __splpy_p_pp_fp __spl_fp_PyObject_CallObject;
  static __splpy_i_p_fp __spl_fp_PyCallable_Check;
  static __splpy_p_p_fp __spl_fp_PyImport_Import;

  static PyObject * __spl_fi_PyObject_GetAttrString(PyObject *o, const char * attr_name) {
     return __spl_fp_PyObject_GetAttrString(o, attr_name);
  }
  static int __spl_fi_PyRun_SimpleStringFlags(const char * command, PyCompilerFlags *flags) {
     return __spl_fp_PyRun_SimpleStringFlags(command, flags);
  }
  static PyObject * __spl_fi_PyObject_Call(PyObject *callable, PyObject * args, PyObject * kwargs) {
     return __spl_fp_PyObject_Call(callable, args, kwargs);
  }
  static PyObject * __spl_fi_PyObject_CallObject(PyObject *callable, PyObject * args) {
     return __spl_fp_PyObject_CallObject(callable, args);
  }
  static int __spl_fi_PyCallable_Check(PyObject *o) {
     return __spl_fp_PyCallable_Check(o);
  }
  static PyObject * __spl_fi_PyImport_Import(PyObject *name) {
     return __spl_fp_PyImport_Import(name);
  }
}
#pragma weak PyObject_GetAttrString = __spl_fi_PyObject_GetAttrString
#pragma weak PyRun_SimpleStringFlags = __spl_fi_PyRun_SimpleStringFlags
#pragma weak PyObject_Call = __spl_fi_PyObject_Call
#pragma weak PyObject_CallObject = __spl_fi_PyObject_CallObject
#pragma weak PyCallable_Check = __spl_fi_PyCallable_Check
#pragma weak PyImport_Import = __spl_fi_PyImport_Import

/*
 * Container Objects
 */
extern "C" {
  static __splpy_p_s_fp __spl_fp_PyTuple_New;

  static PyObject * __spl_fi_PyTuple_New(Py_ssize_t size) {
     return __spl_fp_PyTuple_New(size);
  }
}
#pragma weak PyTuple_New = __spl_fi_PyTuple_New

/*
 * Err Objects
 */
typedef void (*__splpy_ef_fp)(PyObject **, PyObject **, PyObject **);
typedef PyObject * (*__splpy_eo_fp)(void);
typedef void (*__splpy_ep_fp)(void);
extern "C" {
  static __splpy_ef_fp __spl_fp_PyErr_Fetch;
  static __splpy_eo_fp __spl_fp_PyErr_Occurred;
  static __splpy_ep_fp __spl_fp_PyErr_Print;

  static void __spl_fi_PyErr_Fetch(PyObject **t, PyObject **v, PyObject **tb) {
     __spl_fp_PyErr_Fetch(t,v,tb);
  }
  static PyObject * __spl_fi_PyErr_Occurred() {
     return __spl_fp_PyErr_Occurred();
  }
  static void  __spl_fi_PyErr_Print() {
     __spl_fp_PyErr_Print();
  }
}
#pragma weak PyErr_Fetch = __spl_fi_PyErr_Fetch
#pragma weak PyErr_Occurred = __spl_fi_PyErr_Occurred
#pragma weak PyErr_Print = __spl_fi_PyErr_Print

#define __SPLFIX(_NAME, _TYPE) \
     __spl_fp_##_NAME = ( _TYPE ) dlsym(pydl, #_NAME )

namespace streamsx {
  namespace topology {

class SplpySym {
  public:
   static void fixSymbols(void * pydl) {

     __SPLFIX(PyGILState_Ensure, __splpy_gil_v_fp);
     __SPLFIX(PyGILState_Release, __splpy_v_gil_fp);

     __SPLFIX(PyObject_Str, __splpy_p_p_fp);
     __SPLFIX(PyUnicode_DecodeUTF8, __splpy_udu_fp);
     __SPLFIX(PyBytes_AsString, __splpy_c_p_fp);

#if PY_MAJOR_VERSION == 3
     __SPLFIX(PyUnicode_AsUTF8AndSize, __splpy_uauas_fp);
     __SPLFIX(PyMemoryView_FromMemory, __splpy_mvfm_fp);
#else
#endif

     __SPLFIX(PyObject_GetAttrString, __splpy_ogas_fp);
     __SPLFIX(PyRun_SimpleStringFlags, __splpy_rssf_fp);
     __SPLFIX(PyObject_Call, __splpy_p_ppp_fp);
     __SPLFIX(PyObject_CallObject, __splpy_p_pp_fp);
     __SPLFIX(PyCallable_Check, __splpy_i_p_fp);
     __SPLFIX(PyImport_Import, __splpy_p_p_fp);
 
     __SPLFIX(PyTuple_New, __splpy_p_s_fp);

     __SPLFIX(PyErr_Fetch, __splpy_ef_fp);
     __SPLFIX(PyErr_Occurred, __splpy_eo_fp);
     __SPLFIX(PyErr_Print, __splpy_ep_fp);
   }
};

}}

#endif

