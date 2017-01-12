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

  static PyObject * __spl_fi_PyObject_Str(PyObject *v) {
     return __spl_fp_PyObject_Str(v);
  }
  static PyObject * __spl_fi_PyUnicode_DecodeUTF8(const char *s, Py_ssize_t size, const char * errors) {
     return __spl_fp_PyUnicode_DecodeUTF8(s, size, errors);
  }
}
#pragma weak PyObject_Str = __spl_fi_PyObject_Str
#pragma weak PyUnicode_DecodeUTF8 = __spl_fi_PyUnicode_DecodeUTF8

/*
 * Loading modules, running code
 */

typedef PyObject* (*__splpy_ogas_fp)(PyObject *, const char *);
typedef int (*__splpy_rssf_fp)(const char *, PyCompilerFlags *);

extern "C" {
  static __splpy_ogas_fp __spl_fp_PyObject_GetAttrString;
  static __splpy_rssf_fp __spl_fp_PyRun_SimpleStringFlags;

  static PyObject * __spl_fi_PyObject_GetAttrString(PyObject *o, const char * attr_name) {
     return __spl_fp_PyObject_GetAttrString(o, attr_name);
  }
  static int __spl_fi_PyRun_SimpleStringFlags(const char * command, PyCompilerFlags *flags) {
     return __spl_fp_PyRun_SimpleStringFlags(command, flags);
  }
}
#pragma weak PyObject_GetAttrString = __spl_fi_PyObject_GetAttrString
#pragma weak PyRun_SimpleStringFlags = __spl_fi_PyRun_SimpleStringFlags

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

     __SPLFIX(PyObject_GetAttrString, __splpy_ogas_fp);
     __SPLFIX(PyRun_SimpleStringFlags, __splpy_rssf_fp);
   }
};

}}

#endif

