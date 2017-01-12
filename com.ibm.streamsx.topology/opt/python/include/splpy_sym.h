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

#define __SPLFIX(_NAME, _TYPE) \
     __spl_fp_##_NAME = ( _TYPE ) dlsym(pydl, #_NAME )


namespace streamsx {
  namespace topology {

class SplpySym {
  public:
   static void fixSymbols(void * pydl) {

     __SPLFIX(PyGILState_Ensure,  __splpy_gil_v_fp);
     __SPLFIX(PyGILState_Release,  __splpy_v_gil_fp);

   }
};

}}

#endif

