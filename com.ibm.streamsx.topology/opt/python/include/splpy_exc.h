/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
*/

/*
 * Internal header file supporting Python
 * for com.ibm.streamsx.topology.
 *
 * This is not part of any public api for
 * the toolkit or toolkit with decorated
 * SPL Python operators.
 *
 * Functionality related to Python errors
 *
 */
#ifndef SPL_SPLPY_EXC_H_
#define SPL_SPLPY_EXC_H_

#include "SPL/Runtime/Common/RuntimeException.h"
namespace streamsx { namespace topology {

class SplpyErrors {
  public:
    static PyObject* ValueError;
    static PyObject* StopIteration;
};

PyObject* SplpyErrors::ValueError;
PyObject* SplpyErrors::StopIteration;

}}
#endif
