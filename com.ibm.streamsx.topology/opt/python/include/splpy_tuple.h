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
 */


#ifndef __SPL__SPLPY_TUPLE_H
#define __SPL__SPLPY_TUPLE_H

#include "splpy_general.h"

namespace streamsx {
  namespace topology {

  /**
   * Call a Python function passing in the SPL tuple as 
   * the single element of a Python tuple.
   * Steals the reference to value.
   */
  inline PyObject * pyCallTupleFunc(PyObject *function, PyObject *pyTuple) {

      PyObject * pyReturnVar = PyObject_CallObject(function, pyTuple);
      Py_DECREF(pyTuple);

      return pyReturnVar;
    }

  /**
   * Convert the SPL tuple that represents a Python object
   * to a Python tuple that holds:
   *  - pickled value as a memory view object
   */
  inline PyObject * pySplProcessTuple(PyObject * function, const SPL::blob & pyo) {
      PyObject *pickledValue = pySplValueToPyObject(pyo);

      PyObject *pyTuple = PyTuple_New(1);
      PyTuple_SET_ITEM(pyTuple, 0, pickledValue);

      return pyCallTupleFunc(function, pyTuple);
  }

  inline PyObject * pySplProcessTuple(PyObject * function, const SPL::rstring & pys) {
      PyObject *stringValue = pySplValueToPyObject(pys);

      PyObject * pyTuple = PyTuple_New(1);
      PyTuple_SET_ITEM(pyTuple, 0, stringValue);

      return pyCallTupleFunc(function, pyTuple);
  }

  inline PyObject * pySplProcessTuple(PyObject * function, PyObject * pyv) {

      PyObject * pyTuple = PyTuple_New(1);
      PyTuple_SET_ITEM(pyTuple, 0, pyv);

      return pyCallTupleFunc(function, pyTuple);
  }
    /**
     * Call a Python function passing in the SPL tuple as 
     * the single element of a Python tuple.
     * Steals the reference to value.
    */
    inline PyObject * pyTupleFunc(PyObject * function, PyObject * value) {
      PyObject * pyTuple = PyTuple_New(1);
      PyTuple_SET_ITEM(pyTuple, 0, value);

      return pyCallTupleFunc(function, pyTuple);
    }

}
}
#endif
