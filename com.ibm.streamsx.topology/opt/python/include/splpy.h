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
 */


#ifndef __SPL__SPLPY_H
#define __SPL__SPLPY_H

#include "splpy_general.h"
#include "splpy_setup.h"
#include "splpy_op.h"

#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <memory>
#include <TopologySplpyResource.h>

#include <SPL/Runtime/Common/RuntimeException.h>
#include <SPL/Runtime/Type/Meta/BaseType.h>
#include <SPL/Runtime/ProcessingElement/PE.h>
#include <SPL/Runtime/Operator/Port/OperatorPort.h>
#include <SPL/Runtime/Operator/Port/OperatorInputPort.h>
#include <SPL/Runtime/Operator/Port/OperatorOutputPort.h>
#include <SPL/Runtime/Operator/OperatorContext.h>
#include <SPL/Runtime/Operator/Operator.h>

/**
 * Functionality for executing Python within IBM Streams.
 */

namespace streamsx {
  namespace topology {

    class Splpy {

      public:

    // Call the function passing an SPL attribute
    // converted to a Python object and discard the return 
    template <class T>
    static void pyTupleSink(PyObject * function, T & splVal) {
      SplpyGIL lock;

      PyObject * arg = pySplValueToPyObject(splVal);

      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      if(pyReturnVar == 0){
        throw SplpyGeneral::pythonException("sink");
      }

      Py_DECREF(pyReturnVar);
    }
    
    /*
    * Call a function passing the SPL attribute value of type T
    * and return the function return as a boolean
    */
    template <class T>
    static int pyTupleFilter(PyObject * function, T & splVal) {

      SplpyGIL lock;

      PyObject * arg = pySplValueToPyObject(splVal);

      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      if(pyReturnVar == 0){
        throw SplpyGeneral::pythonException("filter");
      }

      int ret = PyObject_IsTrue(pyReturnVar);

      Py_DECREF(pyReturnVar);
      return ret;
    }

    /*
    * Call a function passing the SPL attribute value of type T
    * and fill in the SPL attribute of type R with its result.
    */
    template <class T, class R>
    static int pyTupleTransform(PyObject * function, T & splVal, R & retSplVal) {
      SplpyGIL lock;

      PyObject * arg = pySplValueToPyObject(splVal);

      // invoke python nested function that calls the application function
      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      if (SplpyGeneral::isNone(pyReturnVar)) {
        Py_DECREF(pyReturnVar);
        return 0;
      } else if(pyReturnVar == 0){
         throw SplpyGeneral::pythonException("transform");
      } 

      pySplValueFromPyObject(retSplVal, pyReturnVar);
      Py_DECREF(pyReturnVar);

      return 1;
    }

    // Python hash of an SPL value
    // Python hashes are signed integer values
    template <class T>
    static SPL::int64 pyTupleHash(PyObject * function, T & splVal) {

      SplpyGIL lock;

      PyObject * arg = pySplValueToPyObject(splVal);

      // invoke python function that generates the hash
      PyObject * pyReturnVar = pyTupleFunc(function, arg); 
      if (pyReturnVar == 0){
        throw SplpyGeneral::pythonException("hash");
      }

      // construct integer from return value
      SPL::int64 hash = (SPL::int64) PyLong_AsLong(pyReturnVar);
      Py_DECREF(pyReturnVar);
      return hash;
   }

    /**
     * Call a Python function passing in the SPL tuple as 
     * the single element of a Python tuple.
     * Steals the reference to value.
    */
    static PyObject * pyTupleFunc(PyObject * function, PyObject * value) {
      PyObject * pyTuple = PyTuple_New(1);
      PyTuple_SET_ITEM(pyTuple, 0, value);

      PyObject * pyReturnVar = PyObject_CallObject(function, pyTuple);
      Py_DECREF(pyTuple);

      return pyReturnVar;
    }

    /**
     *  Return a Python tuple containing the attribute
     *  names for a port in order.
     */
    static PyObject * pyAttributeNames(SPL::OperatorPort & port) {
       SPL::Meta::TupleType const & tt = 
           dynamic_cast<SPL::Meta::TupleType const &>(port.getTupleType());
       uint32_t ac = tt.getNumberOfAttributes();
       PyObject * pyNames = PyTuple_New(ac);
       for (uint32_t i = 0; i < ac; i++) {
            PyObject * pyName = pyUnicode_FromUTF8(tt.getAttributeName(i));
            PyTuple_SET_ITEM(pyNames, i, pyName);
       }
       return pyNames;
    }

    };
  }
}
#endif
