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
#include "splpy_tuple.h"
#include "splpy_op.h"

#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <memory>
#include <TopologySplpyResource.h>

/**
 * Functionality for executing Python within IBM Streams.
 */

namespace streamsx {
  namespace topology {

    class Splpy {

      public:

    /*
    * Call a function passing the SPL attribute value of type T
    * and fill in the SPL attribute of type R with its result.
    * Implementation for function Map operator.
    */
    template <class T, class R>
    static int pyTupleMap(PyObject * function, T & splVal, R & retSplVal) {
      SplpyGIL lock;

      // invoke python nested function that calls the application function
      PyObject * pyReturnVar = pyTupleMap(function, splVal);

      if (pyReturnVar == NULL)
          return 0;

      pySplValueFromPyObject(retSplVal, pyReturnVar);
      Py_DECREF(pyReturnVar);

      return 1;
    }

    template <class T>
    static PyObject * pyTupleMap(PyObject * function, T & splVal) {

      // invoke python nested function that calls the application function
      PyObject * pyReturnVar = pySplProcessTuple(function, splVal);

      if (SplpyGeneral::isNone(pyReturnVar)) {
        Py_DECREF(pyReturnVar);
        return NULL;
      } else if(pyReturnVar == 0){
         throw SplpyExceptionInfo::pythonError("map");
      } 

      return pyReturnVar;
    }

    /**
     * Implementation for Map operator when the output port
     * can pass by reference.
     * occ = 0,-1 do not pass by ref
     * occ >= 1 - pass by ref - occ is the reference count bumps
     * we must leave the object with.
     */
    template <class T>
    static int pyTupleMapByRef(PyObject * function, T & splVal, SPL::blob & retSplVal, int32_t occ) {
      SplpyGIL lock;

      // invoke python nested function that calls the application function
      PyObject * pyReturnVar = pyTupleMap(function, splVal);

      if (pyReturnVar == NULL)
        return 0;

      if (occ > 0) {
          pyTupleByRef(retSplVal, pyReturnVar, occ);
          return 1;
      } 

      pySplValueFromPyObject(retSplVal, pyReturnVar);
      Py_DECREF(pyReturnVar);

      return 1;
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
