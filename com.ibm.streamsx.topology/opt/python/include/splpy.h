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

    /*
    ** Conversion of Python objects to SPL values.
    */

    /*
    ** Convert to a SPL blob from a Python bytes object.
    */
    inline void pySplValueFromPyObject(SPL::blob & splv, PyObject * value) {
      char * bytes = PyBytes_AsString(value);          
      if (bytes == NULL) {
         SPLAPPTRC(L_ERROR, "Python can't convert to SPL blob!", "python");
         throw SplpyGeneral::pythonException("blob");
      }
      long int size = PyBytes_GET_SIZE(value);
      splv.setData((const unsigned char *)bytes, size);
    }

    /*
    ** Convert to a SPL rstring from a Python string object.
    */
    inline void pySplValueFromPyObject(SPL::rstring & splv, PyObject * value) {
      if (pyRStringFromPyObject(splv, value) != 0) {
         SPLAPPTRC(L_ERROR, "Python can't convert to UTF-8!", "python");
         throw SplpyGeneral::pythonException("rstring");
      }
    }

    inline void pySplValueFromPyObject(SPL::ustring & splv, PyObject * value) {
         SPL::rstring rs;
         pySplValueFromPyObject(rs, value);

         splv = SPL::ustring::fromUTF8(rs);
    }

    inline SPL::rstring pyRstringFromPyObject(PyObject * value)
    {
        SPL::rstring rs;
        pySplValueFromPyObject(rs, value);
        return rs ;
    }
    inline SPL::ustring pyUstringFromPyObject(PyObject * value)
    {
        SPL::ustring us;
        pySplValueFromPyObject(us, value);
        return us ;
    }

    // signed integers
    inline void pySplValueFromPyObject(SPL::int8 & splv, PyObject * value) {
       splv = (SPL::int8) PyLong_AsLong(value);
    }
    inline void pySplValueFromPyObject(SPL::int16 & splv, PyObject * value) {
       splv = (SPL::int16) PyLong_AsLong(value);
    }
    inline void pySplValueFromPyObject(SPL::int32 & splv, PyObject * value) {
       splv = (SPL::int32) PyLong_AsLong(value);
    }
    inline void pySplValueFromPyObject(SPL::int64 & splv, PyObject * value) {
       splv = (SPL::int64) PyLong_AsLong(value);
    }

    // unsigned integers
    inline void pySplValueFromPyObject(SPL::uint8 & splv, PyObject * value) {
       splv = (SPL::uint8) PyLong_AsUnsignedLong(value);
    }
    inline void pySplValueFromPyObject(SPL::uint16 & splv, PyObject * value) {
       splv = (SPL::uint16) PyLong_AsUnsignedLong(value);
    }
    inline void pySplValueFromPyObject(SPL::uint32 & splv, PyObject * value) {
       splv = (SPL::uint32) PyLong_AsUnsignedLong(value);
    }
    inline void pySplValueFromPyObject(SPL::uint64 & splv, PyObject * value) {
       splv = (SPL::uint64) PyLong_AsUnsignedLong(value);
    }

    // boolean
    inline void pySplValueFromPyObject(SPL::boolean & splv, PyObject * value) {
       splv = PyObject_IsTrue(value);
    }
 
    // floats
    inline void pySplValueFromPyObject(SPL::float32 & splv, PyObject * value) {
       splv = (SPL::float32) PyFloat_AsDouble(value);
    }
    inline void pySplValueFromPyObject(SPL::float64 & splv, PyObject * value) {
       splv = PyFloat_AsDouble(value);
    }

    // complex
    inline void pySplValueFromPyObject(SPL::complex32 & splv, PyObject * value) {
        splv = SPL::complex32(
          (SPL::float32) PyComplex_RealAsDouble(value),
          (SPL::float32) PyComplex_ImagAsDouble(value)
        );
    }
    inline void pySplValueFromPyObject(SPL::complex64 & splv, PyObject * value) {
        splv = SPL::complex64(
          (SPL::float64) PyComplex_RealAsDouble(value),
          (SPL::float64) PyComplex_ImagAsDouble(value)
        );
    }

    // SPL list from Python list
    template <typename T>
    inline void pySplValueFromPyObject(SPL::list<T> & l, PyObject *value) {
        const Py_ssize_t size = PyList_Size(value);

        for (Py_ssize_t i = 0; i < size; i++) {
            T se;
            l.add(se); // Add takes a copy of the value

            PyObject * e = PyList_GET_ITEM(value, i);
            pySplValueFromPyObject(l.at(i), e);
        }
    }
 
    // SPL set from Python set
    template <typename T>
    inline void pySplValueFromPyObject(SPL::set<T> & s, PyObject *value) {
        // validates that value is a Python set
        const Py_ssize_t size = PySet_Size(value);

        PyObject * iterator = PyObject_GetIter(value);
        if (iterator == 0) {
            throw SplpyGeneral::pythonException("iter(set)");
        }
        PyObject *item;
        while (item = PyIter_Next(iterator)) {
            T se;
            pySplValueFromPyObject(se, item);
            Py_DECREF(item);
            s.add(se);
        }
        Py_DECREF(iterator);
    }

    // SPL map from Python dictionary
    template <typename K, typename V>
    inline void pySplValueFromPyObject(SPL::map<K,V> & m, PyObject *value) {
        PyObject *k,*v;
        Py_ssize_t pos = 0;
        while (PyDict_Next(value, &pos, &k, &v)) {
           K sk;

           // Set the SPL key
           pySplValueFromPyObject(sk, k);

           // map[] creates the value if it does not exist
           V & sv = m[sk];
 
           // Set the SPL value 
           pySplValueFromPyObject(sv, v);
        }
    }

    /**************************************************************/

    /*
    ** Conversion of SPL attributes or value to Python objects
    */

    /**
     * Integer conversions.
    */

    inline PyObject * pySplValueToPyObject(const SPL::int32 & value) {
      return PyLong_FromLong(value);
    }
    inline PyObject * pySplValueToPyObject(const SPL::int64 & value) {
      return PyLong_FromLong(value);
    }
    inline PyObject * pySplValueToPyObject(const SPL::uint32 & value) {
      return PyLong_FromUnsignedLong(value);
    }
    inline PyObject * pySplValueToPyObject(const SPL::uint64 & value) {
      return PyLong_FromUnsignedLong(value);
    }

    /**
     * Convert a SPL blob into a Python Memory view object.
     */
    inline PyObject * pySplValueToPyObject(const SPL::blob & value) {
      long int sizeb = value.getSize();
      const unsigned char * bytes = value.getData();

#if PY_MAJOR_VERSION == 3
      return PyMemoryView_FromMemory((char *) bytes, sizeb, PyBUF_READ);
#else
      return PyBuffer_FromMemory((void *)bytes, sizeb);
#endif
    }

    /**
     * Convert a SPL rstring into a Python Unicode string 
     */
    inline PyObject * pySplValueToPyObject(const SPL::rstring & value) {
      long int sizeb = value.size();
      const char * pybytes = value.data();

      return PyUnicode_DecodeUTF8(pybytes, sizeb, NULL);
    }
    /**
     * Convert a SPL ustring into a Python Unicode string 
     */
    inline PyObject * pySplValueToPyObject(const SPL::ustring & value) {
      long int sizeb = value.length() * 2; // Need number of bytes
      const char * pybytes =  (const char*) (value.getBuffer());

      return PyUnicode_DecodeUTF16(pybytes, sizeb, NULL, NULL);
    }

    inline PyObject * pySplValueToPyObject(const SPL::complex32 & value) {
       return PyComplex_FromDoubles(value.real(), value.imag());
    }
    inline PyObject * pySplValueToPyObject(const SPL::complex64 & value) {
       return PyComplex_FromDoubles(value.real(), value.imag());
    }

    inline PyObject * pySplValueToPyObject(const SPL::boolean & value) {
       PyObject * pyValue = SplpyGeneral::getBool(value);
       return pyValue;
    }
    inline PyObject * pySplValueToPyObject(const SPL::float32 & value) {
       return PyFloat_FromDouble(value);
    }
    inline PyObject * pySplValueToPyObject(const SPL::float64 & value) {
       return PyFloat_FromDouble(value);
    }

    /**
     * Convert a PyObject to a PyObject by simply returning the value
     * nb. that if object has it ref count decremented to 0 the 
     * "copied" pointer is no longer valid
     */
    inline PyObject * pySplValueToPyObject(PyObject * object) {
      return object;
    }

    /*
    ** SPL List Conversion to Python list
    */
    template <typename T>
    inline PyObject * pySplValueToPyObject(const SPL::list<T> & l) {
        PyObject * pyList = PyList_New(l.size());
        for (int i = 0; i < l.size(); i++) {
            PyList_SET_ITEM(pyList, i, pySplValueToPyObject(l[i]));
        }
        return pyList;
    }

    /*
    ** SPL Map Conversion to Python dict.
    */
    template <typename K, typename V>
    inline PyObject * pySplValueToPyObject(const SPL::map<K,V> & m) {
        PyObject * pyDict = PyDict_New();
        for (typename std::tr1::unordered_map<K,V>::const_iterator it = m.begin();
             it != m.end(); it++) {
             PyObject *k = pySplValueToPyObject(it->first);
             PyObject *v = pySplValueToPyObject(it->second);
             PyDict_SetItem(pyDict, k, v);
             Py_DECREF(k);
             Py_DECREF(v);
        }
      
        return pyDict;
    }

    /*
    ** SPL Set Conversion to Python set
    */
    template <typename T>
    inline PyObject * pySplValueToPyObject(const SPL::set<T> & s) {
        PyObject * pySet = PySet_New(NULL);
        for (typename std::tr1::unordered_set<T>::const_iterator it = s.begin();
             it != s.end(); it++) {
             PyObject * e = pySplValueToPyObject(*it);
             PySet_Add(pySet, e);
             Py_DECREF(e);
        }
        return pySet;
    }

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

    // Python hash of an SPL attribute
    template <class T>
    static SPL::int32 pyTupleHash(PyObject * function, T & splVal) {

      SplpyGIL lock;

      PyObject * arg = pySplValueToPyObject(splVal);

      // invoke python nested function that generates the int32 hash
      // clear any indication of an error and then check later for an 
      // error
      PyErr_Clear();
      PyObject * pyReturnVar = pyTupleFunc(function, arg); 
     
       // construct integer from  return value
      SPL::int32 retval=0;
      try {
      	retval = PyLong_AsLong(pyReturnVar);
      } catch(...) {
        Py_DECREF(pyReturnVar);
        SplpyGeneral::flush_PyErr_Print();
        throw;
      }
      // PyLong_AsLong will return an error without 
      // throwing an error, so check if an error happened
      if (PyErr_Occurred()) {
        SplpyGeneral::flush_PyErr_Print();
      }
      else {
        Py_DECREF(pyReturnVar);
      }
      return retval;
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
