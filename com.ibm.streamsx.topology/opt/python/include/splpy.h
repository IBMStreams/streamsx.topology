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

#include "Python.h"
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <memory>
#include <dlfcn.h>
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

#if PY_MAJOR_VERSION == 3
#define TOPOLOGY_PYTHON_LIBNAME "libpython3.5m.so"
#else
#define TOPOLOGY_PYTHON_LIBNAME "libpython2.7.so"
#endif
    
#if PY_MAJOR_VERSION == 3
#define GET_PYTHON_ATTR_FROM_MEMORY(pbytes,psizeb)                       \
     PyMemoryView_FromMemory((char *) pbytes, psizeb, PyBUF_READ);
#else
#define GET_PYTHON_ATTR_FROM_MEMORY(pbytes,psizeb)                       \
     PyBuffer_FromMemory((void *)bytes, sizeb);
#endif

namespace streamsx {
  namespace topology {

    class PyGILLock {
      public:
        PyGILLock() {
          gstate_ = PyGILState_Ensure();
        }
        ~PyGILLock() {
          PyGILState_Release(gstate_);
        }
        
      private:
        PyGILState_STATE gstate_;
    };

    /*
    * Flush Python stderr and stdout.
    */
    inline void flush_PyErrPyOut() {
        PyRun_SimpleString("sys.stdout.flush()");
        PyRun_SimpleString("sys.stderr.flush()");
    }

    /*
    * Call PyErr_Print() and then flush stderr.
    * This is because CPython buffers stderr (and stdout)
    * when it is not connected to a terminal.
    * Without the flush output is lost in distributed
    * (since stderr is conncted to a file) and
    * makes diagnosing errors impossible.
    */
    inline void flush_PyErr_Print() {
        PyErr_Print();
        flush_PyErrPyOut();
    }

    /*
    ** Convert to a SPL rstring from a Python string object.
    ** Returns 0 if successful, non-zero if error.
    */
    inline int pyRStringFromPyObject(SPL::rstring & attr, PyObject * value) {
      Py_ssize_t size = 0;
      PyObject * converted = NULL;
      char * bytes = NULL;

#if PY_MAJOR_VERSION == 3
      // Python 3 character strings are unicode objects
      // Caller is not responsible for deallocating buffer
      // returned by PyUnicode_AsUTF8AndSize
      //
      if (!PyUnicode_Check(value)) {
          // Create a string from the object
          value = converted = PyObject_Str(value);
      }
      bytes = PyUnicode_AsUTF8AndSize(value, &size);
#else
      // Python 2 supports Unicode and byte character strings 
      // Default is byte character strings.
      // PyString_AsStringAndSize returns a pointer to an
      // internal buffer that must not be modified or deallocated
      if (PyUnicode_Check(value)) {
          value = converted = PyUnicode_AsUTF8String(value);
      } else if (PyString_Check(value)) {
           // no coversion needed
      } else {
          // Create a string from the object
          value = converted = PyObject_Str(value);
      }
      int rc = PyString_AsStringAndSize(value, &bytes, &size);
      if (rc != 0)
          bytes = NULL;
#endif

      if (bytes == NULL) {
         if (converted != NULL)
             Py_DECREF(converted);
         return -1;
      }

      // This copies from bytes into the rstring.
      attr.assign((const char *)bytes, (size_t) size);

      // Need to decrement the reference after we
      // have copied the bytes out as bytes points
      // into the Python object.
      if (converted != NULL)
          Py_DECREF(converted);

      return 0;
    }

    /**
     * Return an SPL runtime exception that can be thrown
     * based upon the Python error. Also flushes Python stdout
     * and stderr to ensure any additional info is visible
     * in the PE console.
    */
    inline SPL::SPLRuntimeException pythonException(std::string const & 	location) {
      PyObject *pyType, *pyValue, *pyTraceback;
      PyErr_Fetch(&pyType, &pyValue, &pyTraceback);

      if (pyType != NULL)
          Py_DECREF(pyType);
      if (pyTraceback != NULL)
          Py_DECREF(pyTraceback);

      SPL::rstring msg("Unknown Python error");
      if (pyValue != NULL) {
          streamsx::topology::pyRStringFromPyObject(msg, pyValue);
          Py_DECREF(pyValue);
      }

      streamsx::topology::flush_PyErr_Print();

      SPL::SPLRuntimeOperatorException exc(location, msg);
      
      return exc;
    }

    /*
    ** Conversion of Python objects to SPL values.
    */
    //inline PyObject * pySplValueToPyObject(const SPL::int32 & value) {

    /*
    ** Convert to a SPL blob from a Python bytes object.
    */
    inline void pySplValueFromPyObject(SPL::blob & splv, PyObject * value) {
      long int size = PyBytes_Size(value);
      char * bytes = PyBytes_AsString(value);          
      splv.setData((const unsigned char *)bytes, size);
    }

    /*
    ** Convert to a SPL rstring from a Python string object.
    */
    inline void pySplValueFromPyObject(SPL::rstring & splv, PyObject * value) {
      if (pyRStringFromPyObject(splv, value) != 0) {
         SPLAPPTRC(L_ERROR, "Python can't convert to UTF-8!", "python");
         throw streamsx::topology::pythonException("rstring");
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
            throw streamsx::topology::pythonException("iter(set)");
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

      return GET_PYTHON_ATTR_FROM_MEMORY(bytes, sizeb);
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
       PyObject * pyValue = value ? Py_True : Py_False;
       Py_INCREF(pyValue);
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
      /**
       * Load the C Python runtime and execute a setup
       * script splpy_setup.py at the given path.
      */
      static void loadCPython(const char* spl_setup_py) {
      	// If the Python runtime is being embedded in a shared library
      	// (as is the case with IBM Streams), there is a bug where the 
      	// symbols from libpython*.*.so are not resolved properly. As
      	// as workaround, it's necessary to manually rediscover the
      	// symbols by calling dlopen().
      	//

        // Provide info on the setting of LD_LIBRARY_PATH
        char * pyLDD = getenv("LD_LIBRARY_PATH");
        if (pyLDD != NULL) {
            SPLAPPLOG(L_INFO, TOPOLOGY_LD_LIB_PATH(pyLDD), "python");
        } else {
            SPLAPPLOG(L_INFO, TOPOLOGY_LD_LIB_PATH_NO, "python");
        }

        // declare pylib and its value  
        std::string pyLib(TOPOLOGY_PYTHON_LIBNAME);
        char * pyHome = getenv("PYTHONHOME");
        if (pyHome != NULL) {
            std::string wk(pyHome);
            wk.append("/lib/");
            wk.append(pyLib);

            pyLib = wk;
        }
        SPLAPPLOG(L_INFO, TOPOLOGY_LOAD_LIB(pyLib), "python");

        if(NULL == dlopen(pyLib.c_str(), RTLD_LAZY |
                                         RTLD_GLOBAL)){
          SPLAPPLOG(L_ERROR, TOPOLOGY_LOAD_LIB_ERROR(pyLib), "python");
          throw;
        }

        if (Py_IsInitialized() == 0) {
          Py_InitializeEx(0);
          PyEval_InitThreads();
          PyEval_SaveThread();
        }

        int fd = open(spl_setup_py, O_RDONLY);
        if (fd < 0) {
          SPLAPPTRC(L_ERROR,
            "Python script splpy_setup.py not found!:" << spl_setup_py,
                             "python");
          throw;
        }
        PyGILLock lock;
        if (PyRun_SimpleFileEx(fdopen(fd, "r"), spl_setup_py, 1) != 0) {
          SPLAPPTRC(L_ERROR, "Python script splpy_setup.py failed!", "python");
          throw streamsx::topology::pythonException("splpy_setup.py");
        }
      }

    /*
     * Import a module, returning the reference to the module.
     * Caller must hold the GILState
     */
    static PyObject * importModule(const char * moduleNameC) 
    {
      PyObject * moduleName = PyUnicode_FromString(moduleNameC);
      PyObject * module = PyImport_Import(moduleName);
      Py_DECREF(moduleName);
      if (module == NULL) {
        SPLAPPLOG(L_ERROR, TOPOLOGY_IMPORT_MODULE_ERROR(moduleNameC), "python");
        throw streamsx::topology::pythonException(moduleNameC);
      }
      SPLAPPLOG(L_INFO, TOPOLOGY_IMPORT_MODULE(moduleNameC), "python");
      return module;
    }

    /*
     * Load a function, returning the reference to the function.
     * Caller must hold the GILState
     */
    static PyObject * loadFunction(const char * moduleNameC, const char * functionNameC)
     {    
       PyObject * module = importModule(moduleNameC);
       PyObject * function = PyObject_GetAttrString(module, functionNameC);
       Py_DECREF(module);
    
       if (!PyCallable_Check(function)) {
         SPLAPPTRC(L_ERROR, "Fatal error: function " << functionNameC << " in module " << moduleNameC << " not callable", "python");
         throw;
        }
        SPLAPPTRC(L_INFO, "Callable function: " << functionNameC, "python");
        return function;
      }

    /*
    * One off generic call a function by name passing one or two arguments
    * returning its return. Used for setup calls in operator constructors
    * as the reference to the method is not kept, assuming it is not
    * called frequently.
    * References to the arguments are stolen by this function.
    */
    static PyObject * callFunction(const char * module, const char *name, PyObject * arg1, PyObject * arg2) {
         PyObject *fn = loadFunction(module, name);

         PyObject *tc = PyTuple_New(arg2 == NULL ? 1 : 2);
         PyTuple_SET_ITEM(tc, 0, arg1);
         if (arg2 != NULL)
             PyTuple_SET_ITEM(tc, 1, arg2);

         PyObject * ret = PyObject_Call(fn, tc, NULL);

         Py_DECREF(tc);
         Py_DECREF(fn);

         return ret;
    }

    // Call the function passing an SPL attribute
    // converted to a Python object and discard the return 
    template <class T>
    static void pyTupleSink(PyObject * function, T & splVal) {
      PyGILLock lock;

      PyObject * arg = pySplValueToPyObject(splVal);

      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      if(pyReturnVar == 0){
        throw streamsx::topology::pythonException("sink");
      }

      Py_DECREF(pyReturnVar);
    }
    
    // prints a string representation of the PyObject for debugging purposes
    static void printPyObject(PyObject * pyObject) {
      PyObject* pyRepr = PyObject_Repr(pyObject);
      PyObject* pyStrBytes = PyUnicode_AsUTF8String(pyRepr);
      const char* s = PyBytes_AsString(pyStrBytes);
      std::cout << "pyObject=" << s << std::endl;
      Py_DECREF(pyStrBytes);
      Py_DECREF(pyRepr);
    }

    /*
    * Call a function passing the SPL attribute value of type T
    * and return the function return as a boolean
    */
    template <class T>
    static int pyTupleFilter(PyObject * function, T & splVal) {

      PyGILLock lock;

      PyObject * arg = pySplValueToPyObject(splVal);

      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      if(pyReturnVar == 0){
        throw streamsx::topology::pythonException("filter");
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
      PyGILLock lock;

      PyObject * arg = pySplValueToPyObject(splVal);

      // invoke python nested function that calls the application function
      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      if (pyReturnVar == Py_None){
        Py_DECREF(pyReturnVar);
        return 0;
      } else if(pyReturnVar == 0){
         throw streamsx::topology::pythonException("transform");
      } 

      pySplValueFromPyObject(retSplVal, pyReturnVar);
      Py_DECREF(pyReturnVar);

      return 1;
    }

    // Python hash of an SPL attribute
    template <class T>
    static SPL::int32 pyTupleHash(PyObject * function, T & splVal) {

      PyGILLock lock;

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
        streamsx::topology::flush_PyErr_Print();
        throw;
      }
      // PyLong_AsLong will return an error without 
      // throwing an error, so check if an error happened
      if (PyErr_Occurred()) {
        streamsx::topology::flush_PyErr_Print();
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
      PyTuple_SetItem(pyTuple, 0, value);

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
            std::string const & name = tt.getAttributeName(i);

            PyObject * pyName = PyUnicode_DecodeUTF8(
                           name.c_str(), name.size(), NULL);
            PyTuple_SetItem(pyNames, i, pyName);
       }
       return pyNames;
    }

    };
  }
}
#endif
