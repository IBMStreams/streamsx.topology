#include "Python.h"
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <memory>
#include <dlfcn.h>

#include <SPL/Runtime/Operator/Operator.h>
#include <SPL/Runtime/Operator/OperatorContext.h>
#include <SPL/Runtime/ProcessingElement/PE.h>

/**
 * Functionality for executing Python within IBM Streams.
 */

    
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
    
    class Splpy {
      public:
      /**
       * Load the C Python runtime and execute a setup
       * script splpy_setup.py at the given path.
      */
      static void loadCPython(const char* spl_setup_py) {
	if(NULL == dlopen("libpython3.5m.so", RTLD_LAZY |
			  RTLD_GLOBAL)){
	  SPLAPPLOG(L_ERROR, "Fatal error: could not open libpython3.5m.so", "python");
	  flush_PyErr_Print();
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
         flush_PyErr_Print();
         throw;
       }
      }
    /*
    * Call PyErr_Print() and then flush stderr.
    * This is because CPython buffers stderr (and stdout)
    * when it is not connected to a terminal.
    * Without the flush output is lost in distributed
    * (since stderr is conncted to a file) and
    * makes diagnosing errors impossible.
    */
    static void flush_PyErr_Print() {
        PyErr_Print();
        PyRun_SimpleString("sys.stderr.flush()");
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
        SPLAPPLOG(L_ERROR, "Fatal error: missing module: " << moduleNameC, "python");
        flush_PyErr_Print();
        throw;
      }
      SPLAPPTRC(L_INFO, "Imported  module: " << moduleNameC, "python");
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
           SPLAPPLOG(L_ERROR, "Fatal error: function " << functionNameC << " in module " << moduleNameC << " not callable", "python");
         throw;
        }
        SPLAPPTRC(L_INFO, "Callable function: " << functionNameC, "python");
        return function;
      }

    // Call the function passing an SPL blob and
    // discard the return 
    static void pyTupleSink(PyObject * function, SPL::blob & pyblob) {

      PyGILLock lock;
      PyObject * pyBytes  = pyBlobToBytes(pyblob);

      _pyTupleSink(function, pyBytes);
    }

    // Call the function passing an SPL blob and
    // discard the return 
    static void pyTupleSink(PyObject * function, SPL::rstring & pyrstring) {

      PyGILLock lock;
      PyObject * pyString  = pyRstringToUnicode(pyrstring);

      _pyTupleSink(function, pyString);
    }

    // Call the function passing a PyObject and
    // discard the return 
    static void _pyTupleSink(PyObject * function, PyObject * arg) {
      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      if(pyReturnVar == 0){
        flush_PyErr_Print();
        throw;
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

    // Call the function passing an SPL blob and
    // treat the return as a boolean
    static int pyTupleFilter(PyObject * function, SPL::blob & pyblob) {

      PyGILLock lock;
      PyObject * pyBytes  = pyBlobToBytes(pyblob);

      return _pyTupleFilter(function, pyBytes);
    }
    //
    // Call the function passing an SPL rstring and
    // treat the return as a boolean
    static int pyTupleFilter(PyObject * function, SPL::rstring & pyrstring) {

      PyGILLock lock;
      PyObject * pyString  = pyRstringToUnicode(pyrstring);

      return _pyTupleFilter(function, pyString);
    }

    // Call the function passing a PyObject and
    // treat the return as a boolean
    static int _pyTupleFilter(PyObject * function, PyObject * arg) {

      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      if(pyReturnVar == 0){
        flush_PyErr_Print();
        throw;
      }

      int ret = PyObject_IsTrue(pyReturnVar);

      Py_DECREF(pyReturnVar);
      return ret;
    }
    
    // Call the function with argument converting the SPL blob
    // to a Python byte string and return the result as a blob
    static std::auto_ptr<SPL::blob> pyTupleTransform(PyObject * function, SPL::blob & pyblob) {

      PyGILLock lock;

      // convert spl blob to bytes
      PyObject * pyBytes  = pyBlobToBytes(pyblob);

      return _pyTupleTransform(function, pyBytes);
    }
    
    // Call the function with argument converting the SPL rstring
    // to a Python Unicode string and return the result as a blob
    static std::auto_ptr<SPL::blob> pyTupleTransform(PyObject * function, SPL::rstring & pyrstring) {

      PyGILLock lock;

      // convert spl rstring to Python Unicode String assuming UTF-8
      PyObject * pyString  = pyRstringToUnicode(pyrstring);

      return _pyTupleTransform(function, pyString);
    }

    // Call the function with argument and return the result as a blob
    static std::auto_ptr<SPL::blob> _pyTupleTransform(PyObject * function, PyObject * arg) {

      // invoke python nested function that calls the application function
      PyObject * pyReturnVar = pyTupleFunc(function, arg);

      std::auto_ptr<SPL::blob> ret;

      if (pyReturnVar == Py_None){
        Py_DECREF(pyReturnVar);
        return ret;
      } else if(pyReturnVar == 0){
        flush_PyErr_Print();
        throw;
      } 

      // construct spl blob from pickled return value
      long int size = PyBytes_Size(pyReturnVar);
      char * bytes = PyBytes_AsString(pyReturnVar);          
      ret.reset(new SPL::blob((const unsigned char *)bytes, size));
      Py_DECREF(pyReturnVar);
      return ret;
    }

    // Hash passing an SPL Blob
    static SPL::int32 pyTupleHash(PyObject * function, SPL::blob & pyblob) {
      PyGILLock lock;

      // convert spl blob to bytes
      PyObject * pyBytes  = pyBlobToBytes(pyblob);

      return _pyTupleHash(function, pyBytes);
    }
    //
    // Hash passing an SPL Blob
    static SPL::int32 pyTupleHash(PyObject * function, SPL::rstring & pyrstring) {
      PyGILLock lock;
 
      // convert spl rstring to Python Unicode String assuming UTF-8
      PyObject * pyString  = pyRstringToUnicode(pyrstring);

      return _pyTupleHash(function, pyString);
    }

    // Hash passing a PyObject *
    static SPL::int32 _pyTupleHash(PyObject * function, PyObject * arg) {
      // invoke python nested function that generates the int32 hash
      PyObject * pyReturnVar = pyTupleFunc(function, arg); 
     
       // construct integer from  return value
      SPL::int32 retval=0;
      try {
      	retval = PyLong_AsLong(pyReturnVar);
      } catch(...) {
        Py_DECREF(pyReturnVar);
        flush_PyErr_Print();
        throw;
      }  	 
      Py_DECREF(pyReturnVar);		
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
     * Convert a SPL blob into a Python Byte string 
     */
    static PyObject * pyBlobToBytes(SPL::blob & pyblob) {
      long int sizeb = pyblob.getSize();
      const unsigned char * pybytes = pyblob.getData();

      PyObject * pyBytes  = PyBytes_FromStringAndSize((const char *)pybytes, sizeb);
      return pyBytes;
    }

    /**
     * Convert a SPL rstring into a Python Unicode string 
     */
    static PyObject * pyRstringToUnicode(SPL::rstring & pyrstring) {
      long int sizeb = pyrstring.size();
      const char * pybytes = pyrstring.data();

      PyObject * pyString  = PyUnicode_FromStringAndSize(pybytes, sizeb);
      return pyString;
    }

    };
   
  }
}
