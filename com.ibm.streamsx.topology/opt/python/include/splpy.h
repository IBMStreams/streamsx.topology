#include "Python.h"
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <memory>

#include <SPL/Runtime/Operator/Operator.h>
#include <SPL/Runtime/Operator/OperatorContext.h>
#include <SPL/Runtime/ProcessingElement/PE.h>

/**
 * Functionality for executing Python within IBM Streams.
 */

namespace streamsx {
  namespace topology {
    class Splpy {
      public:
      /**
       * Load the C Python runtime and execute a setup
       * script splpy_setup.py at the given path.
      */
      static void loadCPython(const char* spl_setup_py) {
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
       PyGILState_STATE gstate;
       gstate = PyGILState_Ensure();
       if (PyRun_SimpleFileEx(fdopen(fd, "r"), spl_setup_py, 1) != 0) {
         PyGILState_Release(gstate);
         SPLAPPTRC(L_ERROR, "Python script splpy_setup.py failed!", "python");
         PyErr_Print();
         throw;
       }
       PyGILState_Release(gstate);
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
        PyErr_Print();
        SPLAPPLOG(L_ERROR, "Fatal error: missing module: " << moduleNameC, "python");
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
           SPLAPPLOG(L_ERROR, "Fatal error: function not callable: " << functionNameC, "python");
         throw;
        }
        SPLAPPTRC(L_INFO, "Callable function: " << functionNameC, "python");
        return function;
      }
    static void pyTupleSink(PyObject * function, SPL::blob & pyblob) {

      PyGILState_STATE gstate;
      gstate = PyGILState_Ensure();

      PyObject * pyBytes  = pyBlobToBytes(pyblob);
      PyObject * pyReturnVar = pyTupleFunc(function, pyBytes);

      if(pyReturnVar == 0){
        PyErr_Print();
        PyGILState_Release(gstate);
        throw;
      }

      Py_DECREF(pyReturnVar);
      PyGILState_Release(gstate);

    }
    static void pyTupleSink(PyObject * function, SPL::rstring & pyrstring) {
      long int sizeb = pyrstring.length();
      const char * pybytes = pyrstring.data();

      PyGILState_STATE gstate;
      gstate = PyGILState_Ensure();

      PyObject * pyUnicode  = PyUnicode_FromStringAndSize(pybytes, sizeb);
      PyObject * pyReturnVar = pyTupleFunc(function, pyUnicode);

      if(pyReturnVar == 0){
        PyErr_Print();
        PyGILState_Release(gstate);
        throw;
      }

      Py_DECREF(pyReturnVar);
      PyGILState_Release(gstate);

    }

    static int pyTupleFilter(PyObject * function, SPL::blob & pyblob) {

      PyGILState_STATE gstate;
      gstate = PyGILState_Ensure();

      PyObject * pyBytes  = pyBlobToBytes(pyblob);
      PyObject * pyReturnVar = pyTupleFunc(function, pyBytes);

      if(pyReturnVar == 0){
        PyErr_Print();
        PyGILState_Release(gstate);
        throw;
      }

      int ret = PyObject_IsTrue(pyReturnVar);

      Py_DECREF(pyReturnVar);
      PyGILState_Release(gstate);

      return ret;
    }
    
    static std::auto_ptr<SPL::blob> pyTupleTransform(PyObject * function, PyObject * pickleObjectFunction, SPL::blob & pyblob) {

      std::auto_ptr<SPL::blob> ret;
      PyGILState_STATE gstate;
      gstate = PyGILState_Ensure();

      PyObject * pyBytes  = pyBlobToBytes(pyblob);
      PyObject * pyReturnVar = pyTupleFunc(function, pyBytes);

      if (pyReturnVar == Py_None){
        Py_DECREF(pyReturnVar);
        PyGILState_Release(gstate);
        return ret;
      } else if(pyReturnVar == 0){
        PyErr_Print();
        PyGILState_Release(gstate);
        throw;
      } 

      PyObject * pyPickledReturnVar = pyTupleFunc(pickleObjectFunction, pyReturnVar);
      long int size = PyBytes_Size(pyPickledReturnVar);
      char * bytes = PyBytes_AsString(pyPickledReturnVar);          
      ret.reset(new SPL::blob((const unsigned char *)bytes, size));
      Py_DECREF(pyPickledReturnVar);
      PyGILState_Release(gstate);
      return ret;
    }

    /**
     * Call a Python function passing in the SPL tuple as 
     * the single element of a Python tuple.
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

    };
  }
}
