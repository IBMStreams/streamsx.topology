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
 * Functionality related to operators
 */

#ifndef __SPL__SPLPY_FUNCOP_H
#define __SPL__SPLPY_FUNCOP_H

#include "splpy_general.h"
#include "splpy_setup.h"
#include "splpy_op.h"

#include <SPL/Runtime/Operator/ParameterValue.h>
#include <SPL/Runtime/Operator/OperatorContext.h>
#include <SPL/Runtime/Operator/Operator.h>

namespace streamsx {
  namespace topology {

class SplpyFuncOp : public SplpyOp {
  public:
      PyObject *function_;

      SplpyFuncOp(SPL::Operator * op, const std::string & wrapfn) :
         SplpyOp(op, "/opt/python/packages/streamsx/topology"),
         function_(NULL)
      {
         addAppPythonPackages();
         //loadAndWrapCallable(wrapfn);
      }
 
      ~SplpyFuncOp() {
          SplpyGILLock lock;
          if (function_)
             Py_DECREF(function_);
      }

  private:

      int hasParam(const char *name) {
          return op()->getParameterNames().count(name);
      }

      const SPL::rstring & param(const char *name) {
          return op()->getParameterValues(name)[0]->getValue();
      }

      /**
       * Load and wrap the callable that will be invoked
       * by the operator.
      */
      void loadAndWrapCallable(const std::string & wrapfn) {
          SplpyGILLock lock;

          // pointer to the application function or callable class
          PyObject * appCallable =
             SplpyGeneral::loadFunction(param("pyModule"), param("pyName"));

          // The object to be called is either appCallable for
          // a function passed into the operator
          // or a pickled encoded class instance
          // represented as a string in parameter pyCallable
    
          if (hasParam("pyCallable")) {
             // argument is the serialized callable instance
             Py_DECREF(appCallable);
             appCallable = pyUnicode_FromUTF8(param("pyCallable").c_str());
          }

          PyObject * depickleInput = SplpyGeneral::loadFunction("streamsx.topology.runtime", wrapfn);
          PyObject * funcArg = PyTuple_New(1);
          PyTuple_SET_ITEM(funcArg, 0, appCallable);
          function_ = PyObject_CallObject(depickleInput, funcArg);
          Py_DECREF(depickleInput);
          Py_DECREF(funcArg);

          if (function_ == NULL){
            SplpyGeneral::flush_PyErr_Print();
            throw;
          }
          Py_DECREF(appCallable);
      }

      /*
       *  Add any packages in the application directory
       *  to the Python path. The application directory
       *  is passed to each invocation of the functional 
       *  operators as the parameter toolkitDir. The value
       *  passed is the toolkit of the invocation of the operator.
       */
      void addAppPythonPackages() {

          std::string appDirSetup = "import streamsx.topology.runtime\n";
          appDirSetup += "streamsx.topology.runtime.setupOperator(\"";
          appDirSetup += param("toolkitDir");
          appDirSetup += "\")\n";

          const char* spl_setup_appdir = appDirSetup.c_str();

          SPLAPPTRC(L_DEBUG, "Executing setupOperator: " << appDirSetup , "python");

          SplpyGILLock lock;
          if (PyRun_SimpleString(spl_setup_appdir) != 0) {
              SPLAPPTRC(L_ERROR, "Python streamsx.topology.runtime.setupOperator failed!", "python");
              throw SplpyGeneral::pythonException("streamsx.topology.runtime.setupOperator");
          }
          SPLAPPTRC(L_DEBUG, "Executing setupOperator complete." , "python");
      }
};

}}

#endif

