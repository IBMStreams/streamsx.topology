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

#include "splpy_op.h"
#include "splpy_cr.h"

#include <SPL/Runtime/Operator/ParameterValue.h>


namespace streamsx {
  namespace topology {

class SplpyFuncOp : public SplpyOp {
  public:

      SplpyFuncOp(SPL::Operator * op, bool needStateHandler,  const std::string & wrapfn) :
        SplpyOp(op, "/opt/python/packages/streamsx/topology")
      {
         setSubmissionParameters();
         addAppPythonPackages();
         loadAndWrapCallable(needStateHandler, wrapfn);
      }

      virtual ~SplpyFuncOp() {}
      
  private:
      int hasParam(const char *name) {
          return op()->getParameterNames().count(name);
      }

      const SPL::rstring & param(const char *name) {
          return op()->getParameterValues(name)[0]->getValue();
      }

      /**
       * Set submission parameters. Note all functional operators
       * share the same submission parameters (they are topology wide)
       * and thus each operator will execute this code. They will
       * all have the same values.
       *
       * Note at this point all the parameters are rstring values,
       * the Python code does any type conversion.
      */
      void setSubmissionParameters() {
          if (hasParam("submissionParamNames")) {
              SplpyGIL lock;

              const SPL::Operator::ParameterValueListType& names = op()->getParameterValues("submissionParamNames");

              const SPL::Operator::ParameterValueListType& values = op()->getParameterValues("submissionParamValues");

              for (int i = 0; i < names.size(); i++) {
                  PyObject *n = pyUnicode_FromUTF8(names[i]->getValue());
                  PyObject *v = pyUnicode_FromUTF8(values[i]->getValue());

                  streamsx::topology::SplpyGeneral::callVoidFunction(
                        "streamsx.ec", "_set_submit_param", n, v);
              }
          }
      }

      /**
       * Load and wrap the callable that will be invoked
       * by the operator.
      */
      void loadAndWrapCallable(bool needStateHandler, const std::string & wrapfn) {
          SplpyGIL lock;

          // pointer to the application function or callable class
          PyObject * appCallable =
             SplpyGeneral::loadFunction(param("pyModule"), param("pyName"));

          // The object to be called is either appCallable for
          // a function passed into the operator
          // or a pickled encoded class instance
          // represented as a string in parameter pyCallable
    
          if (hasParam("pyCallable")) {
             // argument is the serialized callable instance
             PyObject * appClass = appCallable;
             appCallable = pyUnicode_FromUTF8(param("pyCallable").c_str());
             Py_DECREF(appClass);
          }

          PyObject *extraArg = NULL;
          if (op()->getNumberOfOutputPorts() == 1) {
              extraArg = streamsx::topology::Splpy::pyAttributeNames(
               op()->getOutputPortAt(0));
          }

          setCallable(SplpyGeneral::callFunction(
               "streamsx.topology.runtime", wrapfn, appCallable, extraArg));
          setup(needStateHandler);
      }

      /*
       *  Add any packages in the application directory
       *  to the Python path. The application directory
       *  is passed to each invocation of the functional 
       *  operators as the parameter toolkitDir. The value
       *  passed is the toolkit of the invocation of the operator.
       */
      void addAppPythonPackages() {
          SplpyGIL lock;

          PyObject * tkDir =
            streamsx::topology::pyUnicode_FromUTF8(param("toolkitDir"));

          SplpyGeneral::callVoidFunction(
              "streamsx._streams._runtime", "_setup_operator", tkDir, NULL);
      }

};

}}

#endif

