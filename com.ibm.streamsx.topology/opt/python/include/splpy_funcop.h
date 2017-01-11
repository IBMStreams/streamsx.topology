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
      SplpyFuncOp(SPL::Operator * op) :
         SplpyOp(op, "/opt/python/packages/streamsx/topology") {
         addAppPythonPackages();
      }

  private:

      /*
       *  Add any packages in the application directory which
       *  is passed to each invocation of the functional 
       *  operators as the paramter toolkitDir. The value
       *  passed is the toolkit of the invocation of the operator.
       */
      void addAppPythonPackages() {
          SplpyGILLock lock;

          std::string appDirSetup = "import streamsx.topology.runtime\n";
          appDirSetup += "streamsx.topology.runtime.setupOperator(\"";
          appDirSetup += static_cast<SPL::rstring>(op()->getParameterValues("toolkitDir")[0]->getValue());
          appDirSetup += "\")\n";

          const char* spl_setup_appdir = appDirSetup.c_str();
          if (PyRun_SimpleString(spl_setup_appdir) != 0) {
              SPLAPPTRC(L_ERROR, "Python streamsx.topology.runtime.setupOperator failed!", "python");
              throw SplpyGeneral::pythonException("streamsx.topology.runtime.setupOperator");
          }
      }
};

}}

#endif

