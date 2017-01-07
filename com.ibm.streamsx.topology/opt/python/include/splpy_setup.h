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
 * Functionality related to setting up
 * the Python VM and generic non-operator,
 * non-data processing functions.
 */


#ifndef __SPL__SPLPY_SETUP_H
#define __SPL__SPLPY_SETUP_H

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


#if PY_MAJOR_VERSION == 3
#define TOPOLOGY_PYTHON_LIBNAME "libpython3.5m.so"
#else
#define TOPOLOGY_PYTHON_LIBNAME "libpython2.7.so"
#endif
    
namespace streamsx {
  namespace topology {

class SplpySetup {
  public:
    static void loadCPython() {
        startPython();
    }

  private:
    /**
     * Start the embedded Python runtime.
     */
    static void startPython() {
        SPLAPPTRC(L_DEBUG, "Checking Python runtime", "python");

        if (Py_IsInitialized() == 0) {

          SPLAPPTRC(L_DEBUG, "Starting Python runtime", "python");

          Py_InitializeEx(0);
          PyEval_InitThreads();
          PyEval_SaveThread();

        } else {
          SPLAPPTRC(L_DEBUG, "Python runtime already started", "python");
        }
        SPLAPPTRC(L_INFO, "Started Python runtime", "python");
    }
};

}}

#endif

