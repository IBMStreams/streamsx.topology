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
    /*
     * Load embedded Python and execute the toolkit's
     * spl_setup.py script.
     * Argument is path (relative to the toolkit root) of
     * the location of spl_setup.py
     */
    static void loadCPython(const char* spl_setup_py_path) {
        startPython();
        runSplSetup(spl_setup_py_path);
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

    static void runSplSetup(const char* spl_setup_py_path) {
        std::string tkDir = SPL::ProcessingElement::pe().getToolkitDirectory();
        std::string streamsxDir = tkDir + spl_setup_py_path;
        std::string splpySetup = streamsxDir + "/splpy_setup.py";
        const char* spl_setup_py = splpySetup.c_str();

        SPLAPPTRC(L_DEBUG, "Python script splpy_setup.py: " << spl_setup_py, "python");

        int fd = open(spl_setup_py, O_RDONLY);
        if (fd < 0) {
          SPLAPPTRC(L_ERROR,
            "Python script splpy_setup.py not found!:" << spl_setup_py,
                             "python");
          throw;
        }

        PyGILLock lock;
        // The 1 closes the file.
        if (PyRun_SimpleFileEx(fdopen(fd, "r"), spl_setup_py, 1) != 0) {
          SPLAPPTRC(L_ERROR, "Python script splpy_setup.py failed!", "python");
          throw SplpyGeneral::pythonException("splpy_setup.py");
        }
        SPLAPPTRC(L_DEBUG, "Python script splpy_setup.py ran ok.", "python");
    }
};

}}

#endif

