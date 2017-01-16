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

#include "splpy_sym.h"

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
    static void * loadCPython(const char* spl_setup_py_path) {
        void * pydl = loadPythonLib();
        setupGeneral(pydl);
        SplpySym::fixSymbols(pydl);
        startPython(pydl);
        runSplSetup(pydl, spl_setup_py_path);
        return pydl;
    }

  private:
    static void * loadPythonLib() {

        std::string pyLib(TOPOLOGY_PYTHON_LIBNAME);
        char * pyHome = getenv("PYTHONHOME");
        if (pyHome != NULL) {
            std::string wk(pyHome);
            wk.append("/lib/");
            wk.append(pyLib);

            pyLib = wk;
        }
        // Log & trace
        SPLAPPLOG(L_INFO, TOPOLOGY_LOAD_LIB(pyLib), "python");
        SPLAPPTRC(L_INFO, TOPOLOGY_LOAD_LIB(pyLib), "python");

        void * pydl = dlopen(pyLib.c_str(),
                         RTLD_LAZY | RTLD_GLOBAL | RTLD_DEEPBIND);

        if (NULL == pydl) {
          SPLAPPLOG(L_ERROR, TOPOLOGY_LOAD_LIB_ERROR(pyLib), "python");
          throw;
        }
        SPLAPPTRC(L_INFO, "Loaded Python library", "python");
        return pydl;
    }

    /**
     * Start the embedded Python runtime.
     * 
     * Py functions are accessed indirectly to allow
     * relocation (dynamic loading) of the Python runtime.
     */
    static void startPython(void *pydl) {
        SPLAPPTRC(L_DEBUG, "Checking Python runtime", "python");

        typedef int (*__splpy_ii)(void);

        __splpy_ii _SPLPy_IsInitialized =
             (__splpy_ii) dlsym(pydl, "Py_IsInitialized");


        if (_SPLPy_IsInitialized() == 0) {
          typedef void (*__splpy_ie)(int);
          typedef void (*__splpy_eit)(void);
          typedef PyThreadState * (*__splpy_est)(void);

          SPLAPPTRC(L_DEBUG, "Starting Python runtime", "python");

          __splpy_ie _SPLPy_InitializeEx =
             (__splpy_ie) dlsym(pydl, "Py_InitializeEx");

          __splpy_eit _SPLPyEval_InitThreads =
             (__splpy_eit) dlsym(pydl, "PyEval_InitThreads");

          __splpy_est _SPLPyEval_SaveThread =
             (__splpy_est) dlsym(pydl, "PyEval_SaveThread");

          _SPLPy_InitializeEx(0);
          _SPLPyEval_InitThreads();
          _SPLPyEval_SaveThread();

        } else {
          SPLAPPTRC(L_DEBUG, "Python runtime already started", "python");
        }
        SPLAPPTRC(L_INFO, "Started Python runtime", "python");
    }

    static void setupGeneral(void * pydl) {
        typedef PyObject * (*__splpy_bv)(const char *, ...);
        typedef PyObject * (*__splpy_bfl)(long);

        __splpy_bv _SPLPy_BuildValue =
             (__splpy_bv) dlsym(pydl, "Py_BuildValue");
        __splpy_bfl _SPLPyBool_FromLong =
             (__splpy_bfl) dlsym(pydl, "PyBool_FromLong");

        // empty format returns None
        PyObject * none = _SPLPy_BuildValue("");

        PyObject * f = _SPLPyBool_FromLong(0);
        PyObject * t = _SPLPyBool_FromLong(1);

        SplpyGeneral::setup(none, f, t);
    }

    static void runSplSetup(void * pydl, const char* spl_setup_py_path) {
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

        typedef int (*__splpy_rsfef)(FILE *, const char *, int, PyCompilerFlags *);
        __splpy_rsfef _SPLPyRun_SimpleFileEx = 
             (__splpy_rsfef) dlsym(pydl, "PyRun_SimpleFileExFlags");

        SplpyGILLock lock;
        // The 1 closes the file.
        if (_SPLPyRun_SimpleFileEx(fdopen(fd, "r"), spl_setup_py, 1, NULL) != 0) {
          SPLAPPTRC(L_ERROR, "Python script splpy_setup.py failed!", "python");
          throw SplpyGeneral::pythonException("splpy_setup.py");
        }
        SPLAPPTRC(L_DEBUG, "Python script splpy_setup.py ran ok.", "python");
    }
};

}}

#endif

