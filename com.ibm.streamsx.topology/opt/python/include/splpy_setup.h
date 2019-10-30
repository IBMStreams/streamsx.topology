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

#include "splpy_python.h"
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <memory>
#include <dlfcn.h>
#include <TopologySplpyResource.h>

#include <SPL/Runtime/Common/RuntimeException.h>
#include <SPL/Runtime/ProcessingElement/PE.h>

#include "splpy_sym.h"
#include "splpy_ec.h"

//#define __SPLPY_BUILD_VERS(_MAJOR, _MINOR) #_MAJOR "." #_MINOR
#define __SPLPY_STR(X) #X
#define __SPLPY_XSTR(X) __SPLPY_STR(X)

#define __SPLPY_MAJOR_VER __SPLPY_XSTR(PY_MAJOR_VERSION)
#define __SPLPY_MINOR_VER __SPLPY_XSTR(PY_MINOR_VERSION)

#define __SPLPY_VERSION __SPLPY_MAJOR_VER "." __SPLPY_MINOR_VER 
 
#define TOPOLOGY_PYTHON_LIBNAME "libpython" __SPLPY_VERSION "m.so"

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
        SplpySym::fixSymbols(pydl);
        startPython(pydl);
        setupErrors(pydl);
        setupNone(pydl);
        setupMemoryViewCheck(pydl);
        runSplSetup(pydl, spl_setup_py_path);
        setupClasses();
        return pydl;
    }
    static void setupErrors(void * pydl) {
        PyObject** ves = (PyObject**) dlsym(pydl, "PyExc_ValueError");
        SplpyErrors::ValueError = *ves;

        ves = (PyObject**) dlsym(pydl, "PyExc_StopIteration");
        SplpyErrors::StopIteration = *ves;
    }

    /*
     * Load 'None' dynamically to avoid a dependency
     * on the variable from libpythonX.Y.so.
     */
    static void setupNone(void * pydl) {
        typedef PyObject * (*__splpy_bv)(const char *, ...);

        SplpyGIL lock;

        // empty format returns None
        PyObject * none =
                ((__splpy_bv) dlsym(pydl, "Py_BuildValue"))("");
        
        // Call isNone() and getNone() passing in none which will
        // be the first caller (as this is in setup)
        // and thus set the local pointer to None (effectively Py_None).
        bool in = SplpyGeneral::isNone(none);
        if (!in) {
          throw SplpyGeneral::generalException("setup",
                        "Internal error - None handling: isNone");
        }
        PyObject * ret = SplpyGeneral::getNone(none);
        if (ret != none) {
          throw SplpyGeneral::generalException("setup",
                        "Internal error - None handling: getNone");
        }
        Py_DECREF(ret);
    }

    /*
     *  Load 'PyMemoryView_Type' dynamically for
     *  our our checkmemoryview.
     */
    static void setupMemoryViewCheck(void * pydl) {

        PyObject *mvta = (PyObject *) dlsym(pydl, "PyMemoryView_Type");

        // Call the checkMemoryView passing in none which will
        // be the first caller (as this is in setup)
        // and thus set the local pointer to &PyMemoryView_Type
        bool notmv = SplpyGeneral::checkMemoryView(mvta);
        if (notmv) {
          // Since the type of memoryview is a type, not
          // a memoryview then the call should be false.
          throw SplpyGeneral::generalException("setup",
                        "Internal error - Memoryview_Check handling");
        }
    }

   static void setupClasses() {
       SplpyGIL lock;
       SplpyGeneral::timestampClass(
          SplpyGeneral::loadFunction("streamsx.spl.types", "Timestamp"));
       SplpyGeneral::timestampGetter(
          SplpyGeneral::loadFunction("streamsx.spl.types", "_get_timestamp_tuple"));
       SplpyGeneral::decimalClass(
          SplpyGeneral::loadFunction("decimal", "Decimal"));
   }

  private:
    static void * loadPythonLib() {

        std::string pyLibName(TOPOLOGY_PYTHON_LIBNAME);
        const char * pyHome = getenv("PYTHONHOME");
        if (pyHome != NULL) {
            SPLAPPLOG(L_INFO, TOPOLOGY_PYTHONHOME(pyHome), "python");
        } else {
          std::string errtxt(TOPOLOGY_PYTHONHOME_NO(__SPLPY_VERSION));

          SPLAPPLOG(L_ERROR, errtxt, "python");

          // Can't use generalException as that calls into Python
          SPL::SPLRuntimeOperatorException exc("setup", errtxt);
          throw exc;
        }
 
#if PY_MAJOR_VERSION == 3
        // When SPL compile is optimized disable Python
        // assertions, equivalent to -O
        // Seems to cause module loading issues from pyc files
        // on Python 2.7 so only optmize on Python 3
        if (SPL::ProcessingElement::pe().isOptimized()) {
           if (getenv("PYTHONOPTIMIZE") == NULL) {
               SPLAPPTRC(L_DEBUG, "Setting optimized Python runtime (-O)", "python");
               setenv("PYTHONOPTIMIZE", "1", 1);
          }
        }
#endif
        std::string wk(pyHome); wk.append("/lib/");
        std::string wk64(pyHome); wk64.append("/lib64/");
        std::string pyLib = wk + pyLibName;
        struct stat st;
        if (stat(pyLib.c_str(), &st) != 0) {
               pyLib = wk64 + pyLibName;
        }

#if PY_MAJOR_VERSION == 3
        /**
         * With Python 3 we compile to the Stable ABI.
         * We first look (above) for libpython3.Xm.so with
         * 3.X matching the Python environment when the SPL
         * application was compiled (e.g. 3.6).
         *
         * If a matching library does not exist then
         * search for libaries from 3.9 to 3.X (exclusive).
         */
        if ((stat(pyLib.c_str(), &st) == -1) && errno == ENOENT)
        {
            // Remove Nm.so
            std::string base = pyLibName.substr(0, pyLibName.size()-4);
            base = base.substr(0, base.find_last_of('.')+1);
            // Only release schedules for up to 3.9 as of Aug2019
            for (int mv = 9; mv > PY_MINOR_VERSION; mv--) {
                std::ostringstream mvs;
                mvs << mv;
                std::string possLibName = base + mvs.str() + "m.so";
                std::string possLib = wk + possLibName;
                if (stat(possLib.c_str(), &st) == 0) {
                    pyLib = possLib;
                    break;
                }
                possLib = wk64 + possLibName;
                if (stat(possLib.c_str(), &st) == 0) {
                    pyLib = possLib;
                    break;
                }
            }
        }
#endif
        SPLAPPLOG(L_INFO, TOPOLOGY_LOAD_LIB(pyLib), "python");

        void * pydl = dlopen(pyLib.c_str(),
                         RTLD_LAZY | RTLD_GLOBAL | RTLD_DEEPBIND);

        if (NULL == pydl) {
          const char * dle = dlerror();
          std::string dles(dle == NULL ? "" : dle);

          std::string errtxt(TOPOLOGY_LOAD_LIB_ERROR(pyLib, __SPLPY_VERSION, dles));
          SPLAPPLOG(L_ERROR, errtxt, "python");

          // Can't use generalException as that calls into Python
          SPL::SPLRuntimeOperatorException exc("setup", errtxt);
          throw exc;
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
#if PY_MAJOR_VERSION == 3
          typedef void (*__splpy_ssae)(int, wchar_t**, int);
#else
          typedef void (*__splpy_ssae)(int, char**, int);
#endif
          typedef void (*__splpy_eit)(void);
          typedef PyThreadState * (*__splpy_est)(void);


{

#if PY_MAJOR_VERSION == 3
          SPLAPPTRC(L_DEBUG, "Including Python extension: _streamsx_ec", "python");
          typedef PyObject *(__splpy_initfunc)(void);
          typedef int (*__splpy_iai)(const char * name, __splpy_initfunc);
          __splpy_iai _SPLPyImport_AppendInittab =
             (__splpy_iai) dlsym(pydl, "PyImport_AppendInittab");
          _SPLPyImport_AppendInittab(__SPLPY_EC_MODULE_NAME, &init_streamsx_ec);
#endif
}

          SPLAPPTRC(L_DEBUG, "Starting Python runtime", "python");

           
          __splpy_ie _SPLPy_InitializeEx =
             (__splpy_ie) dlsym(pydl, "Py_InitializeEx");

          __splpy_ssae _SPLPySys_SetArgvEx =
             (__splpy_ssae) dlsym(pydl, "PySys_SetArgvEx");

          __splpy_eit _SPLPyEval_InitThreads =
             (__splpy_eit) dlsym(pydl, "PyEval_InitThreads");

          __splpy_est _SPLPyEval_SaveThread =
             (__splpy_est) dlsym(pydl, "PyEval_SaveThread");

          _SPLPy_InitializeEx(0);
#if PY_MAJOR_VERSION == 3
          const wchar_t *argv[] = {L""};
          _SPLPySys_SetArgvEx(1, (wchar_t **) argv, 0);
#else
          const char *argv[] = {""};
          _SPLPySys_SetArgvEx(1, (char **) argv, 0);
#endif
#if PY_MAJOR_VERSION == 2
          SPLAPPTRC(L_DEBUG, "Including Python extension: _streamsx_ec", "python");
          init_streamsx_ec();
#endif
         
          _SPLPyEval_InitThreads();
          _SPLPyEval_SaveThread();

        } else {
          SPLAPPTRC(L_DEBUG, "Python runtime already started", "python");
        }
        SPLAPPTRC(L_INFO, "Started Python runtime", "python");
    }

    static void runSplSetup(void * pydl, const char* spl_setup_py_path) {
        std::string tkDir = SPL::ProcessingElement::pe().getToolkitDirectory();
        std::string streamsxDir = tkDir + spl_setup_py_path;
        std::string splpySetup = streamsxDir + "/splpy_setup.py";
        const char* spl_setup_py = splpySetup.c_str();

        SPLAPPTRC(L_DEBUG, "Python script splpy_setup.py: " << spl_setup_py, "python");

        int fd = open(spl_setup_py, O_RDONLY);
        if (fd < 0) {
          std::stringstream msg;
          msg << "Internal Error: Python script splpy_setup.py not found!:" << splpySetup;
          throw SplpyGeneral::generalException("splpy_setup.py", msg.str());
        }

        typedef int (*__splpy_rsfef)(FILE *, const char *, int, PyCompilerFlags *);
        __splpy_rsfef _SPLPyRun_SimpleFileEx = 
             (__splpy_rsfef) dlsym(pydl, "PyRun_SimpleFileExFlags");

        SplpyGIL lock;
        // The 1 closes the file.
        if (_SPLPyRun_SimpleFileEx(fdopen(fd, "r"), spl_setup_py, 1, NULL) != 0) {
          SPLAPPTRC(L_ERROR, "Python script splpy_setup.py failed!", "python");
          throw SplpyGeneral::pythonException("splpy_setup.py");
        }
        SPLAPPTRC(L_DEBUG, "Python script splpy_setup.py ran ok.", "python");

        SplpyGeneral::callVoidFunction("streamsx.ec", "_setup", NULL, NULL);
    }
};

}}

#endif

