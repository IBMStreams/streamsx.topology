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
 * Functionality related to general
 * non-operator, non-data processing functions.
 */

#ifndef __SPL__SPLPY_GENERAL_H
#define __SPL__SPLPY_GENERAL_H

#include "Python.h"

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

class SplpyGeneral {
  public:
    /*
     * Flush Python stderr and stdout.
    */
    static void flush_PyErrPyOut() {
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
    static void flush_PyErr_Print() {
        PyErr_Print();
        SplpyGeneral::flush_PyErrPyOut();
    }
};

}}

#endif
