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

#ifndef __SPL__SPLPY_OP_H
#define __SPL__SPLPY_OP_H

#include "spl_general.h"

namespace streamsx {
  namespace topology {

class SplpyOp {
  public:
      /**
       * Actions for a Python operator on prepareToShutdown
       * Flush any pending output.
      */
      static void prepareToShutdown() {
          PyGILLock lock;
          SplpyGeneral::flush_PyErrPyOut();
      }
};

}}

#endif

