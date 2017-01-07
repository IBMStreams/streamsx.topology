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

namespace streamsx {
  namespace topology {

class SplpyFuncOp {
  public:
      static void initialize() {
          SplpySetup::loadCPython("/opt/python/packages/streamsx/topology");
      }
};

}}

#endif

