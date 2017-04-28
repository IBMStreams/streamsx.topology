/**
 * SPI for adding extensions to the functional api.
 * 
 * Invocation of SPL Java primitive operators that use a functional
 * style.
 * 
 * Invocation of an operator can receive a configuration JSON object that supports
 * the following fields.
 * 
 * name: name of the operator invocation
 * outputs: array of outputs
 *     name: name of output
 * sourcelocation: array of source location objects containing the following (all optional).
 *     file
 *     class
 *     method
 *     line
 */
package com.ibm.streamsx.topology.spi;