/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology

/**
  * Implicit conversions for IBM Streams Scala applications.
  *
  * Importing [[com.ibm.streamsx.topology.functions.FunctionConversions]],
  * allows Scala functions to be used as functions used
  * to transform, filter tuples etc.
  * {{{
  * import com.ibm.streamsx.topology.functions.FunctionConversions._
  * }}}
  * 
  * Here's a code extract (from {@code simple.FilterEchoScala}) that
  * implicitly converts the Scala anonymous function
  * `(v:String) => v.startsWith("d")` to a
  * `com.ibm.streamsx.topology.function.Predicate`
  * instance required by the `TStream.filter` method.
  *  
  * {{{
  * 
  * // Implicit conversions of Scala anonymous functions
  * // to functions for the Java Application API
  * import com.ibm.streamsx.topology.functions.FunctionConversions._
  * 
  * object FilterEchoScala {
  *   def main(args: Array[String]) {
  *     val topology = new Topology("FilterEchoScala")
  *
  *     var echo = topology.strings(args:_*)
  * 
  *     echo = echo.filter((v:String) => v.startsWith("d"))
  * }}} 
  * 
 */
package object functions {
  
}