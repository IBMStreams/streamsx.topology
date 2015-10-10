package com.ibm.streamsx.topology

/**
  * Scala application support for IBM Streams applications.
  * 
  * ==Overview==
  * Applications are written in Scala for IBM Streams by
  * using the Java Application API classes and methods.
  *
  * Importing [[com.ibm.streamsx.topology.functions.FunctionConversions]],
  * allows Scala functions to be used as functions used
  * to transform, filter tuples etc.
  * {{{
  * import com.ibm.streamsx.topology.functions.FunctionConversions._
  * }}}
  * 
  * Here's a simple example ({@code simple.FilterEchoScala}) that filters
  * a stream created from the command line arguments so that it only
  * includes values that being with `d`.
  *  
  * {{{
  * package simple;
  * 
  * import com.ibm.streamsx.topology.TStream;
  * import com.ibm.streamsx.topology.Topology;
  * import com.ibm.streamsx.topology.context.StreamsContextFactory;
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
  *   
  *     echo.print()
  * 
  *     StreamsContextFactory.getStreamsContext("EMBEDDED").submit(topology).get()
  *   }
  * }
  * }}} 
  * 
  * ===Samples===
  * Sample Scala applications are under `samples/scala`.
  * 
 */
package object functions {
  
}