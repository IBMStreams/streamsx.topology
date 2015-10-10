/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package simple;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;

// Implicit conversions of Scala anonymous functions
// to functions for the Java Application API
import com.ibm.streamsx.topology.functions.FunctionConversions._

/**
* Sample filtering echo topology application. This Java application builds a
* simple topology that echos its command line arguments to standard output.
* <BR>
* The application implements the typical pattern of code that declares a
* topology followed by submission of the topology to a Streams context
* {@code com.ibm.streamsx.topology.context.StreamsContext}.
* <BR>
* This demonstrates use of Java functional logic to filter the tuples.
* An in-line anonymous class implements the filtering logic, in this
* case only echo tuples that start with the letter {@code d}.
* <P>
* This topology is always executed in embedded mode,
* within this JVM.
* <BR>
* This may be executed from the {@code samples/java/functional} directory as:
* <UL>
* <LI>{@code ant run.filterecho} - Using Apache Ant, this will run in embedded
* mode.</LI>
* <LI>
* {@code java -cp functionalsamples.jar:../../../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar
*  simple.FilterEcho "d print this" "omit this"
* } - Run directly from the command line in embedded mode.
* <LI>
* An application execution within your IDE once you set the class path to include the correct jars.</LI>
* </UL>
* </P>
*/
object FilterEchoScala {
  def main(args: Array[String]) {
    val topology = new Topology("FilterEchoScala")

    var echo = topology.strings(args:_*)
    
    echo = echo.filter((v:String) => v.startsWith("d"))
       
    echo.print()

   StreamsContextFactory.getStreamsContext("EMBEDDED").submit(topology).get()
  }
}
