/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package simple;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;

/**
* Sample echo topology application. This Java application builds a
* simple topology that echos its command line arguments to standard output.
* <BR>
* The application implements the typical pattern of code that declares a
* topology followed by submission of the topology to a Streams context
* {@code com.ibm.streamsx.topology.context.StreamsContext}.
* <BR>
* This demonstrates passing in values (the command line arguments) into a topology,
* so that the same application can produce different ouput depending on the passed
* in values.
* <P>
* This topology is always executed in embedded mode,
* within this JVM.
* <BR>
* This may be executed from the {@code samples/java/functional} directory as:
* <UL>
* <LI>{@code ant run.echo} - Using Apache Ant, this will run in embedded
* mode.</LI>
* <LI>
* {@code java -cp functionalsamples.jar:../../../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar
*  simple.Echo text to echo
* } - Run directly from the command line in embedded mode.
* <LI>
* An application execution within your IDE once you set the class path to include the correct jars.</LI>
* </UL>
* </P>
*/
public class Echo {

    /**
     * Sample Echo topology application. This Java application builds a
     * simple topology that echos its command line arguments to standard output.
     * <BR>
     * The application implements the typical pattern
     * of code that declares a topology followed by
     * submission of the topology to a Streams context
     * (@code com.ibm.streamsx.topology.context.StreamsContext}.

     * @param args Arguments to be echoed to standard out.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Topology topology = new Topology("Echo");

        /*
         * The command line arguments (args) are captured by
         * the strings() method and will be used at runtime
         * as the contents of the echo stream.
         */
        TStream<String> echo = topology.strings(args);
        echo.print();
        
        /*
         * At this point the topology is declared with a single
         * stream that is printed to System.out.
         */

        /*
         * Now execute the topology by submitting to an
         * embedded (within this JVM) StreamsContext.
         */
        StreamsContextFactory.getEmbedded().submit(topology).get();
    }
}
