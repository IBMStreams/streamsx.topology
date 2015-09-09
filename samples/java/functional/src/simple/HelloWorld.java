/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package simple;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;

/**
 * Sample Hello World topology application. This Java application builds a
 * simple topology that prints Hello World to standard output. <BR>
 * The application implements the typical pattern of code that declares a
 * topology followed by submission of the topology to a Streams context
 * {@code com.ibm.streamsx.topology.context.StreamsContext}.
 * <BR>
 * This demonstrates the mechanics of declaring a topology and executing it.
 * <P>
 * This may be executed from the {@code samples/java/functional} directory as:
 * <UL>
 * <LI>{@code ant run.helloworld} - Using Apache Ant, this will run in embedded
 * mode.</LI>
 * <LI>
 * {@code java -jar funcsamples.jar:../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar
 *  simple.HelloWorld [CONTEXT_TYPE] 
 * } - Run directly from the command line.
 * </LI>
 * If no arguments are provided then the topology is executed in embedded mode,
 * within this JVM.
 * <BR>
 * <i>CONTEXT_TYPE</i> is one of:
 * <UL>
 * <LI>{@code DISTRIBUTED} - Run as an IBM Streams distributed
 * application.</LI>
 * <LI>{@code STANDALONE} - Run as an IBM Streams standalone
 * application.</LI>
 * <LI>{@code EMBEDDED} - Run embedded within this JVM.</LI>
 * <LI>{@code BUNDLE} - Create an IBM Streams application bundle.</LI>
 * <LI>{@code TOOLKIT} - Create an IBM Streams application toolkit.</LI>
 * </UL>
 * </LI>
 * <LI>
 * An application execution within your IDE once you set the class path to include the correct jars.</LI>
 * </UL>
 * </P>
 */
public class HelloWorld {

    /**
     * Sample Hello World topology application.
     * This Java application builds a simple topology
     * that prints Hello World to standard output.
     * <BR>
     * The application implements the typical pattern
     * of code that declares a topology followed by
     * submission of the topology to a Streams context
     * (@code com.ibm.streamsx.topology.context.StreamsContext}.

     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        /*
         * Create the container for the topology that will
         * hold the streams of tuples.
         */
        Topology topology = new Topology("HelloWorld");

        /*
         * Declare a source stream (hw) with String tuples containing two tuples,
         * "Hello" and "World!".
         */
        TStream<String> hw = topology.strings("Hello", "World!");
        
        /*
         * Sink hw by printing each of its tuples to System.out.
         */
        hw.print();
        
        /*
         * At this point the topology is declared with a single
         * stream that is printed to System.out.
         */

        /*
         * Now execute the topology by submitting to a StreamsContext.
         * If no argument is provided then the topology is executed
         * within this JVM (StreamsContext.Type.EMBEDDED).
         * Otherwise the first and only argument is taken as the
         * String representation of the 
         */
        if (args.length == 0)
            StreamsContextFactory.getEmbedded().submit(topology).get();
        else
            StreamsContextFactory.getStreamsContext(args[0]).submit(topology)
                    .get();
    }
}
