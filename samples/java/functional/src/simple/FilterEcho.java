/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package simple;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Predicate;

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
public class FilterEcho {

    /**
    * Sample filtering echo topology application. This Java application builds a
    * simple topology that echos its command line arguments to standard output.
    * <BR>
    * The application implements the typical pattern of code that declares a
    * topology followed by submission of the topology to a Streams context (@code
    * com.ibm.streamsx.topology.context.StreamsContext}.
    *
    */
    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        Topology topology = new Topology("FilterEcho");

        TStream<String> echo = topology.strings(args);
        
        /*
         * Declare a stream that will execute functional logic
         * against tuples on the echo stream.
         * For each tuple that will appear on echo, the below
         * test(tuple) method will be called, it it returns
         * true then the tuple will appear on the filtered
         * stream, otherwise the tuple is discarded.
         */
        TStream<String> filtered = echo.filter(new Predicate<String>() {

            @Override
            public boolean test(String tuple) {
                return tuple.startsWith("d");
            }
        });

        filtered.print();
        
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
