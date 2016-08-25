/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package simple;

import java.io.ObjectStreamException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.file.FileStreams;
import com.ibm.streamsx.topology.function.Predicate;

/**
 * Sample continuous (streaming) regular expression grep topology application.
 * This is a variant of the {@link Grep} application that demonstrates
 * filtering using Java functional programming.
 * This Java application builds a
 * simple topology that watches a directory for files, reads each file and
 * output lines that match a regular expression.
 * Thus as each file is added to the directory, the application will read
 * it and output matching lines.
 * <BR>
 * The application implements the typical pattern of code that declares a
 * topology followed by submission of the topology to a Streams context {@code
 * com.ibm.streamsx.topology.context.StreamsContext}.
 * <BR>
 * This demonstrates Java functional programming using an anonymous class.
 * <P>
 * <BR>
 * This may be executed from the {@code samples/java/functional} directory as:
 * <UL>
 * <LI>
 * {@code java -cp functionalsamples.jar:../../../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar
 *   simple.RegexGrep CONTEXT_TYPE $HOME/books ".*Queen.*England.*"
 * } - Run directly from the command line.
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
public class RegexGrep {
    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        String contextType = args[0];
        String directory = args[1];
        final Pattern pattern = Pattern.compile(args[2]);

        // Define the topology
        Topology topology = new Topology("RegexGrep");

        // All streams with tuples that are Java String objects
        TStream<String> files = FileStreams.directoryWatcher(topology, directory);
        TStream<String> lines = FileStreams.textFileReader(files);
        
        /*
         * Functional filter using an anonymous class to define the
         * filtering logic, in this case execution of a regular
         * expression against each input String tuple (each line
         * of the files in the directory).
         */
        TStream<String> filtered = lines.filter(new Predicate<String>() {

            @Override
            public boolean test(String v1) {
                // Pass the line through if it matches the
                // regular expression pattern
                return matcher.reset(v1).matches();
            }

            // Recreate the matcher (which is not serializable)
            // when the object is deserialized using readResolve.
            transient Matcher matcher;

            /*
             * Since the constructor is no invoked after serialization
             * we use readResolve as a hook to execute initialization
             * code, in this case creating the matcher from the
             * pattern. 
             * The alternative would be to create it on its first use,
             * which would require an if statement in the test method.
             */
            private Object readResolve() throws ObjectStreamException {
                matcher = pattern.matcher("");
                return this;
            }
        });

        // For debugging just print out the tuples
        filtered.print();

        // Execute the topology, just like Grep.
        Future<?> future = StreamsContextFactory.getStreamsContext(contextType)
                .submit(topology);
        Thread.sleep(30000);
        future.cancel(true);
    }
}
