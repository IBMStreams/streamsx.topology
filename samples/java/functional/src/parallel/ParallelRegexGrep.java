/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package parallel;

import static com.ibm.streamsx.topology.file.FileStreams.directoryWatcher;
import static com.ibm.streamsx.topology.file.FileStreams.textFileReader;

import java.io.ObjectStreamException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;

public class ParallelRegexGrep {
    static final Logger trace = Logger.getLogger("samples");

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        String contextType = args[0];
        String directory = args[1];
        final Pattern pattern = Pattern.compile(args[2]);

        // Define the topology
        Topology topology = new Topology("ParallelRegexGrep");

        // All streams with tuples that are Java String objects
        TStream<String> files = directoryWatcher(topology, directory);

        // Create a stream of lines from each file.
        TStream<String> lines = textFileReader(files);

        // Count the total number of lines before they are split between
        // different parallel channels.
        TStream<String> lines_counter = lines.transform(
                new Function<String, String>() {

                    private int numSentStrings = 0;

                    @Override
                    public String apply(String v1) {
                        trace.info("Have sent " + (++numSentStrings)
                                + "to be filtered.");
                        return v1;
                    }

                });

        // Parallelize the Stream.
        // Since there are 5 channels of the stream, the approximate number of
        // lines sent to each channel should be numSentStrings/5. This can be
        // verified by comparing the outputs of the lines_counter stream to that
        // of the parallel channels.
        TStream<String> lines_parallel = lines_counter.parallel(5);

        // Filter for the matched string, and print the number strings that have
        // been tested. This is happening in parallel.
        TStream<String> filtered_parallel = lines_parallel
                .filter(new Predicate<String>() {

                    private int numReceivedStrings = 0;

                    @Override
                    public boolean test(String v1) {
                        trace.info("Have received " + (++numReceivedStrings)
                                + "strings on this parallel channel.");
                        // Pass the line through if it matches the
                        // regular expression pattern
                        return matcher.reset(v1).matches();
                    }

                    transient Matcher matcher;

                    private Object readResolve() throws ObjectStreamException {
                        matcher = pattern.matcher("");
                        return this;
                    }
                });

        // Join the results of each parallel filter into one stream,
        // merging the parallel streams back into one stream.
        TStream<String> filtered_condensed = filtered_parallel.endParallel();

        // Print the combined results
        filtered_condensed.print();

        // Execute the topology
        StreamsContextFactory.getStreamsContext(contextType).submit(topology);
    }
}
