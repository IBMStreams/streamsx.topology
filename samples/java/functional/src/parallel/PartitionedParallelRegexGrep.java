/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package parallel;

import static com.ibm.streamsx.topology.file.FileStreams.directoryWatcher;
import static com.ibm.streamsx.topology.file.FileStreams.textFileReader;
import static com.ibm.streamsx.topology.logic.Value.of;

import java.io.ObjectStreamException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Predicate;

/**
 * PartitionedParallelRegexGrep is like ParallelRegexGrep, except that the Java
 * object in the tuple being passed into the parallel region implements the
 * Keyable interface, and provides a getKey() function which is used to map
 * tuples to their corresponding channel in the parallel region.
 * 
 * Each channel of the parallel region only receives tuples that have the same
 * hashCode() value of the Key returned by the tuple value's getKey() method. In
 * other words, for each tuple, the value returned by
 * 
 * tupleValue.getKey().hashCode()
 * 
 * will go to the same channel, for each tuple which returns that result. To
 * show this, instead of passing a java.lang.String into the parallel region, a
 * stringWrapper class is created that implements the Keyable interface.
 * 
 * For this sample, if you read from a file that contains the following:
 * 
 * Apple Orange Banana Banana Apple Apple
 * 
 * you notice that the lines containing Apple will always be sent to the same
 * channel of the parallel region; the same for the lines containing Orange and
 * Banana.
 * 
 * 
 */
public class PartitionedParallelRegexGrep {
    static final Logger trace = Logger
            .getLogger("samples.partitionedparallelregexgrep");

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        String contextType = args[0];
        String directory = args[1];
        final Pattern pattern = Pattern.compile(args[2]);

        // Define the topology
        Topology topology = new Topology("PartitionedParallelRegexGrep");

        // All streams with tuples that are Java String objects
        TStream<String> files = directoryWatcher(topology, directory);
        TStream<String> lines = textFileReader(files);

        // Begin parallel region
        TStream<String> parallelLines = lines
                .parallel(of(5), TStream.Routing.HASH_PARTITIONED);
        TStream<String> ParallelFiltered = parallelLines
                .filter(new Predicate<String>() {

                    @Override
                    public boolean test(String v1) {
                        // If you inspect the output of the streams in this
                        // parallel
                        // region, you will see that any string that is sent to
                        // one
                        // channel will not be sent to another. In other words,
                        // if you
                        // see "apple" being sent to this channel, you will
                        // never see
                        // "apple" being sent to any other channel.
                        trace.info("Testing  string \"" + v1
                                + "\" for the pattern.");
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

        // Combine the results of each parallel filter into one stream, ending
        // the parallel region.
        TStream<String> filtered_condensed = ParallelFiltered
                .endParallel();

        // Print the combined results
        filtered_condensed.print();

        // Execute the topology
        StreamsContextFactory.getStreamsContext(contextType).submit(topology);
    }
}
