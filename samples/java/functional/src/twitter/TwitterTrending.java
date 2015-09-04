/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package twitter;

import static com.ibm.streamsx.topology.file.FileStreams.directoryWatcher;
import static com.ibm.streamsx.topology.file.FileStreams.textFileReader;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;

/**
 * Sample twitter trending topology application. This Java application builds a 
 * topology that reads from a file of tweets, extracts the hashtags from each
 * line, and uses a window to keep track of the most popular hashtags from the
 * past 40,000 tweets.
 * 
 * <br><br>
 * 
 * Although the application reads from a file, in principle it could be attached
 * to a live data source.
 * 
 * <BR>
 * <P>
 * If no arguments are provided then the topology is executed in embedded mode,
 * within this JVM.
 * <BR>
 * This may be executed from the {@code samples/java/functional} directory as:
 * <UL>
 * <LI>{@code ant run.twitter.trending} - Using Apache Ant, this will run in embedded
 * mode and assumes tweets are in CSV files in {@code $HOME/tweets}.</LI>
 * <LI>
 * {@code java -cp functionalsamples.jar:../../../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar
 *  twitter.TwitterTrending CONTEXT_TYPE DIRECTORY
 * } - Run directly from the command line.
 * </LI>
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
 * and <i>DIRECTORY</i> is the location of a directory that contains one or more
 * text files containing lines of tweets.
 * </LI>
 * <LI>
 * An application execution within your IDE once you set the class path to include the correct jars.</LI>
 * </UL>
 * </P>
 */
public class TwitterTrending {
    private static final Pattern TAG_PATTERN = Pattern
            .compile("(?:^|\\s|[\\p{Punct}&&[^/]])(#[\\p{L}0-9-_]+)");

    @SuppressWarnings("serial")
    public static void main(String args[]) throws Exception {
        if(args.length == 0){
            throw new IllegalArgumentException("Must supply CONTEXT_TYPE and DIRECTORY as arguments");
        }
        String contextType = args[0];
        String directory = args[1];

        // Define the topology
        Topology topology = new Topology("twitterPipeline");

        // Stream containing file with tweets
        TStream<String> files = directoryWatcher(topology, directory);

        // Create a stream of lines from each file.
        TStream<String> lines = textFileReader(files);

        // Extract the hashtags from the string
        TStream<String> hashtags = lines.multiTransform(
                new Function<String, Iterable<String>>() {

                    @Override
                    public Iterable<String> apply(String v1) {
                        ArrayList<String> tweetHashTags = new ArrayList<String>();
                        matcher.reset(v1);
                        while (matcher.find()) {
                            tweetHashTags.add(matcher.group(1));
                        }
                        return tweetHashTags;
                    }

                    transient Matcher matcher;

                    private Object readResolve() throws ObjectStreamException {
                        matcher = TAG_PATTERN.matcher("");
                        return this;
                    }

                });

        // Extract the most frequent hashtags
        TStream<List<HashTagCount>> hashTagMap = hashtags.last(40000).aggregate(
                new Function<List<String>, List<HashTagCount>>() {

                    @Override
                    public List<HashTagCount> apply(List<String> v1) {
                        Trender tre = new Trender();
                        for (String s_iter : v1) {
                            tre.add(s_iter);
                        }
                        return tre.getTopTen();
                    }

                });

        hashTagMap.print();

        StreamsContextFactory.getStreamsContext(contextType).submit(topology);
    }
}
