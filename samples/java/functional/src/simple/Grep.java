/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package simple;

import java.util.concurrent.Future;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.file.FileStreams;
import com.ibm.streamsx.topology.streams.StringStreams;

/**
 * Sample continuous (streaming) grep topology application. This Java application builds a
 * simple topology that watches a directory for files, reads each file and
 * output lines that contain the search term.
 * Thus as each file is added to the directory, the application will read
 * it and output matching lines.
 * <BR>
 * The application implements the typical pattern of code that declares a
 * topology followed by submission of the topology to a Streams context
 * {@code com.ibm.streamsx.topology.context.StreamsContext}.
 * <BR>
 * This demonstrates the a continuous application and use of
 * utility classes that produce streams.
 * <P>
 * <BR>
 * This may be executed from the {@code samples/java/functional} directory as:
 * <UL>
 * <LI>
 * {@code java -cp functionalsamples.jar:../../../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar
 *    simple.Grep CONTEXT_TYPE $HOME/books Elizabeth
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
 * </LI>
 * <LI>
 * An application execution within your IDE once you set the class path to include the correct jars.</LI>
 * </UL>
 * </P>
 */
public class Grep {
    
    /**
     * Sample continuous (streaming) grep topology application. This Java
     * application builds a simple topology that watches a directory for
     * files, reads each file and output lines that contain the search term.
     * Thus as each file is added to the directory, the application will read
     * it and output matching lines.
     * <BR>
     * The application implements the typical pattern of code that declares a
     * topology followed by submission of the topology to a Streams context {@code
     * com.ibm.streamsx.topology.context.StreamsContext}.
     * <P>
     * Three arguments are required:
     * <UL>
     * <LI>{@code contextType} - The type of the context to execute the topology in, e.g. {@code EMBEDDED or STANDALONE}.</LI>
     * <LI>{@code directory} - Directory to watch for files.</LI>
     * <LI>{@code term} - Search term, if any line in a file contains {@code term} then it will be printed.
     * </UL>
     * For example (classpath omitted for brevity):
     * <BR>
     * {@code java simple.Grep EMBEDDED $HOME/books Elizabeth}
     */
    public static void main(String[] args) throws Exception {
        String contextType = args[0];
        String directory = args[1];
        String term = args[2];

        Topology topology = new Topology("Grep");

        /*
         * Use the file stream utility class com.ibm.streamsx.topology.file.FileStreams
         * to declare a stream that will contain file names from the specified directory.
         * As each new file is created in directory its absolute file path will
         * appear on fileNames.
         */
        TStream<String> filePaths = FileStreams.directoryWatcher(topology, directory);
        
        /* 
         * Use the file stream utility class com.ibm.streamsx.topology.file.FileStreams
         * to declare a stream that will contain the contents of the files.
         * FileStreams.textFileReader creates a function that for each input
         * file pat, opens the file and reads its contents as a text file,
         * producing a tuple for each line of the file. The tuple contains
         * the contents of the line, as a String.
         */
        TStream<String> lines = FileStreams.textFileReader(filePaths);
        
        /*
         * Use the string stream utility class com.ibm.streamsx.topology.streams.StringStreams
         * to filter out non-matching lines. StringStreams.contains creates a functional
         * Predicate that will be executed for each tuple on lines, that is each line
         * read from a file.
         */
        TStream<String> matching = StringStreams.contains(lines, term);
        
        /*
         * And print the matching lines to standard out.
         */
        matching.print();

        /*
         * Execute the topology, since FileStreams.directoryWatcher declares
         * a stream that lasts forever, that it is is always watching the directory,
         * then when submit() returns the application is not complete.
         * In fact it will never complete.
         */
        Future<?> future = StreamsContextFactory.getStreamsContext(contextType)
                .submit(topology);
        
        /*
         * Let the application run for thirty seconds (30,000ms)
         * and then cancel it.
         */
        Thread.sleep(30 * 1000);
        future.cancel(true);
    }
}
