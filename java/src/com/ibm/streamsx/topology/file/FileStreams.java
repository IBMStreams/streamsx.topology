/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.file;

import java.util.Collections;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.spl.JavaPrimitive;
import com.ibm.streamsx.topology.spl.SPLSchemas;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

/**
 * Utilities for file system related streams.
 * 
 */
public class FileStreams {

    /**
     * Creates a stream of absolute newly created file names from watching
     * {@code directory}.
     * 
     * @param directory
     *            Name of the directory to watch.
     * @return A stream that will contain newly created files in
     *         {@code directory}.
     */
    public static TStream<String> directoryWatcher(TopologyElement te,
            String directory) {

        SPLStream filesSpl = JavaPrimitive.invokeJavaPrimitiveSource(te,
                DirectoryWatcher.class,
                SPLSchemas.STRING,
                Collections.singletonMap("directory", directory)
                );

        return filesSpl.toStringStream();
    }

    /**
     * Filter a {@code Stream<String>} containing file names by suffix values.
     * If a file name on {@code fileNames} ends with a suffix in
     * {@code suffixes} preceded by a dot {@code '.'}.
     * 
     * @param fileNames
     *            Input stream that will contain file names.
     * @param suffixes
     *            Suffixes to filter for.
     * @return Stream that will contain file names with suffixes in
     *         {@code suffixes}.
     */
    public static TStream<String> suffixFilter(TStream<String> fileNames,
            String... suffixes) {

        final String[] dotSuffixes = new String[suffixes.length];
        for (int i = 0; i < suffixes.length; i++) {
            dotSuffixes[i] = "." + suffixes[i];
        }
        return fileNames.filter(new Predicate<String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean test(String fileName) {
                for (int i = 0; i < dotSuffixes.length; i++) {
                    if (fileName.endsWith(dotSuffixes[i]))
                        return true;
                }
                return false;
            }
        });
    }

    /**
     * Returns a Stream that reads each file named on its input stream,
     * outputting a tuple for each line read. All files are assumed to be
     * encoded using UTF-8. The lines are output in the order they appear in
     * each file, with the first line of a file appearing first.
     * 
     * @param input
     *            Stream containing files to read.
     * @return Stream contains lines from input files.
     */
    public static TStream<String> textFileReader(TStream<String> input) {

        SPLStream tupleInput = SPLStreams.stringToSPLStream(input);
        SPLStream lines = JavaPrimitive.invokeJavaPrimitive(
                TextFileReader.class, tupleInput, SPLSchemas.STRING, null);
        return lines.toStringStream();
    }
}
