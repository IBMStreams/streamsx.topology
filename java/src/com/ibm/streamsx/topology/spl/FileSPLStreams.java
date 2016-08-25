/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import static com.ibm.streamsx.topology.spl.SPLStreams.stringToSPLStream;

import java.util.Collections;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.topology.TStream;

/**
 * Creation of SPLStreams that relate to files.
 * 
 */
public class FileSPLStreams {

    public enum Compression {
        zlib, gzip, bzip2
    }

    /**
     * Create an {@link SPLStream} using the {@code spl.adapter::FileSource}
     * operator that reads CSV records from files with names delivered by
     * {@code fileNames}.
     * 
     * @param fileNames
     *            Stream containing the file names to be read.
     * @param outputSchema
     *            SPL Schema of the CSV records and the returned stream.
     * @return Stream containing CSV records for the files present on fileNames.
     */
    public static SPLStream csvReader(TStream<String> fileNames,
            StreamSchema outputSchema) {
        

        SPLStream csvReader = SPL.invokeOperator(
                "CSVFileReader",
                "spl.adapter::FileSource", stringToSPLStream(fileNames),
                outputSchema, null);
        
        return csvReader;
    }

    /**
     * Create an {@link SPLStream} using the {@code spl.adapter::FileSource}
     * operator that reads CSV records from compressed files with names
     * delivered by {@code fileNames}.
     * 
     * @param fileNames
     *            Stream containing the file names to be read.
     * @param outputSchema
     *            SPL Schema of the CSV records and the returned stream.
     * @return Stream containing CSV records for the files present on fileNames.
     */
    public static SPLStream csvCompressedReader(TStream<String> fileNames,
            StreamSchema outputSchema, Compression compression) {
        return SPL.invokeOperator("CSVCompressedFileReader",
                "spl.adapter::FileSource", stringToSPLStream(fileNames),
                outputSchema,
                Collections.singletonMap("compression", compression));
    }
}
