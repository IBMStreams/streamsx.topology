/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.context.local;

import java.io.File;
import java.io.FileReader;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;

/**
 * Given the path of a file containing the JSON representation of a graph,
 * read the file, generate the SPL, and write it to the specified file. All
 * paths should be absolute.
 */
public class StreamsContextSubmit {

    public static void main(String[] args) throws Exception {
    	String context = args[0];
        String JSONPath = args[1];
        
        File JSONFile = new File(JSONPath);
        
        try (FileReader fr= new FileReader(JSONFile)) {
            JsonParser parser = new JsonParser();
            JsonObject graph = parser.parse(fr).getAsJsonObject();
            
            StreamsContext<?> sc = StreamsContextFactory.getStreamsContext(context);
            Object rc = sc.submit(graph).get();
            if (rc instanceof Integer)
            	System.exit((Integer) rc);
        }
    }
}
