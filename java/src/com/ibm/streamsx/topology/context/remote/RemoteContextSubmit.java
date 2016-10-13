/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.context.remote;

import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Given the path of a file containing the JSON representation of a graph,
 * read the file and submit it to a remote context as JSON.
 */
public class RemoteContextSubmit {

    public static void main(String[] args) throws Exception {
    	String context = args[0];
        String JSONPath = args[1];
        
        File JSONFile = new File(JSONPath);
        
        try (BufferedReader reader = Files.newBufferedReader(JSONFile.toPath(), StandardCharsets.UTF_8)) { 
            JsonParser parser = new JsonParser();
            JsonObject json = parser.parse(reader).getAsJsonObject();
            reader.close();
            
            RemoteContext<?> sc = RemoteContextFactory.getRemoteContext(context);
            sc.submit(json).get();
        }
    }
}
