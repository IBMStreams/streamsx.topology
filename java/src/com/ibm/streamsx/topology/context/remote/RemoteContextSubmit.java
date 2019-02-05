/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2019
 */
package com.ibm.streamsx.topology.context.remote;

import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.logging.Level;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.topology.internal.context.remote.RemoteContextImpl;
import com.ibm.streamsx.topology.internal.logging.Logging;

/**
 * Given the path of a file containing the JSON representation of a graph,
 * read the file and submit it to a remote context as JSON.
 */
public class RemoteContextSubmit {

    public static void main(String[] args) throws Exception {
    	String context = args[0];
        String JSONPath = args[1];
        Logging.setRootLevels(args[2]);
        
        File JSONFile = new File(JSONPath);
        
        try (BufferedReader reader = Files.newBufferedReader(JSONFile.toPath(), StandardCharsets.UTF_8)) { 
            JsonParser parser = new JsonParser();
            JsonObject json = parser.parse(reader).getAsJsonObject();
            reader.close();
            
            RemoteContextImpl.PROGRESS.setLevel(Level.INFO);
            RemoteContext<?> sc = RemoteContextFactory.getRemoteContext(context);
            sc.submit(json).get();
        }
    }
}
