/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * Given the path of a file containing the JSON representation of a graph,
 * read the file, generate the SPL, and write it to the specified file. All
 * paths should be absolute.
 */
public class SPLFromFileName {

    public static void main(String[] args) throws IOException {
        String JSONPath = args[0];
        String SPLPath = args[1];
        
        File JSONFile = new File(JSONPath);
        if(!JSONFile.exists()){
            throw new FileNotFoundException(Messages.getString("GENERATOR_FILE_NOT_EXIST", JSONPath));
        }
                
        try (BufferedReader input = new BufferedReader(
                new InputStreamReader(new FileInputStream(JSONFile), StandardCharsets.UTF_8))) {

            JsonParser parser = new JsonParser();

            JsonObject jso = parser.parse(input).getAsJsonObject();

            SPLGenerator splGen = new SPLGenerator();
            String SPLString = splGen.generateSPL(jso);

            File f = new File(SPLPath);
            PrintWriter splFile = new PrintWriter(f, "UTF-8");
            splFile.print(SPLString);
            splFile.flush();
            splFile.close();
        }
    }
}
