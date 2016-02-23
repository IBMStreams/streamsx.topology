package com.ibm.streamsx.topology.context;

import java.io.File;
import java.io.FileInputStream;

import com.ibm.json.java.JSONObject;

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
        
        try (FileInputStream fis= new FileInputStream(JSONFile)) {           
            JSONObject json = JSONObject.parse(fis);
            fis.close();
            
            StreamsContext<?> sc = StreamsContextFactory.getStreamsContext(context);
            Object rc = sc.submit(json).get();
            if (rc instanceof Integer)
            	System.exit((Integer) rc);
        }
    }
}
