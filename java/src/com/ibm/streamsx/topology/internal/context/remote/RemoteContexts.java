package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;

public class RemoteContexts {
    /**
     * Write the results output to the results file.
     * @param submission The JsonObject representation of the application.
     * @return The Future<> result of the submission.
     */
    public static void writeResultsToFile(JsonObject submission) {
        // Get the results file location.
        String resultsFile = jstring(submission, RemoteContext.SUBMISSION_RESULTS_FILE);
        if(resultsFile == null)
            return;
    
        // Write to the file and close the file.
        List<String> lines = new ArrayList<>();
        JsonObject results_json = object(submission, RemoteContext.SUBMISSION_RESULTS);
        if(results_json == null)
            return;
        lines.add(results_json.toString());
        try{          
            Files.write(Paths.get(resultsFile), lines);
        } catch(IOException ioe){
            ioe.printStackTrace();
        }
    }
}
