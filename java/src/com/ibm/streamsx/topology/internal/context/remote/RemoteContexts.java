package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;

public class RemoteContexts {
    /**
     * Write the results output to the results file.
     * @param submission The JsonObject representation of the application.
     * @return The Future<> result of the submission.
     * @throws IOException 
     */
    public static void writeResultsToFile(JsonObject submission) throws IOException {
        // Get the results file location.
        String resultsFile = jstring(submission, RemoteContext.SUBMISSION_RESULTS_FILE);
        if(resultsFile == null)
            return;
    
        // Write to the file and close the file.
        JsonObject results_json = object(submission, RemoteContext.SUBMISSION_RESULTS);
        if(results_json == null)
            return;
        
        Files.write(Paths.get(resultsFile), results_json.toString().getBytes(StandardCharsets.UTF_8));
    }
}
