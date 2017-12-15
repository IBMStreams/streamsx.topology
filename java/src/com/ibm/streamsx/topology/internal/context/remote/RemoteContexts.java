package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

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
        JsonObject results_json = GsonUtilities.objectCreate(submission, RemoteContext.SUBMISSION_RESULTS);        
        Files.write(Paths.get(resultsFile), results_json.toString().getBytes(StandardCharsets.UTF_8));
    }
    
    public static void checkServiceRunning(StreamingAnalyticsService service) throws IOException {
        final String serviceName = service.getName();
        RemoteContext.REMOTE_LOGGER.info("Streaming Analytics service (" + serviceName + "): Checking status");

        Result<StreamingAnalyticsService, JsonObject> status = service.checkStatus(true);

        RemoteContext.REMOTE_LOGGER.info("Streaming Analytics service (" + serviceName + "): instance status response:" +
                status.getRawResult().toString());
    }
}
