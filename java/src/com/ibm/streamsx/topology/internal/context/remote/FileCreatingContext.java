package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;

public abstract class FileCreatingContext extends RemoteContextImpl<File> {
    @Override
    /**
     * Write the file name of the created toolkit to the results file.
     * @param submission The JsonObject representation of the application.
     * @param future The Future<> result of the submission.
     * @return The Future<> result of the submission.
     */
    Future<File> postSubmit(JsonObject submission, Future<File> future) {
        // Get the results file location.
        String resultsFile = jstring(submission, RemoteContext.SUBMISSION_RESULTS_FILE);
        if(resultsFile == null){
            return future;
        }
        
        // Create the contents of the results file: a json object containing the created file path.
        JsonObject results = new JsonObject(); 
        try {
            results.addProperty(RESULTS_FILE_KEY, future.get().getAbsolutePath());
        } catch (InterruptedException e) {
            e.printStackTrace();
            return future;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return future;
        }
    
        // Write to the file and close the file.
        List<String> lines = new ArrayList<>();
        lines.add(results.toString());
        try{          
            Files.write(Paths.get(resultsFile), lines);
        } catch(IOException ioe){
            ioe.printStackTrace();
        }
        return future;
    }
    
    final static String RESULTS_FILE_KEY = "createdFilePath";
}
