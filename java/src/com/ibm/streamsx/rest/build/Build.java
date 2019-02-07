/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2019
 */
package com.ibm.streamsx.rest.build;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * 
 * An object describing an IBM Streams build.
 * 
 */
public class Build extends Element {
    
    @Expose
    private String id;
    @Expose
    private String status;
    @Expose
    private long creationTime;
    @Expose
    private String creationUser;
    @Expose
    private String name;
    @Expose
    private long lastActivityTime;
    @Expose
    private String type;
    
    @Expose
    private String artifacts;
    @Expose
    private String logMessages;
    
    private JsonObject metrics = new JsonObject();
    
    static final Build create(BuildService service, AbstractConnection connection, JsonObject gsonString) {
        // Build element = gson.fromJson(gsonString, Build.class);
        Build element = new Build();
        element.self = GsonUtilities.jstring(gsonString, "build");
        element.setConnection(connection);
        return element;
    }

    /**
     * Gets the time in milliseconds when this domain was created.
     * 
     * @return the epoch time in milliseconds when the domain was created.
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Gets the user ID that created this build.
     * 
     * @return the creation user ID
     */
    public String getCreationUser() {
        return creationUser;
    }

    /**
     * Gets the IBM Streams unique identifier for this build.
     * 
     * @return the IBM Streams unique identifier.
     */
    public String getId() {
        return id;
    }


    /**
     * Gets the status of the build.
     *
     * @return the instance status that contains one of the following values:
     *         <ul>
     *         <li>running</li>
     *         <li>stopping</li>
     *         <li>stopped</li>
     *         <li>starting</li>
     *         <li>removing</li>
     *         <li>unknown</li>
     *         </ul>
     * 
     */
    public String getStatus() {
        return status;
    }
    

    /**
     * Gets the name for this build.
     * @return name for this build.
     */
    public String getName() {
        return name;
    }
    
    public JsonObject getMetrics() {
        return metrics;
    }
    
    public Build uploadArchive(File archive) throws IOException {
        final long startUploadTime = System.currentTimeMillis();

		Request put = Request.Put(self)	      
			    .addHeader("Authorization", connection().getAuthorization())
			    .bodyFile(archive, ContentType.create("application/zip"));
		
		JsonObject response = StreamsRestUtils.requestGsonResponse(connection().executor, put);
		refresh(response);
		
        final long endUploadTime = System.currentTimeMillis();
        metrics.addProperty(SubmissionResultsKeys.SUBMIT_UPLOAD_TIME, (endUploadTime - startUploadTime));
		
    	return this;
    }
    
    public Build uploadArchiveAndBuild(File archive) throws IOException, InterruptedException {
        
        metrics.addProperty(SubmissionResultsKeys.SUBMIT_ARCHIVE_SIZE, archive.length());
        
    	uploadArchive(archive);
    	
        final long startBuildTime = System.currentTimeMillis();
        long lastCheckTime = startBuildTime;

    	
    	submit();
    	
		do {			
			refresh();
			if ("built".equals(getStatus())) {
	            final long endBuildTime = System.currentTimeMillis();
	            metrics.addProperty(SubmissionResultsKeys.SUBMIT_TOTAL_BUILD_TIME, (endBuildTime - startBuildTime));
				return this;
			}
			
            String mkey = SubmissionResultsKeys.buildStateMetricKey(getStatus());
            long now = System.currentTimeMillis();
            long duration;
            if (metrics.has(mkey)) {
                duration = metrics.get(mkey).getAsLong();                  
            } else {
                duration = 0;
            }
            duration += (now - lastCheckTime);
            metrics.addProperty(mkey, duration);
            lastCheckTime = now;
            
			try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                try {
                    delete();
                } catch (IOException ie) {
                }
                throw e;
            }
		} while ("building".equals(getStatus()) || "waiting".equals(getStatus()) || "submitted".equals(getStatus()));
		
		StreamsRestUtils.TRACE.severe("The submitted archive " + archive.getName() + " failed to build with status " + getStatus() + ".");
		
		Request gr = Request.Get(this.logMessages).addHeader("Authorization", connection().getAuthorization());
				
		String output = StreamsRestUtils.requestTextResponse(connection().executor, gr);
		String[] lines = output.split("\\R");
		for (String line : lines)
		    StreamsRestUtils.TRACE.severe(line);
    	
    	return this;
    }
    

    
    public Build submit() throws IOException {   	
    	action("submit");
    	return this;
    }
    
    public void action(String type) throws IOException {
    	
    	JsonObject action = new JsonObject();
    	action.addProperty("type", type);
		
		Request post = Request.Post(self + "/actions")	      
		    .addHeader("Authorization", connection().getAuthorization())
		    .bodyString(action.toString(), ContentType.APPLICATION_JSON);
		
		refresh( StreamsRestUtils.requestGsonResponse(connection().executor, post));
    }
    
    public List<Artifact> getArtifacts() throws IOException {
    	return Artifact.createArtifactList(this, this.artifacts);
    }
    
    public void delete() throws IOException {
    	_delete();
    }
}
