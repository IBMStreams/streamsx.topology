package com.ibm.streamsx.rest;

import static com.ibm.streamsx.rest.StreamsRestUtils.requestGsonResponse;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.http.auth.AUTH;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;

import com.google.gson.JsonObject;

class StreamsRestActions {
	
	static ApplicationBundle uploadBundle(Instance instance, File bundle) throws IOException {
				
		Request postBundle = Request.Post(instance.self() + "/applicationbundles");
		postBundle.addHeader(AUTH.WWW_AUTH_RESP, instance.connection().getAuthorization());
		postBundle.body(new FileEntity(bundle, ContentType.create("application/x-jar")));
		
		JsonObject response = requestGsonResponse(instance.connection().executor, postBundle);
		
		UploadedApplicationBundle uab = Element.createFromResponse(instance.connection(), response, UploadedApplicationBundle.class);
		uab.setInstance(instance);
		return uab;
    }

	static Result<Job, JsonObject> submitJob(ApplicationBundle bundle, JsonObject jco) throws IOException {
		UploadedApplicationBundle uab = (UploadedApplicationBundle) bundle;
		
		if (uab.application == null)
			uab.refresh();
		String appId = uab.application;
		uab.application = null;
		
		JsonObject body = new JsonObject();
		body.addProperty("application", appId);
		body.addProperty("preview", false);
		body.add("jobConfigurationOverlay", jco);
		
		final AbstractStreamsConnection conn = bundle.instance().connection();
		
		System.err.println(new Date() + " SUBMIT_JOB:" + jco);
		
		Request postBundle = Request.Post(bundle.instance().self() + "/jobs");
		postBundle.addHeader(AUTH.WWW_AUTH_RESP, conn.getAuthorization());
		postBundle.body(new StringEntity(body.toString(), ContentType.APPLICATION_JSON));		
		
		JsonObject response = requestGsonResponse(conn.executor, postBundle);
		
		Job job = Job.create(bundle.instance(), response.toString());
		
		
		System.err.println(new Date() + " JOB_SUBMITTED:" + job.getId() + " -- " +job.getName());

		return new ResultImpl<Job, JsonObject>(true, job.getId(),
				() -> job, new JsonObject());
	}
	
    static boolean cancelJob(Instance instance, String jobId) throws IOException {
    	System.err.println("CANCELJOB:" + jobId);
    	
    	Request deleteJob = Request.Delete(instance.self() + "/jobs/" + jobId);
    	Response response = instance.connection().executor.execute(deleteJob);
    	
    	System.err.println(new Date() + " JOB_CANCELED:" + jobId + " -- " + response.returnResponse().getStatusLine().getStatusCode());
    	// TODO - error handling
    	return true;
    }
}
