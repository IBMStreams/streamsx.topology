package com.ibm.streamsx.rest;

import static com.ibm.streamsx.rest.StreamsRestUtils.requestGsonResponse;

import java.io.File;
import java.io.IOException;

import java.util.Collections;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AUTH;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.internal.ZipStream;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;

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
		
		JsonObject body = new JsonObject();
		body.addProperty("application", uab.getBundleId());
		body.addProperty("preview", false);		
		body.add("jobConfigurationOverlay", jco);
		
		final AbstractStreamsConnection conn = bundle.instance().connection();
		
		Request postBundle = Request.Post(bundle.instance().self() + "/jobs");
		postBundle.addHeader(AUTH.WWW_AUTH_RESP, conn.getAuthorization());
		postBundle.body(new StringEntity(body.toString(), ContentType.APPLICATION_JSON));		
		
		JsonObject response = requestGsonResponse(conn.executor, postBundle);
		
		Job job = Job.create(bundle.instance(), response.toString());
		
		if (!response.has(SubmissionResultsKeys.JOB_ID))
			response.addProperty(SubmissionResultsKeys.JOB_ID, job.getId());

		return new ResultImpl<Job, JsonObject>(true, job.getId(),
				() -> job, response);
	}
	
    static boolean cancelJob(Instance instance, String jobId) throws IOException {
    	Request deleteJob = Request.Delete(instance.self() + "/jobs/" + jobId);
    	Response response = instance.connection().executor.execute(deleteJob);
    	
    	// TODO - error handling
    	return true;
    }

  static boolean deleteToolkit(Toolkit toolkit) throws IOException
  {
    Request deleteToolkit = Request.Delete(toolkit.self());
    Response response = toolkit.connection().getExecutor().execute(deleteToolkit);
    HttpResponse httpResponse = response.returnResponse();
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (statusCode == 204)
      return true;
    if (statusCode == 404)
      return false;
    String message = EntityUtils.toString(httpResponse.getEntity());
    throw RESTException.create(statusCode, message);
  }

  static Toolkit putToolkit(AbstractStreamsConnection connection, File path) throws IOException {
    // Make sure it is a directory
    if (! path.isDirectory()) {
      throw new IllegalArgumentException("The specified toolkit path '" + path.toString() + "' is not a directory.");
    }

    // Make sure it contains toolkit.xml
    File toolkit = new File(path, "toolkit.xml");
    if (! toolkit.isFile()) {
      throw new IllegalArgumentException("The specified toolkit path '" + path.toString() + "' is not a toolkit.");
    }

    String toolkitsURL = connection.getToolkitsURL();
    Request post = Request.Post(toolkitsURL);
    post.addHeader(AUTH.WWW_AUTH_RESP, connection.getAuthorization());
    post.bodyStream(ZipStream.fromPath(path.toPath()), ContentType.create("application/zip"));

    JsonObject response = requestGsonResponse(connection.getExecutor(), post);
    List<Toolkit> toolkitList = Toolkit.createToolkitList(connection, response);

    // We expect a list of zero or one element.
    if (toolkitList.size() == 0) {
      return null;
    }
    return toolkitList.get(0);
  }
}
