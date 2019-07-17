package com.ibm.streamsx.rest.build;

// TODO pare down these imports
import static com.ibm.streamsx.rest.build.StreamsRestUtils.requestGsonResponse;

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

import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.internal.DirectoryZipInputStream;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;

class StreamsRestActions {
	

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

  static Toolkit uploadToolkit(StreamsBuildService connection, File path) throws IOException {
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
    post.bodyStream(DirectoryZipInputStream.fromPath(path.toPath()), ContentType.create("application/zip"));

    JsonObject response = requestGsonResponse(connection.getExecutor(), post);
    List<Toolkit> toolkitList = Toolkit.createToolkitList(connection, response);

    // We expect a list of zero or one element.
    if (toolkitList.size() == 0) {
      return null;
    }
    return toolkitList.get(0);
  }
}
