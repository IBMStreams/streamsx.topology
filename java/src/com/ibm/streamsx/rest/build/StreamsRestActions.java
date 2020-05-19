package com.ibm.streamsx.rest.build;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.internal.DirectoryZipInputStream;

class StreamsRestActions {
	

  static boolean deleteToolkit(Toolkit toolkit) throws IOException
  {
    Request deleteToolkit = Request.Delete(toolkit.self());
    Response response = toolkit.connection().getExecutor().execute(deleteToolkit);
    HttpResponse httpResponse = response.returnResponse();
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (statusCode == HttpStatus.SC_NO_CONTENT)
      return true;
    if (statusCode == HttpStatus.SC_NOT_FOUND)
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

    Response response = connection.getExecutor().execute(post);
    HttpResponse httpResponse = response.returnResponse();
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    // TODO The API is supposed to return CREATED, but there is a bug and it
    // returns OK.  When the bug is fixed, change this to accept only OK
    if (statusCode != HttpStatus.SC_CREATED && statusCode != HttpStatus.SC_OK) {
      String message = EntityUtils.toString(httpResponse.getEntity());
      throw RESTException.create(statusCode, message);
    }
    
    HttpEntity entity = httpResponse.getEntity();
    try (Reader r = new InputStreamReader(entity.getContent())) {
      JsonObject jresponse = new Gson().fromJson(r, JsonObject.class);
      EntityUtils.consume(entity);
      List<Toolkit> toolkitList = Toolkit.createToolkitList(connection, jresponse);

      // We expect a list of zero or one element.
      if (toolkitList.size() == 0) {
        return null;
      }
      return toolkitList.get(0);
    }
  }
}
