/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.build;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.logging.Logger;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AUTH;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.RESTException;
import com.ibm.streamsx.rest.internal.InputStreamConsumer;

class StreamsRestUtils {

    static final Logger TRACE = Logger.getLogger("com.ibm.streamsx.rest");
    static final int STREAMING_GET_SO_TIMEOUT_MILLIS = 60000;   // socket read timeout
    private StreamsRestUtils() {}


    /**
     * Gets a JSON response to an HTTP call
     * 
     * @param httpClient HTTP client to use for call
     * @param auth Authentication header contents, or null
     * @param inputString REST call to make
     * @return response from the inputString
     * @throws IOException
     */
    static JsonObject getGsonResponse(CloseableHttpClient httpClient,
            HttpRequestBase request) throws IOException {
        request.addHeader("accept",
                ContentType.APPLICATION_JSON.getMimeType());

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            return gsonFromResponse(response);
        }
    }

    /**
     * Gets a JSON response to an HTTP GET call
     * 
     * @param executor HTTP client executor to use for call
     * @param auth Authentication header contents, or null
     * @param inputString
     *            REST call to make
     * @return response from the inputString
     * @throws IOException
     */
    static JsonObject getGsonResponse(Executor executor, String auth, String inputString)
            throws IOException {
        TRACE.fine("HTTP GET: " + inputString);
        Request request = Request.Get(inputString).useExpectContinue();

        if (null != auth) {
            request = request.addHeader(AUTH.WWW_AUTH_RESP, auth);
        }

        return requestGsonResponse(executor, request);
    }

    /**
     * Gets a JSON response to an HTTP request call
     */
    static JsonObject requestGsonResponse(Executor executor, Request request) throws IOException {
        request.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        Response response = executor.execute(request);
        return gsonFromResponse(response.returnResponse());
    }

    static String requestTextResponse(Executor executor, Request request) throws IOException {
        request.addHeader("accept", ContentType.TEXT_PLAIN.getMimeType());
        Response response = executor.execute(request);
        return textFromResponse(response.returnResponse());
    }

    /**
     * Get a member that is expected to exist and be non-null.
     * @param json The JSON object
     * @param member The member name in the object.
     * @return The string value of the member.
     * @throws IllegalStateException if the member does not exist or is null.
     */
    static String getRequiredMember(JsonObject json, String member)
            throws IllegalStateException {
        JsonElement element = json.get(member);
        if (null == element || element.isJsonNull()) {
            throw new IllegalStateException("JSON missing required member "
                    + member);
        }
        return element.getAsString();
    }

    /**
     * Gets a response to an HTTP call as a string
     * 
     * @param executor HTTP client executor to use for call
     * @param auth Authentication header contents, or null
     * @param inputString REST call to make
     * @return response from the inputString
     * @throws IOException
     * 
     * TODO: unify error handling between this and gsonFromResponse(), and
     * convert callers that want JSON to getGsonResponse()
     */
    static String getResponseString(Executor executor,
            String auth, String inputString) throws IOException {
        TRACE.fine("HTTP GET: " + inputString);
        String sReturn = "";
        Request request = Request
                .Get(inputString)
                .addHeader("accept", ContentType.APPLICATION_JSON.getMimeType())
                .useExpectContinue();
        if (null != auth) {
            request = request.addHeader(AUTH.WWW_AUTH_RESP, auth);
        }

        Response response = executor.execute(request);
        HttpResponse hResponse = response.returnResponse();
        int rcResponse = hResponse.getStatusLine().getStatusCode();

        if (HttpStatus.SC_OK == rcResponse) {
            sReturn = EntityUtils.toString(hResponse.getEntity());
        } else if (HttpStatus.SC_NOT_FOUND == rcResponse) {
            // with a 404 message, we are likely to have a message from Streams
            // but if not, provide a better message
            sReturn = EntityUtils.toString(hResponse.getEntity());
            if (sReturn != null && !sReturn.isEmpty()) {
                throw RESTException.create(rcResponse, sReturn + " for url " + inputString);
            } else {
                String httpError = "HttpStatus is " + rcResponse + " for url " + inputString;
                throw new RESTException(rcResponse, httpError);
            }
        } else {
            // all other errors...
            String httpError = "HttpStatus is " + rcResponse + " for url " + inputString;
            throw new RESTException(rcResponse, httpError);
        }
        TRACE.finest(rcResponse + ": " + sReturn);
        return sReturn;
    }

    /**
     * Gets an entity in streaming mode.
     * @param executor 
     * @param auth
     * @param url
     * @param streamConsumer
     * @return The return of {@link InputStreamConsumer#getResult()}
     * @throws IOException
     */
    static <T> T rawStreamingGet(Executor executor,
            String auth, String url, final InputStreamConsumer<T> streamConsumer) throws IOException {
        TRACE.fine("HTTP GET: " + url);
        Request request = Request
                .Get(url)
                .addHeader("accept", ContentType.APPLICATION_JSON.getMimeType())
                .useExpectContinue()
                .socketTimeout(STREAMING_GET_SO_TIMEOUT_MILLIS);  // throw Exception when we do not read data for more than x millis 
        if (null != auth) {
            request = request.addHeader(AUTH.WWW_AUTH_RESP, auth);
        }
        Response response = executor.execute(request);
        response.handleResponse(new ResponseHandler<InputStreamConsumer<T>>() {

            @Override
            public InputStreamConsumer<T> handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
                StatusLine statusLine = response.getStatusLine();
                int rcResponse = statusLine.getStatusCode();
                HttpEntity entity = response.getEntity();
                if (HttpStatus.SC_OK == rcResponse) {
                    if (entity != null) {
                        try (InputStream is = entity.getContent()) {
                            streamConsumer.consume(is);
                        }
                        return streamConsumer;
                    }
                    else {
                        throw new ClientProtocolException("Response contains no content");
                    }
                } else {
                    // all other errors...
                    String httpError = "HttpStatus is " + rcResponse + " for url " + url;
                    throw new RESTException(rcResponse, httpError);
                }
            }
        });
        return streamConsumer.getResult();
    }

    static File getFile(Executor executor, String auth, String url, File file) throws IOException {
        return rawStreamingGet(executor, auth, url, new InputStreamConsumer<File>() {

            @Override
            public void consume(InputStream is) throws IOException {
                try {
                    Files.copy(is, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
                catch(IOException ioe) {
                    if (file.delete()) {
                        TRACE.fine("downloaded fragment " + file + " deleted");
                    }
                    throw ioe;
                }
            }

            @Override
            public File getResult() {
                return file;
            }
        });
    }

    // TODO: unify error handling between this and getResponseString()
    private static JsonObject gsonFromResponse(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        final int statusCode = response.getStatusLine().getStatusCode();
        switch (statusCode) {
        case HttpStatus.SC_OK:
        case HttpStatus.SC_CREATED:
        case HttpStatus.SC_ACCEPTED:
            break;
        default: {
            final String errorInfo;
            if (entity != null)
                errorInfo = " -- " + EntityUtils.toString(entity);
            else
                errorInfo = "";
            throw new IllegalStateException(
                    "Unexpected HTTP resource from service:"
                            + response.getStatusLine().getStatusCode() + ":" +
                            response.getStatusLine().getReasonPhrase() + errorInfo);
        }
        }

        if (entity == null)
            throw new IllegalStateException("No HTTP resource from service");

        Reader r = new InputStreamReader(entity.getContent());
        JsonObject jsonResponse = new Gson().fromJson(r, JsonObject.class);
        EntityUtils.consume(entity);
        return jsonResponse;
    }

    // TODO: unify error handling between this and getResponseString()
    private static String textFromResponse(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        final int statusCode = response.getStatusLine().getStatusCode();
        switch (statusCode) {
        case HttpStatus.SC_OK:
        case HttpStatus.SC_CREATED:
        case HttpStatus.SC_ACCEPTED:
            break;
        default: {
            final String errorInfo;
            if (entity != null)
                errorInfo = " -- " + EntityUtils.toString(entity);
            else
                errorInfo = "";
            throw new IllegalStateException(
                    "Unexpected HTTP resource from service:"
                            + response.getStatusLine().getStatusCode() + ":" +
                            response.getStatusLine().getReasonPhrase() + errorInfo);
        }
        }

        if (entity == null)
            throw new IllegalStateException("No HTTP resource from service");

        return EntityUtils.toString(entity);
    }
}
