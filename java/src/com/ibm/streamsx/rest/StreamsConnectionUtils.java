/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class StreamsConnectionUtils {

    private static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.rest.StreamsConnectionUtils");

    private StreamsConnectionUtils() {}

    /**
     * Gets a response to an HTTP call
     * 
     * @param executor HTTP client executor to use for call
     * @param auth Authentication header contents, or null
     * @param inputString
     *            REST call to make
     * @return response from the inputString
     * @throws IOException
     */
    static String getResponseString(Executor executor, String auth, String inputString) throws IOException
    {
        String sReturn = "";
        Request request = Request
                .Get(inputString)
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
            if ((sReturn != null) && (!sReturn.equals(""))) {
                throw RESTException.create(rcResponse, sReturn);
            } else {
                String httpError = "HttpStatus is " + rcResponse + " for url " + inputString;
                throw new RESTException(rcResponse, httpError);
            }
        } else {
            // all other errors...
            String httpError = "HttpStatus is " + rcResponse + " for url " + inputString;
            throw new RESTException(rcResponse, httpError);
        }
        traceLog.finest("Request: " + inputString);
        traceLog.finest(rcResponse + ": " + sReturn);
        return sReturn;
    }

    /**
     * Create an encoded Basic auth key for the given userName and authToken
     */
    static String createApiKey(String userName, String authToken) {
        String apiCredentials = userName + ":" + authToken;
        return "Basic " + DatatypeConverter.printBase64Binary(
                apiCredentials.getBytes(StandardCharsets.UTF_8));
    }

    // TODO: stub until we know how this really works, may end up using a JSON
    // object so we can stash in credentials JSON.
    static class IAMAuth {
        public String authToken;
        public long authTokenExpiry;
    };

    static IAMAuth getIAMAuth(JsonObject credentials) {
        throw new IllegalStateException("IAM authentication not yet implemented");
    }

    static String getRequiredMember(JsonObject json, String member)
            throws IllegalStateException {
        JsonElement element = json.get(member);
        if (null == element || element.isJsonNull()) {
            throw new IllegalStateException("JSON missing required member "
                    + member);
        }
        return element.getAsString();
    }

}
