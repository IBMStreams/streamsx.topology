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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.RESTException;

class StreamsRestUtils {
    
    static final Logger TRACE = Logger.getLogger("com.ibm.streamsx.rest");

    private StreamsRestUtils() {}

    // V1 credentials members
    static final String MEMBER_PASSWORD = "password";
    static final String MEMBER_USERID = "userid";

    private static final String AUTH_BEARER = "Bearer ";
    private static final String AUTH_BASIC = "Basic ";


    /**
     * Create an encoded Basic auth header for the given userName and authToken
     * @param userName The user name for authentication
     * @param authToken The password for authentication
     * @return the body of a Authentication: Basic header using the userName
     * and authToken
     */
    static String createBasicAuth(String userName, String authToken) {
        String apiCredentials = userName + ":" + authToken;
        return AUTH_BASIC + DatatypeConverter.printBase64Binary(
                apiCredentials.getBytes(StandardCharsets.UTF_8));
    };

    /**
     * Create an encoded Bearer auth header for the given token.
     * @param tokenBase64 An authentication token, expected to be already
     * encoded in base64, as it is when returned from the IAM server
     * @return the body of a Authentication: Bearer header using tokenBase64
     */
    static String createBearerAuth(String tokenBase64) {
        StringBuilder sb = new StringBuilder(AUTH_BEARER.length()
                + tokenBase64.length());
        sb.append(AUTH_BEARER);
        sb.append(tokenBase64);
        return sb.toString();
    }

    static Executor createExecutor() {
        return Executor.newInstance(createHttpClient(false));
    }

    static Executor createExecutor(boolean allowInsecure) {
        return Executor.newInstance(createHttpClient(allowInsecure));
    }

    static CloseableHttpClient createHttpClient() {
        return createHttpClient(false);
    }

    static CloseableHttpClient createHttpClient(boolean allowInsecure) {
        CloseableHttpClient client = null;
        if (allowInsecure) {
            try {
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial(new TrustStrategy() {
                            @Override
                            public boolean isTrusted(X509Certificate[] chain,
                                    String authType) throws CertificateException {
                                return true;
                            }
                        }).build();

                // Set protocols to allow for different handling of "TLS" by Oracle and
                // IBM JVMs.
                SSLConnectionSocketFactory factory =
                        new SSLConnectionSocketFactory(
                                sslContext,
                                new String[] {"TLSv1", "TLSv1.1","TLSv1.2"},
                                null,
                                NoopHostnameVerifier.INSTANCE);
                client = HttpClients.custom()
                        .setSSLSocketFactory(factory)
                        .build();
                TRACE.warning("Insecure host connections enabled.");
            } catch (KeyStoreException | KeyManagementException | NoSuchAlgorithmException e) {
                TRACE.warning("Unable to allow insecure host connections.");
            }
        }
        if (null == client) {
            client = HttpClients.createSystem();
        }
        return client;
    }

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
    
    static InputStream rawStreamingGet(Executor executor,
            String auth, String url) throws IOException {
        TRACE.fine("HTTP GET: " + url);
        Request request = Request
                .Get(url)
                .addHeader("accept", ContentType.APPLICATION_JSON.getMimeType())
                .useExpectContinue();
        if (null != auth) {
            request = request.addHeader(AUTH.WWW_AUTH_RESP, auth);
        }
        
        Response response = executor.execute(request);
        HttpResponse hResponse = response.returnResponse();
        int rcResponse = hResponse.getStatusLine().getStatusCode();
        
        if (HttpStatus.SC_OK == rcResponse) {
            return hResponse.getEntity().getContent();
        } else {
            // all other errors...
            String httpError = "HttpStatus is " + rcResponse + " for url " + url;
            throw new RESTException(rcResponse, httpError);
        }
    }
    
    static File getFile(Executor executor, String auth, String url, File file) throws IOException {
    	try (InputStream is = rawStreamingGet(executor, auth, url)) {
    		Files.copy(is, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    	}
    	
    	return file;
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
            default:
            
            {
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
            default:
            
            {
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
