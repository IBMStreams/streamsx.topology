/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.internal;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.RESTException;

public interface RestUtils {
    
    static final Logger TRACE = Logger.getLogger("com.ibm.streamsx.rest");

    // V1 credentials members
    String MEMBER_PASSWORD = "password";
    String MEMBER_USERID = "userid";
    
    String AUTH_BEARER = "Bearer ";
    String AUTH_BASIC = "Basic ";
    
    int CONNECT_TIMEOUT_MILLISECONDS = 30000;

    /**
     * Create an encoded Basic auth header for the given credentials.
     * @param credentials Service credentials.
     * @return the body of a Authentication: Basic header using the user and
     * password contained in the credentials. 
     */
    static String createBasicAuth(JsonObject credentials) {
        return createBasicAuth(jstring(credentials,  MEMBER_USERID),
                jstring(credentials, MEMBER_PASSWORD));
    }

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
    }

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
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(CONNECT_TIMEOUT_MILLISECONDS) // timeout to connect with server
                // we don't change the socket (receive) timeout as it affects ALL REST requests
                // system defaults would be used when setting to -1, system defaults are usually 0, however.
//                .setSocketTimeout(-1)             // socket idle timeout between data packages
//                .setConnectionRequestTimeout(0)   // timeout for a connection manager to obtain a connection
                .build();
        
        if (allowInsecure) {
            try {
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial(new TrustStrategy() {
                            @Override
                            public boolean isTrusted(X509Certificate[] chain,
                                    String authType) throws CertificateException {
                                // simply trust all certificates
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
                        .setDefaultRequestConfig(requestConfig)
                        .build();
                TRACE.warning("Insecure host connections enabled.");
            } catch (KeyStoreException | KeyManagementException | NoSuchAlgorithmException e) {
                TRACE.warning("Unable to allow insecure host connections.");
            }
        }
        if (null == client) {
            // Obtain default SSL socket factory with an SSL context based on system properties
            // as described in Java Secure Socket Extension (JSSE) Reference Guide.
            SSLConnectionSocketFactory factory = SSLConnectionSocketFactory.getSystemSocketFactory();
            client = HttpClients.custom()
                    .setSSLSocketFactory(factory)
                    .setDefaultRequestConfig(requestConfig)
                    .build();
        }
        return client;
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
    static JsonObject getGsonResponse(Executor executor, String auth, String url)
            throws IOException {
        TRACE.fine("HTTP GET: " + url);
        Request request = Request.Get(url).useExpectContinue();
        
        if (null != auth) {
            request = request.addHeader(AUTH.WWW_AUTH_RESP, auth);
        }
        
        return requestGsonResponse(executor, request);
    }
    
    static JsonObject getGsonResponse(Executor executor, String auth, URL url) throws IOException {
        return getGsonResponse(executor, auth, url.toExternalForm());
    }


    /**
     * Gets a response to an HTTP GET call as a string
     * 
     * @param executor HTTP client executor to use for call
     * @param auth Authentication header contents, or null
     * @param inputString REST call to make, i.e. the URI
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
     * Gets a JSON response to an HTTP request call
     */
    static JsonObject requestGsonResponse(Executor executor, Request request) throws IOException {
        request.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        Response response = executor.execute(request);
        return gsonFromResponse(response.returnResponse());
    }
    
    // TODO: unify error handling between this and getResponseString()
    static JsonObject gsonFromResponse(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED && statusCode != HttpStatus.SC_ACCEPTED) {
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

        if (entity == null)
            throw new IllegalStateException("No HTTP resource from service");

        Reader r = new InputStreamReader(entity.getContent());
        JsonObject jsonResponse = new Gson().fromJson(r, JsonObject.class);
        EntityUtils.consume(entity);
        return jsonResponse;
    }
}
