/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
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
import org.apache.http.client.methods.HttpPost;
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

class StreamsRestUtils {
    
    static final Logger TRACE = Logger.getLogger("com.ibm.streamsx.rest");

    private StreamsRestUtils() {}

    enum StreamingAnalyticsServiceVersion { V1, V2, UNKNOWN }

    // V1 credentials members
    static final String MEMBER_PASSWORD = "password";
    static final String MEMBER_USERID = "userid";

    // V2 credentials members
    static final String MEMBER_V2_REST_URL = "v2_rest_url";
    private static final String MEMBER_APIKEY = "apikey";

    // IAM response members 
    private static final String MEMBER_EXPIRATION = "expiration";
    private static final String MEMBER_ACCESS_TOKEN = "access_token";

    private static final String AUTH_BEARER = "Bearer ";
    private static final String AUTH_BASIC = "Basic ";
    private static final String TOKEN_PARAMS = genTokenParams();

    private static final long MS = 1000L;
    private static final long EXPIRY_PAD_MS = 300 * MS;

    private static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.rest.StreamsRestUtils");

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
                traceLog.warning("Insecure host connections enabled.");
            } catch (KeyStoreException | KeyManagementException | NoSuchAlgorithmException e) {
                traceLog.warning("Unable to allow insecure host connections.");
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
        traceLog.finest(rcResponse + ": " + sReturn);
        return sReturn;
    }
    
    static InputStream rawStreamingGet(Executor executor,
            String auth, String url) throws IOException {
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
    
    static void getFile(Executor executor, String auth, String url, File file) throws IOException {
    	try (InputStream is = rawStreamingGet(executor, auth, url)) {
    		Files.copy(is, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    	}
    }

    /**
     * Get the IAM API key that will be used to request a token.
     * @param credentials Service credentials.
     * @return the IAM API key
     */
    static String getServiceApiKey(JsonObject credentials) {
        return jstring(credentials, MEMBER_APIKEY);
    }

    /**
     * Determine service version based on credential contents.
     * <p>
     * Ideally, the service would return version information directly, but for
     * now key off contents we expect in the credentials.
     * <p>
     * As described by the IBM Cloud team there are 3 cases:
     * <ol>
     * <li>New container-based RHEL 7.x instances use IAM authentication for both
     * Streaming Analytics Service and Streams REST APIs, must use V2 of the
     * Service API, and have <tt>v2_rest_url</tt> but neither <tt>userid</tt>
     * nor <tt>password</tt>.</li>
     * <li>Old VM-based RHEL 6.x instances use Basic authentication for both
     * REST APIs, use V1 for the Service API, and have <tt>userid</tt> and
     * <tt>password</tt> but do not have <tt>v2_rest_url</tt></li>
     * <li>New instances created on old plans are VM-based and RHEL 6.x, and
     * must use Basic authentication for the Streams REST API, but can use
     * either V1 with Basic or V2 with IAM for the Service API. These have
     * <tt>userid</tt>, <tt>password</tt>, and <tt>v2_rest_url</tt>.
     * </ol>
     * <p>
     * Since the last case can function just like the second as a V1 Service
     * and using Basic authentication for both APIs, we use the existence of
     * <tt>userid</tt> and <tt>password</tt> to signal a V1 environment and
     * treat them the same.
     * <p>
     * Both versions have other required parameters, but those will be handled
     * by callers as needed and are not checked here.
     * <p>
     * Note also that while service version and authentication mechanism are
     * conceptually distinct, at present they are coupled so the version implies
     * the authentication mechanism.
     *  
     * @param credentials Service credentials.
     * @return A version or UNKNOWN.
     */
    static StreamingAnalyticsServiceVersion getStreamingAnalyticsServiceVersion(
            JsonObject credentials) {
        if (credentials.has(MEMBER_USERID) && credentials.has(MEMBER_PASSWORD)) {
            return StreamingAnalyticsServiceVersion.V1;
        } else if (credentials.has(MEMBER_V2_REST_URL)) { 
            return StreamingAnalyticsServiceVersion.V2;
        }
        return StreamingAnalyticsServiceVersion.UNKNOWN;
    }

    static String fixStreamsRestUrl(String streamsRestUrl) {
        final String suffix = "resources";
        StringBuilder sb = new StringBuilder(streamsRestUrl.length() + 1 + suffix.length());
        sb.append(streamsRestUrl);
        if (!streamsRestUrl.endsWith("/")) {
            sb.append('/');
        }
        sb.append(suffix);
        String streamsResourcesUrl = sb.toString();
        return streamsResourcesUrl;
    }

    static JsonObject getServiceResources(String authorization,
            String url)throws IOException {
        JsonObject resources = StreamsRestUtils.getGsonResponse(createExecutor(),
                authorization, url);
        if (null == resources) {
            throw new IllegalStateException("Missing resources for service");
        }
        return resources;
    }
    /**
     * Given a token request response, return the access token.
     * @param tokenResponse The response from an earlier call to {@link getToken}
     * @return The access token, or null.
     */
    static String getToken(JsonObject tokenResponse) {
        String token = null;
        if (tokenResponse.has(MEMBER_ACCESS_TOKEN)) {
            token = tokenResponse.get(MEMBER_ACCESS_TOKEN).getAsString();
        }
        return token;
    }

    /**
     * Given a token request response, return the expiry time as milliseconds
     * since the epoch, with default padding before the final expiry deadline.
     * @param tokenResponse The response from an earlier call to {@link getToken}
     * @return An expiry time, or 0
     */
    static long getTokenExpiryMillis(JsonObject tokenResponse) {
        return getTokenExpiryMillis(tokenResponse, EXPIRY_PAD_MS);
    }

    /**
     * Given a token request response, return the expiry time as milliseconds
     * since the epoch, with padding before the final expiry deadline.
     * @param tokenResponse The response from an earlier call to {@link getToken}
     * @return An expiry time, or 0
     */
    static long getTokenExpiryMillis(JsonObject tokenResponse, long padMillis) {
        long expiryMillis = 0;
        if (tokenResponse.has(MEMBER_EXPIRATION)) {
            expiryMillis = tokenResponse.get(MEMBER_EXPIRATION).getAsLong() * MS
                    - padMillis;
        }
        return expiryMillis;
    }

    /**
     * Request a token from an IAM server.
     * @param iamUrl The URL of the IAM server's token service
     * @param apiKey The API key to use for the token request
     * @return The response from the server.
     */
    static JsonObject getTokenResponse(String iamUrl, String apiKey) {
        try (CloseableHttpClient httpClient = StreamsRestUtils.createHttpClient()) {
            String key = URLEncoder.encode(apiKey, StandardCharsets.UTF_8.name());
            StringBuilder sb = new StringBuilder(iamUrl.length()
                    + TOKEN_PARAMS.length() + key.length());
            sb.append(iamUrl);
            sb.append(TOKEN_PARAMS);
            sb.append(key);
            HttpPost httpPost = new HttpPost(sb.toString());
            httpPost.addHeader("Content-Type", ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
            httpPost.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());

            return StreamsRestUtils.getGsonResponse(httpClient, httpPost);
        } catch (IOException e) {
        }

        return null;
    }

    /**
     * Get the IAM token URL.
     * @param credentials Service credentials
     * @return The IAM token URL.
     */
    static String getTokenUrl(JsonObject credentials) {
        // Default to the IBM Cloud production URL
        String iamUrl = "https://iam.cloud.ibm.com/oidc/token";
        if (credentials.has(MEMBER_V2_REST_URL)) {
            try {
                URL restUrl = new URL(jstring(credentials, MEMBER_V2_REST_URL));
                String host = restUrl.getHost();
                
                // See if it's a test or staging environment.
                if (host.endsWith(".test.cloud.ibm.com") || host.matches(".*\\.stage1.*\\.bluemix\\.net"))
                    iamUrl = "https://iam.test.cloud.ibm.com/oidc/token";              
            } catch (MalformedURLException ignored) {}
        }
        return iamUrl;
    }

    // Construct the constant token request parameters. URLEncoder will never
    // throw because UTF-8 is a required charset.
    private static String genTokenParams() {
        try {
            String grantParam = "?grant_type=";
            String grantType = URLEncoder.encode("urn:ibm:params:oauth:grant-type:apikey", StandardCharsets.UTF_8.name());
            String apikeyParam = "&apikey=";
            StringBuilder sb = new StringBuilder(grantParam.length()
                    + grantType.length() + apikeyParam.length());
            sb.append(grantParam);
            sb.append(grantType);
            sb.append(apikeyParam);
            return sb.toString();
        } catch (UnsupportedEncodingException never) {
            // Can't happen since UTF-8 is always supported
        }
        return null;
    }

    // TODO: unify error handling between this and getResponseString()
    private static JsonObject gsonFromResponse(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
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
