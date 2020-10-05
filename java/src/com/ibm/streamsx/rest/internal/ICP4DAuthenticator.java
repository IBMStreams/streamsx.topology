/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019 
 */
package com.ibm.streamsx.rest.internal;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.context.streamsrest.StreamsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.streams.Util;

public class ICP4DAuthenticator implements Function<Executor,String> {

    public static ICP4DAuthenticator of(String urlS, String instanceName, String user, String password) throws MalformedURLException, UnsupportedEncodingException {

        if (urlS == null)
            urlS = Util.getenv(Util.ICP4D_DEPLOYMENT_URL);
        if (instanceName == null)
            instanceName = Util.getenv(Util.STREAMS_INSTANCE_ID);

        URL icpdUrl = new URL(urlS);

        URL authorizeUrl = new URL("https", icpdUrl.getHost(), icpdUrl.getPort(),
                "/icp4d-api/v1/authorize");

        URL detailsUrl = new URL("https", icpdUrl.getHost(), icpdUrl.getPort(),
                "/zen-data/v2/serviceInstance/details?displayName=" + URLEncoder.encode(instanceName, StandardCharsets.UTF_8.name()));

        URL serviceTokenUrl = new URL("https", icpdUrl.getHost(), icpdUrl.getPort(),
                "/zen-data/v2/serviceInstance/token");
System.out.println(detailsUrl);
        return new ICP4DAuthenticator(icpdUrl, authorizeUrl, detailsUrl, serviceTokenUrl, instanceName, user, password);
    }



    private final URL icpdUrl;
    private final URL authorizeUrl;
    private final URL detailsUrl;
    private final URL serviceTokenUrl;
    private final String instanceName;
    private final String user;
    private final String password;

    private String serviceAuth;
    private long expire;

    private JsonObject cfg;

    ICP4DAuthenticator(URL icpdUrl, URL authorizeUrl, URL detailsUrl, URL serviceTokenUrl,
            String instanceName, String user, String password) {
        this.icpdUrl = icpdUrl;
        this.authorizeUrl = authorizeUrl;
        this.detailsUrl = detailsUrl;
        this.serviceTokenUrl = serviceTokenUrl;
        this.instanceName = instanceName;
        this.user = user;
        this.password = password;
    }

    /** creates a vaild external URL from an external endpoint from the connection-info JSON structure */
    private URL urlFromEndPoint(String externalEndpoint) throws MalformedURLException {
        URL url = null;
        try {
            url = new URL(externalEndpoint);
            // here we should end with CPD < 2.5
        } catch (MalformedURLException e) {
            // CPD 2.5 switched to path-absolute; not a valid URL; create a valid URL with externalEndpoint as the path
            url = new URL("https", this.icpdUrl.getHost(), this.icpdUrl.getPort(), externalEndpoint);
        }
        // Ensure the build endpoint matches the fully external ICP4D URL
        URL ret = new URL("https", this.icpdUrl.getHost(), url.getPort(), url.getPath());
        return ret;
    }

    public JsonObject config(boolean verify) throws IOException {
        if (cfg != null)
            return cfg;

        Executor executor = RestUtils.createExecutor(!verify);

        JsonObject namepwd = new JsonObject();
        String[] userPwd = Util.getDefaultUserPassword(user, password);
        namepwd.addProperty("username", userPwd[0]);
        namepwd.addProperty("password", userPwd[1]);
        Request post = Request.Post(authorizeUrl.toExternalForm())         
                .bodyString(namepwd.toString(), ContentType.APPLICATION_JSON);

        JsonObject resp = RestUtils.requestGsonResponse(executor, post);
        String icp4dToken = GsonUtilities.jstring(resp, "token");

        String icpdAuth = RestUtils.createBearerAuth(icp4dToken);

        String serviceId = null;
        JsonObject sci = null;
        JsonObject sca = null;
        // Occasionally see null for connection-info.
        for (int i = 0; i < 5; i++) {
            resp = RestUtils.getGsonResponse(executor, icpdAuth, detailsUrl);

            JsonObject sro = object(resp, "requestObj");
            serviceId = jstring(sro, "ID");

            sca = object(sro, "CreateArguments");

            sci = object(sca, "connection-info");
            if (sci != null && !sci.entrySet().isEmpty())
                break;
            sci = null;
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                break;
            }

        }
        if (sci == null)
            throw new IllegalStateException("Unable to retrieve connection details for Streams instance: " + instanceName);

        JsonObject pd = new JsonObject();
        pd.addProperty("serviceInstanceId", serviceId);
        post = Request.Post(serviceTokenUrl.toExternalForm())         
                .addHeader("Authorization", icpdAuth)
                .bodyString(pd.toString(), ContentType.APPLICATION_JSON);

        resp = RestUtils.requestGsonResponse(executor, post);
        String serviceToken = jstring(resp, "AccessToken");

        serviceAuth = RestUtils.createBearerAuth(serviceToken);
        expire = System.currentTimeMillis() + 19 * 60;

        JsonObject connInfo = new JsonObject();

        final String externalBuildPoolsEndpoint = jstring(sci, "externalBuildPoolsEndpoint");
        if (externalBuildPoolsEndpoint != null) {
            URL buildPoolsUrl = urlFromEndPoint(externalBuildPoolsEndpoint);
            connInfo.addProperty("serviceBuildPoolsEndpoint", buildPoolsUrl.toExternalForm());
        }
        final String externalBuildEndpoint = jstring(sci, "externalBuildEndpoint");
        URL buildUrl = urlFromEndPoint(externalBuildEndpoint);
        
        final String externalRestResourceEndpoint = jstring(sci, "externalRestResourceEndpoint");
        if (externalRestResourceEndpoint != null) {
            URL restResourceUrl = urlFromEndPoint(externalRestResourceEndpoint);
            connInfo.addProperty (StreamsKeys.STREAMS_REST_RESOURCES_ENDPOINT, restResourceUrl.toExternalForm());
        }

        final String externalRestEndpoint = jstring(sci, "externalRestEndpoint");
        URL streamsUrl = urlFromEndPoint(externalRestEndpoint);

        JsonObject instance = object(sca,  "metadata", "instance");
        String serviceName = jstring(instance, "id");

        // Return object matches one created in Python.
        connInfo.addProperty("serviceRestEndpoint", streamsUrl.toExternalForm());
        connInfo.addProperty("serviceBuildEndpoint", buildUrl.toExternalForm());
        JsonObject cfg = new JsonObject();

        cfg.addProperty("type", "streams");
        cfg.addProperty("externalClient", true);
        cfg.add("connection_info", connInfo);
        cfg.addProperty("service_token", serviceToken);
        cfg.addProperty("service_token_expire", expire);
        cfg.addProperty("service_name", serviceName);
        cfg.addProperty("cluster_ip", icpdUrl.getHost());
        cfg.addProperty("cluster_port", icpdUrl.getPort());
        cfg.addProperty("service_id", serviceId);
        this.cfg = cfg;
        return cfg;
    }

    public static ICP4DAuthenticator of(JsonObject service) throws MalformedURLException, UnsupportedEncodingException {

        String serviceName = jstring(service, "service_name");
        final ICP4DAuthenticator auth;
        if (jboolean(service, "externalClient")) {
            // Set externally
            String cpd_host = jstring(service, "cluster_ip");
            int cpd_port = GsonUtilities.jint(service, "cluster_port");
            URL cpd_url = new URL("https", cpd_host, cpd_port, "");
            auth = ICP4DAuthenticator.of(cpd_url.toExternalForm(), serviceName, (String) null, (String) null);
        } else {
            auth = new ICP4DAuthenticator(null, null, null, null, serviceName, null, null);
        }

        String serviceToken = jstring(service, "service_token");

        if (serviceToken != null) {
            auth.serviceAuth = RestUtils.createBearerAuth(serviceToken);
            auth.expire = service.get("service_token_expire").getAsLong();
        }
        auth.cfg = service;
        return auth;
    }

    @Override
    public String apply(Executor executor) {   
        return serviceAuth;
    }  
}
