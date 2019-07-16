/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019 
 */
package com.ibm.streamsx.rest.internal;

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
    
    public JsonObject config(Executor executor) throws IOException {
        
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
        
        
        URL buildEndpoint = new URL(jstring(sci, "externalBuildEndpoint"));
        
        // Ensure the build endpoint matches the fully external ICP4D URL
        URL buildUrl = new URL("https", icpdUrl.getHost(),
                buildEndpoint.getPort(), buildEndpoint.getPath());
        
        URL streamsEndpoint = new URL(jstring(sci, "externalRestEndpoint"));
        
        // Ensure the build endpoint matches the fully external ICP4D URL
        URL streamsUrl = new URL("https", icpdUrl.getHost(),
                streamsEndpoint.getPort(), streamsEndpoint.getPath());
        
        JsonObject instance = object(sca,  "metadata", "instance");
        String serviceName = jstring(instance, "id");
       
        // Return object matches one created in Python.
        JsonObject connInfo = new JsonObject();
        connInfo.addProperty("externalClient", true);
        connInfo.addProperty("serviceRestEndpoint", streamsUrl.toExternalForm());
        connInfo.addProperty("serviceBuildEndpoint", buildUrl.toExternalForm());

        JsonObject cfg = new JsonObject();
      
        cfg.addProperty("type", "streams");
        cfg.add("connection_info", connInfo);
        cfg.addProperty("service_token", serviceToken);
        cfg.addProperty("service_name", serviceName);
        cfg.addProperty("cluster_ip", icpdUrl.getHost());
        cfg.addProperty("cluster_port", icpdUrl.getPort());
        cfg.addProperty("service_id", serviceId);
        
        return cfg;
    }

    @Override
    public String apply(Executor executor) {
        
        
        return null;
    }
    
    
}
