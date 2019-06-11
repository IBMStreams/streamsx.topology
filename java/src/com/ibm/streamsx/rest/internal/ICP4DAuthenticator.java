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
    
    public static ICP4DAuthenticator of(String urlS) throws MalformedURLException, UnsupportedEncodingException {
        
        URL streamsUrl = new URL(urlS);
        
        String instancePath = streamsUrl.getPath();
        String name = instancePath.substring(instancePath.lastIndexOf('/') + 1);
        
        URL authorizeUrl = new URL("https", streamsUrl.getHost(), 31843,
                "/icp4d-api/v1/authorize");
        
        URL detailsUrl = new URL("https", streamsUrl.getHost(), 31843,
                "/zen-data/v2/serviceInstance/details?displayName=" + URLEncoder.encode(name, StandardCharsets.UTF_8.name()));
        
        URL serviceTokenUrl = new URL("https", streamsUrl.getHost(), 31843,
                "/zen-data/v2/serviceInstance/token");
        
        return new ICP4DAuthenticator(streamsUrl, authorizeUrl, detailsUrl, serviceTokenUrl);
    }
    
    private final URL streamsUrl;
    private final URL authorizeUrl;
    private final URL detailsUrl;
    private final URL serviceTokenUrl;
    private String serviceAuth;
    private long expire;
    
    ICP4DAuthenticator(URL streamsUrl, URL authorizeUrl, URL detailsUrl, URL serviceTokenUrl) {
        this.streamsUrl = streamsUrl;
        this.authorizeUrl = authorizeUrl;
        this.detailsUrl = detailsUrl;
        this.serviceTokenUrl = serviceTokenUrl;
    }
    
    public JsonObject config(Executor executor) throws IOException {
        
        JsonObject namepwd = new JsonObject();
        String[] userPwd = Util.getDefaultUserPassword();
        namepwd.addProperty("username", userPwd[0]);
        namepwd.addProperty("password", userPwd[1]);
        Request post = Request.Post(authorizeUrl.toExternalForm())         
                .bodyString(namepwd.toString(), ContentType.APPLICATION_JSON);
               
        JsonObject resp = RestUtils.requestGsonResponse(executor, post);
        String icp4dToken = GsonUtilities.jstring(resp, "token");
        
        String icpdAuth = RestUtils.createBearerAuth(icp4dToken);
        resp = RestUtils.getGsonResponse(executor, icpdAuth, detailsUrl);
        
        JsonObject sro = object(resp, "requestObj");
        String serviceId = jstring(sro, "ID");
        
        JsonObject sca = object(sro, "CreateArguments");
        
        JsonObject pd = new JsonObject();
        pd.addProperty("serviceInstanceId", serviceId);
        
        post = Request.Post(serviceTokenUrl.toExternalForm())         
                .addHeader("Authorization", icpdAuth)
                .bodyString(pd.toString(), ContentType.APPLICATION_JSON);
            
        resp = RestUtils.requestGsonResponse(executor, post);
        String serviceToken = jstring(resp, "AccessToken");
        
        serviceAuth = RestUtils.createBearerAuth(serviceToken);
        expire = System.currentTimeMillis() + 19 * 60;
        
        JsonObject sci = object(sca, "connection-info");
        URL buildEndpoint = new URL(jstring(sci, "externalBuildEndpoint"));
        
        // Ensure the build endpoint matches the fully external Streams URL
        URL buildUrl = new URL("https", streamsUrl.getHost(),
                buildEndpoint.getPort(), buildEndpoint.getPath());
       
        // Return object matches one created in Python.
        JsonObject connInfo = new JsonObject();
        connInfo.addProperty("externalClient", true);
        connInfo.addProperty("serviceRestEndpoint", streamsUrl.toExternalForm());
        connInfo.addProperty("serviceBuildEndpoint", buildUrl.toExternalForm());

        JsonObject cfg = new JsonObject();
      
        cfg.addProperty("type", "streams");
        cfg.add("connection_info", connInfo);
        cfg.addProperty("service_token", serviceToken);
        cfg.addProperty("cluster_ip", streamsUrl.getHost());
        cfg.addProperty("service_id", serviceId);
        
        return cfg;
    }

    @Override
    public String apply(Executor executor) {
        
        
        return null;
    }
    
    
}
