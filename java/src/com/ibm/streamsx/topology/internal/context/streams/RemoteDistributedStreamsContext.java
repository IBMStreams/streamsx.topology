/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;

import java.io.IOException;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.rest.internal.ICP4DAuthenticator;
import com.ibm.streamsx.rest.internal.StandaloneAuthenticator;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.context.RemoteContextForwarderStreamsContext;
import com.ibm.streamsx.topology.internal.context.JSONStreamsContext.AppEntity;
import com.ibm.streamsx.topology.internal.context.streamsrest.DistributedStreamsRestContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.StreamsKeys;
import com.ibm.streamsx.topology.internal.streams.Util;

/**
 * Context that submits the SPL to the Stream build service
 * for a remote build.
 * 
 * Delegates to DistributedStreamsRestContext.
 * 
 */
public final class RemoteDistributedStreamsContext extends RemoteContextForwarderStreamsContext<BuildService> {

    public RemoteDistributedStreamsContext() {
        super(new DistributedStreamsRestContext());
    }

    @Override
    public com.ibm.streamsx.topology.context.StreamsContext.Type getType() {
        return StreamsContext.Type.DISTRIBUTED;
    }
    
    protected void createSubmission(AppEntity entity) throws Exception {
        super.createSubmission(entity);
        setSubmissionInstance(entity);
    }

    private static final String CONNECTION_INFO = "connection_info";
    private static final String SERVICE_BUILD_ENDPOINT = "serviceBuildEndpoint";

    static Instance getConfigInstance(AppEntity entity) {
        
        Map<String,Object> config = entity.config;
        if (config != null && config.containsKey(ContextProperties.STREAMS_INSTANCE)) {       
            Object instance = config.get(ContextProperties.STREAMS_INSTANCE);         
            if (instance instanceof Instance)
                return (Instance) instance;
        }
        return null;
    }

    static String getConfigBuildServiceUrl(AppEntity entity) {
        Map<String,Object> config = entity.config;
        if (config != null && config.containsKey(ContextProperties.BUILD_SERVICE_URL)) {       
            Object url = config.get(ContextProperties.BUILD_SERVICE_URL);         
            if (url instanceof String)
                return (String) url;
        }
        return null;
    }

    static void setSubmissionInstance(AppEntity entity) throws IOException {
    
        Instance cfgInstance = getConfigInstance(entity);
        if (cfgInstance != null) {
            StreamsConnection sc = cfgInstance.getStreamsConnection();
            boolean verify = cfgInstance.getStreamsConnection().isVerify();
            JsonObject deploy = deploy(entity.submission);
            Object authenticatorO = sc.getAuthenticator();
            deploy.addProperty(ContextProperties.SSL_VERIFY, verify);
            JsonObject service;
            if (authenticatorO instanceof ICP4DAuthenticator) {
                ICP4DAuthenticator authenticator = (ICP4DAuthenticator) authenticatorO;
                service = authenticator.config(verify);
            } else if (authenticatorO instanceof StandaloneAuthenticator) {
                StandaloneAuthenticator authenticator = (StandaloneAuthenticator) authenticatorO;
                service = authenticator.config(verify);
                String buildServiceUrl = getConfigBuildServiceUrl(entity);
                if (buildServiceUrl == null) {
                    buildServiceUrl = System.getenv(Util.STREAMS_BUILD_URL);
                }
                if (buildServiceUrl != null) {
                    // Copy so we don't affect instance. Version of gson we
                    // use lacks deepCopy() so we serialize / parse to copy.
                    String json = service.toString();
                    service = new JsonParser().parse(json).getAsJsonObject();
                    JsonObject connInfo = service.getAsJsonObject(CONNECTION_INFO);
                    if (connInfo.has(SERVICE_BUILD_ENDPOINT)) {
                        connInfo.remove(SERVICE_BUILD_ENDPOINT);
                    }
                    connInfo.addProperty(SERVICE_BUILD_ENDPOINT, buildServiceUrl);
                }
            } else {
                throw new IllegalStateException(
                        "Invalid Instance for Streams V5: " + cfgInstance);
            }
            deploy.add(StreamsKeys.SERVICE_DEFINITION, service);
        }
    }
}
