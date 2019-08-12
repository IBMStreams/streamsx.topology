/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;

import java.io.IOException;
import java.util.Map;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.rest.internal.ICP4DAuthenticator;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.context.RemoteContextForwarderStreamsContext;
import com.ibm.streamsx.topology.internal.context.JSONStreamsContext.AppEntity;
import com.ibm.streamsx.topology.internal.context.streamsrest.DistributedStreamsRestContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.StreamsKeys;

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

    static Instance getConfigInstance(AppEntity entity) {
        
        Map<String,Object> config = entity.config;
        if (config != null && config.containsKey(ContextProperties.STREAMS_INSTANCE)) {       
            Object instance = config.get(ContextProperties.STREAMS_INSTANCE);         
            if (instance instanceof Instance)
                return (Instance) instance;
        }
        return null;
    }
    
    static void setSubmissionInstance(AppEntity entity) throws IOException {
    
        Instance cfgInstance = getConfigInstance(entity);
        if (cfgInstance != null) {
            StreamsConnection sc = cfgInstance.getStreamsConnection();
            Object authenticatorO = sc.getAuthenticator();
            if (authenticatorO instanceof ICP4DAuthenticator) {
                ICP4DAuthenticator authenticator = (ICP4DAuthenticator) authenticatorO;

                boolean verify = cfgInstance.getStreamsConnection().isVerify();
                JsonObject deploy = deploy(entity.submission);
                deploy.add(StreamsKeys.SERVICE_DEFINITION,
                        authenticator.config(verify));
                deploy.addProperty(ContextProperties.SSL_VERIFY, verify);
            } else {
                throw new IllegalStateException(
                        "Invalid Instance for Streams V5: " + cfgInstance);
            }
        }
    }
}
