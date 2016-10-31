package com.ibm.streamsx.topology.context.remote;

import java.util.concurrent.Future;

import com.google.gson.JsonObject;

public interface RemoteContext<T> {
    String SUBMISSION_DEPLOY = "deploy";
    String SUBMISSION_GRAPH = "graph";
    
    /**
     * Types of the context that a
     * JSON graph can be executed against.
     * 
     */
    public enum Type {
        TOOLKIT,        
        BUILD_ARCHIVE,
        REMOTE_BUILD_AND_SUBMIT
    }
    
    
    /**
     * The type of this context.
     * @return type of this context.
     */
    Type getType();

    /**
     * Submit a topology} to this Streams context as a JSON object.
     * The JSON object contains two keys:
     * <UL>
     * <LI>{@code deploy} - Optional - Deployment information.</LI>
     * <LI>{@code graph} - Required - JSON representation of the topology graph.</LI>
     * </UL>
     * @param submission Topology and deployment info to be submitted.
     * @return Future for the submission, see the descriptions for the {@link Type}
     * returned by {@link #getType()} for details on what the encapsulated returned
     * value represents.
     * @throws Exception Exception submitting the topology.
     */
    Future<T> submit(JsonObject submission) throws Exception;
}
