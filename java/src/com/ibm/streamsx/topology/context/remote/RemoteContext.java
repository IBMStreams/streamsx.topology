package com.ibm.streamsx.topology.context.remote;

import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;

public interface RemoteContext<T> {
    /**
     * Types of the {@link StreamsContext IBM Streams context} that a
     * {@link Topology} can be executed against.
     * 
     */
    public enum Type {
        TOOLKIT,        
        ZIPPED_TOOLKIT,
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
     * 
     * @see ContextProperties
     */
    Future<T> submit(JsonObject submission) throws Exception;
}
