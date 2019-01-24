package com.ibm.streamsx.topology.context.remote;

import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.google.gson.JsonObject;

public interface RemoteContext<T> {
	/**
     * Logger used for the Topology API, name {@code com.ibm.streamsx.topology}. Must be defined here,
     * in addition to {@link com.ibm.streamsx.topology.Topology}, since 
     * {@link com.ibm.streamsx.topology.Topology} depends on JSON4j.jar.
     */
    public static Logger REMOTE_LOGGER = Logger.getLogger("com.ibm.streamsx.topology");
	
    String SUBMISSION_DEPLOY = "deploy";
    String SUBMISSION_GRAPH = "graph";
    String SUBMISSION_RESULTS = "submissionResults";
    String SUBMISSION_RESULTS_FILE = "submissionResultsFile";
    
    /**
     * Types of the context that a
     * JSON graph can be executed against.
     * 
     */
    public enum Type {
        TOOLKIT,        
        BUILD_ARCHIVE,
        ANALYTICS_SERVICE,
        STREAMING_ANALYTICS_SERVICE,
        BUNDLE,
        DISTRIBUTED,
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
