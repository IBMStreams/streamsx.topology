/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

import com.ibm.streamsx.topology.Topology;

/**
 * Properties that can be specified when submitting the topology to a context.
 * @see StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)
 * @see JobProperties
 */
public interface ContextProperties {

    /**
     * Location of the generated application directory
     * when creating a Streams application bundle.
     * If not supplied to the in the configuration passed into
     * {@link StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)}
     * then a unique location will be used, and placed into the configuration with this key.
     * The value should be a {@code String} that is the absolute path of the application directory.

     */
    String APP_DIR = "topology.applicationDir";

    /**
     * Location of the generated toolkit root.
     * If not supplied to the in the configuration passed into
     * {@link StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)}
     * then a unique location will be used, and placed into the configuration with this key.
     * The value should be a {@code String} that is the absolute path of the toolkit directory.
     */
    String TOOLKIT_DIR = "topology.toolkitDir";

    String BUNDLE = "topology.bundle";

    /**
     * Argument is a List of Strings.
     */
    String VMARGS = "topology.vmArgs";
    
    /**
     * Argument is a Boolean.
     */
    String KEEP_ARTIFACTS = "topology.keepArtifacts";

    /**
     * Trace level for a {@link StreamsContext.Type#DISTRIBUTED} or
     * {@link StreamsContext.Type#STANDALONE} application. Value is an instance
     * of {@code java.util.logging.Level} including instances of
     * {@code com.ibm.streams.operator.logging.TraceLevel}.
     */
    String TRACING_LEVEL = "topology.tracing";
    
    /**
     * Override IBM Streams install directory for
     * bundle compilation, defaults to $STREAMS_INSTALL.
     * Argument is a String.
     */
    String COMPILE_INSTALL_DIR = "topology.install.compile";
    
    /**
     * Submission parameters to be supplied to the topology when
     * submitted for {@code DISTRIBUTED}, {@code STANDALONE}
     * or {@code ANALYTIC_SERVICE} execution.
     * <p>
     * The property value is a {@code Map<String,Object>} where the key
     * is the parameter name and the value is the parameter value.
     * <p>
     * e.g.,
     * <pre>{@code
     * Supplier<Integer> topology.createSubmissionParameter("p1", 5);
     * ...
     * 
     * ContextProperties config = new HashMap<>();
     * Map<String,Object> params = new HashMap<>();
     * params.put("p1", 10);
     * config.put(SUBMISSION_PARAMS, params);
     * 
     * ... StreamsContextFactory.getStreamsContext(DISTRIBUTED)
     *              .submit(topology, config);
     * }</pre>
     * 
     * See {@link Topology#createSubmissionParameter(String, Class)}
     */
    String SUBMISSION_PARAMS = "topology.submissionParams";
}
