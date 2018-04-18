/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

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

    /**
     * Java virtual machine arguments.
     * These arguments are added to all invocations of
     * Java virtual machines for the topology. For any
     * SPL invoked operators the invocation must have
     * been through methods in {@link com.ibm.streamsx.topology.spl.JavaPrimitive}.
     * <BR>
     * Setting the classpath is not supported.
     * <P>
     * For example, setting the maximum heap memory to 2GB:
     * <pre>
     * <code>
     *    List&lt;String> vmArgs = new ArrayList&lt;>();
     *    vmArgs.add("-Xmx2048m");
     *    config.put(ContextProperties.VMARGS, vmArgs);
     * </code>
     * </pre>
     * <BR>
     * Argument is a {@code List<String>}.
     * </P>
     */
    String VMARGS = "topology.vmArgs";
    
    /**
     * Keep any intermediate artifacts.
     * By default intermediate artifacts are deleted
     * after submission of a topology. For example when
     * create an IBM Streams application bundle the
     * intermediate SPL code and toolkit are deleted.
     * Keeping the artifacts can aid in debugging.
     * Argument is a {@code Boolean}.
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
     * See {@link com.ibm.streamsx.topology.Topology#createSubmissionParameter(String, Class)}
     */
    String SUBMISSION_PARAMS = "topology.submissionParams";

    /**
     * Flag to be supplied to force the compilation to occur on the
     * build service associated with a Streaming Analytics service.
     * Currently only be used in
     * conjunction with the {@code STREAMING_ANALYTICS_SERVICE} context.
     * <p>
     * Its values can be {@code true} and {@code false}. If true,
     * it will force remote compilation when possible. If false
     * (or unset), the parameter will have no impact.
     * <p>
     * If the {@code FORCE_REMOTE} parameter is supplied with a
     * value of {@code true}, the {@code SERVICE_NAME} and 
     * {@code VCAP_SERVICES} parameters must also supplied. 
     * Otherwise, an error will be thrown.
     */
    String FORCE_REMOTE_BUILD = "topology.forceRemoteBuild";
}
