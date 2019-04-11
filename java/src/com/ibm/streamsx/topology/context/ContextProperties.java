/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2018
 */
package com.ibm.streamsx.topology.context;

import com.ibm.streamsx.rest.StreamsConnection;

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
    
    /**
     * Connection to IBM Streams REST api to be used for submission.
     * <BR>
     * Only supported for {@link StreamsContext.Type#DISTRIBUTED distributed}
     * and {@link StreamsContext.Type#STREAMING_ANALYTICS_SERVICE Streaming Analytics}
     * contexts.
     * <P>
     * The value in the configuration map must be an instance of
     * {@link StreamsConnection} and will be used for job submission.
     * </P>
     * <P>
     * For {@link StreamsContext.Type#DISTRIBUTED distributed} contexts the instance
     * to use is defined by either:
     * <UL>
     * <LI>the environment variable {@code STREAMS_INSTANCE_ID} if set</LI>
     * <LI>or the first instance returned by {@link StreamsConnection#getInstances()}.
     * It is recommended that this is only used when only a single instance is available
     * through the connection.
     * </UL>
     * </P>
     * 
     * @since 1.11
     */
    String STREAMS_CONNECTION = "topology.streamsConnection";
    
    /**
     * Set SSL certification verification state.
     * 
     * If set as {@code true} (the default) then SSL certificate verification
     * is enabled when using a REST connection to a IBM Streams distributed instance.
     * <BR>
     * Otherwise if set to {@code false} then SSL certification verification does
     * not occur. This is useful for test distributed instances or the IBM Streams
     * Quick Start edition where a self-signed certificate is used.
     * 
     * If a connection is passed in to a submission context using
     * {@link #STREAMS_CONNECTION} then this value is ignored.
     * 
     * @since 1.11
     */
    String SSL_VERIFY = "topology.SSLVerify";

    /**
     * Options to be passed to IBM Streams sc command.
     * <BR>
     * A topology is compiled into a Streams application
     * bundle ({@code sab}) using the SPL compiler {@code sc}.
     * <BR>
     * Additional options to be passed to {@code sc}
     * may be set using this key. The value can be a
     * single string option (e.g. {@code --c++std=c++11} to select C++ 11 compilation)
     * or a list of strings for multiple options.
     * 
     * <P>
     * Options that modify the requested submission context (e.g. setting a different
     * main composite) or deprecated options should not be specified.
     * </P>
     * 
     * @since 1.12.10
     * 
     * @see https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.3.0/com.ibm.streams.ref.doc/doc/sc.html
     */
    String SC_OPTIONS = "topology.sc.options";
}
