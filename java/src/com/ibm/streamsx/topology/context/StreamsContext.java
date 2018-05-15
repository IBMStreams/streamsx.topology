/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

import java.util.Map;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.remote.RemoteContext;

/**
 * A {@code StreamsContext} provides the ability to turn
 * a {@link Topology} into an executable.
 * The {@link #getType() type} of the context determines
 * the form of the executable, see {@link Type}.
 * 
 * @param <T>
 *            Type of value returned from submit
 */
public interface StreamsContext<T> {

    /**
     * Types of the {@link StreamsContext IBM Streams context} that a
     * {@link Topology} can be executed against.
     * 
     */
    public enum Type {

        /**
         * Topology is executed within the Java virtual machine that declared
         * it. This requires that the topology only contains Java functions
         * or primitive operators.
         * 
         */
        EMBEDDED,

        /**
         * Execution of the topology produces the application as a Streams
         * toolkit.
         * <P>
         * The returned type for the {@code submit} calls is
         * a {@code Future&lt;File>} where the value is
         * the location of the toolkit.
         * <BR>
         * The {@code Future} returned from {@code submit()} will
         * always be complete when the {@code submit()} returns.
         * </P>
         */
        TOOLKIT,
        
        /**
         * Execution of the topology produces the application a
         * Streams build archive.
         * <P>
         * The returned type for the {@code submit} calls is
         * a {@code Future&lt;File>} where the value is
         * the location of the build archive.
         * <BR>
         * The {@code Future} returned from {@code submit()} will
         * always be complete when the {@code submit()} returns.
         * </P>
         */
        BUILD_ARCHIVE,

        /**
         * Submission of the topology produces an Streams application bundle.
         * <P>
         * A bundle ({@code .sab} file) can be submitted to a Streaming Analytics
         * service running on IBM Cloud using:
         * <UL>
         * <LI> Streaming Analytics Console</LI>
         * <LI> Streaming Analytics REST API </LI>
         * </UL>
         * <BR>
         * The {@link #STREAMING_ANALYTICS_SERVICE} context submits a topology directly to a
         * Streaming Analytics service.
         * </P>
         * <P>
         * A bundle ({@code .sab} file) can be submitted to an IBM Streams
         * instance using:
         * <UL>
         * <LI> {@code streamtool submitjob} from the command line</LI>
         * <LI> IBM Streams Console</LI>
         * <LI> IBM Streams JMX API </LI>
         * </UL>
         * <BR>
         * The {@link #DISTRIBUTED} context submits a topology directly to a Streams instance.
         * </P>
         * <P>
         * The returned type for the {@code submit} calls is
         * a {@code Future<File>} where the value is
         * the location of the bundle.
         * <BR>
         * If running with IBM Streams 4.2 or later then additionally a
         * <a href="https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/job_configuration_overlays.html">job configuration overlays</a>
         * file is produced. This file provides the correct job deployment instructions
         * to enforce any constraints declared in the {@code Topology}. The file is located
         * in the same directory as the application bundle with a suffix of {@code json} and
         * the name of the application bundle file (without the {@code sab} suffix} with {@code _JobConfig}
         * appended. For example for the application bundle {@code simple.HelloWorld.sab}
         * its job configuration overlays file would be {@code simple.HelloWorld_JobConfig.json}.
         * <BR>
         * <pre>
         * Example of using job configuration overlays file at submit job time with {@code streamtool}:
         * <code>
         * streamtool submitjob --jobConfig simple.HelloWorld_JobConfig.json simple.HelloWorld.sab
         * </code>
         * </pre>
         * The {@code Future} returned from {@code submit()} will
         * always be complete when the {@code submit()} returns.
         * </P>
         */
        BUNDLE,

        /**
         * Execution of the topology produces an SPL application bundle
         * {@code .sab} file that can be executed as an IBM Streams
         * standalone application.
         * <P>
         * The returned type for the {@code submit} calls is
         * a {@code Future&lt;File>} where the value is
         * the location of the bundle.
         * <BR>
         * The {@code Future} returned from {@code submit()} will
         * always be complete when the {@code submit()} returns.
         * </P>
         */
        STANDALONE_BUNDLE,

        /**
         * The topology is executed directly as an Streams standalone application.
         * The standalone execution is spawned as a separate process from the
         * Java virtual machine.
         * <P>
         * The returned type for the {@code submit} calls is
         * a {@code Future&lt;Integer>} where the value is
         * return code from the standalone execution.
         * <BR>
         * The {@code Future} returned from {@code submit()} will be complete,
         * when the standalone executable terminates. This will only occur
         * if all the streams in the topology finalize, so that all the operators
         * complete. Thus some topologies, for example those polling external
         * data sources, will run until cancelled.
         * <BR>
         * Calling {@code Future.cancel(true)} will terminate the standalone executable.
         * </P>
         */
        STANDALONE,

        /**
         * The topology is submitted to an IBM Streams instance.
         * <P>
         * The returned type for the {@code submit} calls is
         * a {@code Future&lt;BigInteger>} where the value is
         * the job identifier.
         * <BR>
         * When {@code submit} returns the {@code Future} will be complete,
         * but the Streams job will still be running, as typically distributed
         * jobs are long running, consuming continuous streams of data.
         * </P>
         * <P>
         * The Streams instance the topology is submitted to is defined by
         * the <b>required</b> environment variables {@code STREAMS_DOMAIN_ID} and
         * {@code STREAMS_INSTANCE_ID}. {@code STREAMS_ZKCONNECT} must also be set for
         * a non-basic instance, one not using embedded ZooKeeper.
         * <BR>
         * The user is set by the <em>optional</em> environment variable {@code STREAMS_USERNAME}
         * defaulting to the current operator system user name. 
         * </P>
         * <P>
         * {@code streamtool} is used to submit the job
         * and requires that {@code streamtool} does not prompt for authentication.
         * This is achieved by using {@code streamtool genkey}.
         * @see <a href="https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.1/com.ibm.streams.cfg.doc/doc/ibminfospherestreams-user-security-authentication-rsa.html">Generating authentication keys for IBM Streams</a>
         * </P>
         */
        DISTRIBUTED,

        /**
         * Testing variant of {@link #EMBEDDED}.
         * This allows testing frameworks
         * to capture the output of unconnected {@link com.ibm.streamsx.topology.TStream streams}
         * in the topology using the facilities of {@link com.ibm.streamsx.topology.tester.Tester}.
         * This allows {@code JUnit} style testing of topologies, including individual functional
         * streams, and sub-topologies.
         */
        EMBEDDED_TESTER,

        /**
         * Testing variant of {@link #STANDALONE}.
         * This allows testing frameworks
         * to capture the output of unconnected {@link com.ibm.streamsx.topology.TStream streams}
         * in the topology using the facilities of {@link com.ibm.streamsx.topology.tester.Tester}.
         * This allows {@code JUnit} testing of topologies, including individual functional
         * streams, sub-topologies, and, SPL primitive operators and composites.
         */
        STANDALONE_TESTER,
        
        /**
         * Testing variant of {@link #DISTRIBUTED_TESTER}.
         * This allows testing frameworks
         * to capture the output of unconnected {@link com.ibm.streamsx.topology.TStream streams}
         * in the topology using the facilities of {@link com.ibm.streamsx.topology.tester.Tester}.
         * This allows {@code JUnit} testing of topologies, including individual functional
         * streams, sub-topologies, and, SPL primitive operators and composites.
         */
        DISTRIBUTED_TESTER,
              
        /**
         * The topology is submitted to a Streams instance running
         * in Streaming Analytics service on
         * <a href="http://www.ibm.com/Bluemix" target="_blank">IBM Cloud</a>
         * cloud platform.
         * 
         * <P>
         * This is a synonym for {@link #STREAMING_ANALYTICS_SERVICE}.
         * </P>
         */
        ANALYTICS_SERVICE,
        

        /**
         * The topology is submitted to a Streams instance running
         * in Streaming Analytics service on
         * <a href="http://www.ibm.com/Bluemix" target="_blank">IBM Cloud</a>
         * cloud platform.
         * <P>
         * The returned type for the {@code submit} calls is
         * a {@code Future<BigInteger>} where the value is
         * the job identifier.
         * <BR>
         * When {@code submit} returns the {@code Future} will be complete,
         * but the Streams job will still be running, as typically distributed
         * jobs are long running, consuming continuous streams of data.
         * </P>
         * <P>
         * The name of the Streaming Analytics Service must be set as a
         * configuration property for a submit, using {@link AnalyticsServiceProperties#SERVICE_NAME}.
         * The definition for the service is defined one of two ways:
         * <UL>
         * <LI>Using the configuration property {@link AnalyticsServiceProperties#VCAP_SERVICES}.
         * This takes precedence over the environment variable.</LI>
         * <LI>Using the environment variable {@code VCAP_SERVICES}</LI>
         * </UL>
         * </P>
         * <P>
         * If the environment variable {@code STREAMS_INSTALL} is set to a non-empty value
         * then a Streams Application bundle ({@code sab} file) is created locally using
         * the IBM Streams install and submitted to the service. This may be overridden
         * by setting the context property {@link ContextProperties#FORCE_REMOTE_BUILD FORCE_REMOTE_BUILD}
         * to {@code true}.
         * <.P>
         */
        STREAMING_ANALYTICS_SERVICE,
        
        /**
         * Testing variant of {@link #STREAMING_ANALYTICS_SERVICE}.
         * <P>
         * This context ignores any local IBM Streams install defined by the
         * environment variable {@code STREAMS_INSTALL}, thus
         * the Streams Application bundle ({@code sab} file) is created using
         * the build service.
         * </P>
         * @since 1.7
         */
        STREAMING_ANALYTICS_SERVICE_TESTER,
        ;
    }

    /**
     * The type of this context.
     * @return type of this context.
     */
    Type getType();
    
    /**
     * Answers if this StreamsContext supports execution of the
     * {@code topology}.
     * @see Type#EMBEDDED
     * @param topology Topology to evaluate.
     * @return true if this context supports execution of the topology.
     */
    boolean isSupported(Topology topology);

    /**
     * Submit {@code topology} to this Streams context.
     * @param topology Topology to be submitted.
     * @return Future for the submission, see the descriptions for the {@link Type}
     * returned by {@link #getType()} for details on what the encapsulated returned
     * value represents.
     * @throws Exception Exception submitting the topology.
     */
    Future<T> submit(Topology topology) throws Exception;

    /**
     * Submit {@code topology} to this Streams context with a specific configuration.
     * @param topology Topology to be submitted.
     * @param config Configuration to be used for the submission, may be modified by this method.
     * @return Future for the submission, see the descriptions for the {@link Type}
     * returned by {@link #getType()} for details on what the encapsulated returned
     * value represents.
     * @throws Exception Exception submitting the topology.
     * 
     * @see ContextProperties
     */
    Future<T> submit(Topology topology, Map<String, Object> config)
            throws Exception;
    
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
    
    String SUBMISSION_DEPLOY = RemoteContext.SUBMISSION_DEPLOY;
    String SUBMISSION_GRAPH = RemoteContext.SUBMISSION_GRAPH;
    String SUBMISSION_RESULTS = RemoteContext.SUBMISSION_RESULTS;
    String SUBMISSION_RESULTS_FILE = RemoteContext.SUBMISSION_RESULTS_FILE;
    
}
