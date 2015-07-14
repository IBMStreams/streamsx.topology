/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.Topology;

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
         * 
         */
        TOOLKIT,

        /**
         * Execution of the topology produces an SPL application bundle
         * {@code .sab} file that can be submitted to an IBM Streams
         * instances as a distributed application. The bundle is self-contained
         * and 
         * <P>
         * A bundle ({@code .sab} file) can be submitted to an IBM Streams
         * instance using:
         * <UL>
         * <LI> {@code streamtool submitjob} from the command line</LI>
         * <LI> IBM Streams Console</LI>
         * <LI> IBM Streams JMX api </LI>
         * </UL>
         * <BR>
         * Using the {@link #DISTRIBUTED} context allows the topology to
         * be submitted directly to a Streams instance.
         * </P>
         * <P>
         * The returned type for the {@code submit} calls is
         * a {@code Future&lt;File>} where the value is
         * the location of the bundle.
         * <BR>
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
         * The topology is submitted to a Streams instance.
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
         * This initial implementation uses {@code streamtool} to submit and cancel jobs,
         * and requires that {@code streamtool} does not prompt for authentication.
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
         * in Streaming Analytics Service on
         * <a href="http://www.ibm.com/Bluemix‎" target="_blank">IBM Bluemix</a>.
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
         * The name of the Streaming Analytics Service must be set as a
         * configuration property for a submit, using {@link AnalyticsServiceProperties#SERVICE_NAME}.
         * The definition for the service is defined one of two ways:
         * <UL>
         * <LI>Using the configuration property {@link AnalyticsServiceProperties#VCAP_SERVICES}.
         * This takes precedence over the environment variable.</LI>
         * <LI>Using the environment variable {@code VCAP_SERVICES}</LI>
         * </UL>
         * </P>
         */
        ANALYTICS_SERVICE,
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
}
