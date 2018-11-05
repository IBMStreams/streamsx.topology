/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015, 2018
 */
package com.ibm.streamsx.topology.tester;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.spl.SPLStream;

/**
 * A {@code Tester} adds the ability to test a topology in a test
 * framework such as JUnit. 
 * 
 * The main feature is the ability to capture tuples from a
 * {@link TStream} in order to perform some form of verification
 * on them. There are two mechanisms to perform verifications:
 * <UL>
 * <LI>{@link Condition} - Provides the ability to check if a common pattern is {@link Condition#valid() valid}, such as did the stream
 * produce the {@link #tupleCount(TStream, long) correct number of tuples}.</LI>
 * <LI>{@link Tester#splHandler(SPLStream, StreamHandler)  StreamHandler} - Provides the ability
 * to add an arbitrary handler to a stream, that will be called for every tuple on the stream. A number of implementations of
 * {@code StreamHandler} are in the {@code com.ibm.streams.flow.handlers} provided by the IBM Streams Java Operator API.</LI>
 * </UL>
 * The stream being verified must not be connected, but may have multiple conditions or handlers added.
 * <BR>
 * Currently, only streams that are instances of {@link SPLStream} or {@code TStream<String>} can have conditions
 * or handlers attached.
 * <P>
 * A {@code Tester} only modifies its {@link Topology} if the topology
 * is submitted to a tester {@link StreamsContext}, of type
 * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#EMBEDDED_TESTER},
 * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#STANDALONE_TESTER},
 * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#DISTRIBUTED_TESTER},
 * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#STREAMING_ANALYTICS_SERVICE_TESTER}.
 * </P>
 * <P>
 * When running a test using {@link com.ibm.streamsx.topology.context.StreamsContext.Type#STANDALONE_TESTER}
 * or {@link com.ibm.streamsx.topology.context.StreamsContext.Type#DISTRIBUTED_TESTER} a TCP server is setup
 * within the JVM running the test (e.g. the JVM running the JUnit tests} and the topology is modified to
 * send tuples from streams being tested to the TCP server. The port used by the TCP server is in the
 * ephemeral port range.
 * </P>
 */
public interface Tester {
    
    /**
     * System property to set the trace level of application under tests.
     * 
     * For jobs to submitted to {@link StreamsContext.Type#STANDALONE_TESTER},
     * {@link StreamsContext.Type#DISTRIBUTED_TESTER} and
     * {@link StreamsContext.Type#STREAMING_ANALYTICS_SERVICE_TESTER} contexts
     * setting this property overrides any default trace level or any trace
     * level set by a test.
     * <P>
     * The value should be one of the enumeration names of {@code java.util.logging.Level}.
     * <UL>
     * <LI>{@code SEVERE} - IBM Streams {@code error}.</LI>
     * <LI>{@code WARNING} - IBM Streams {@code warn}.</LI>
     * <LI>{@code INFO} - IBM Streams {@code info}.</LI>
     * <LI>{@code FINE} - IBM Streams {@code debug}.</LI>
     * <LI>{@code FINEST}, {@code ALL} - IBM Streams {@code trace}.</LI>
     * <LI>{@code OFF}- IBM Streams {@code off}.</LI>
     * </UL>
     * </P>
     * 
     * @since 1.11
     */
    String TEST_TRACE_LEVEL = "topology.tester.traceLevel";
    
    /**
     * Get the topology for this tester.
     * @return the topology for this tester.
     */
    Topology getTopology();

    /**
     * Adds {@code handler} to capture the output of {@code stream}.
     * 
     * <P>
     * Not supported when testing using
     * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#STREAMING_ANALYTICS_SERVICE_TESTER STREAMING_ANALYTICS_SERVICE_TESTER}
     * context.
     * </P>
     * @param stream Stream to have its tuples captured.
     * @param handler {@code StreamHandler} to capture tuples.
     * @return {@code handler}
     * 
     * @deprecated Since 1.11. In most distributed environments stream contents cannot be obtained
     * due to network isolation. An alternative is to use
     * {@link Tester#stringTupleTester(com.ibm.streamsx.topology.TStream, com.ibm.streamsx.topology.function.Predicate) stringTupleTester}
     * to perform a per-tuple check.
     */
    <T extends StreamHandler<Tuple>> T splHandler(SPLStream stream, T handler);

    /**
     * Return a condition that evaluates if {@code stream} has submitted
     * exactly {@code expectedCount} number of tuples.
     * 
     * <P>
     * <b>Note:</b> Since 1.11 {@code getResult()} from the returned {@code Condition} is
     * deprecated and tests should not rely on it returning the tuple count seen on the stream.
     * <strike>
     * <BR>
     *  The {@link Condition#getResult() result} of the returned {@code Condition} is the number of
     * tuples seen on {@code stream} so far.
     * </strike>
     * </P>
     *
     * 
     * @param stream
     *            Stream to be tested.
     * @param expectedCount
     *            Number of tuples expected on {@code stream}.
     * @return Exact tuple count condition.
     */
    Condition<Long> tupleCount(TStream<?> stream, long expectedCount);
    
    /**
     * Return a condition that evaluates if {@code stream} has submitted
     * at least {@code expectedCount} number of tuples.
     * <P>
     * <b>Note:</b> Since 1.11 {@code getResult()} from the returned {@code Condition} is
     * deprecated and tests should not rely on it returning the tuple count seen on the stream.
     * <strike>
     * <BR>
     * The {@link Condition#getResult() result} of the returned {@code Condition} is the number of
     * tuples seen on {@code stream} so far.
     * </strike>
     * </P>
     * 
     * @param stream
     *            Stream to be tested.
     * @param expectedCount
     *            Number of tuples expected on {@code stream}.
     * @return At least tuple count condition.
     */
    Condition<Long> atLeastTupleCount(TStream<?> stream, long expectedCount);

    /**
     * Return a condition that evaluates if {@code stream} has submitted
     * at tuples matching {@code values} in the same order.
     * <P>
     * <b>Note:</b> Since 1.11 {@code getResult()} from the returned {@code Condition} is
     * deprecated and tests should not rely on it returning the tuple count seen on the stream.
     * <strike>
     * <BR>
     * The {@link Condition#getResult() result} of the returned {@code Condition} is the
     * tuples seen on {@code stream} so far.
     * </strike>
     * </P>
     * 
     * @param stream
     *            Stream to be tested.
     * @param values
     *            Expected tuples on {@code stream}.
     * @return Tuple contents condition.
     */
    Condition<List<String>> stringContents(TStream<String> stream, String... values);
    
    /**
     * Return a condition that evaluates if {@code stream} has submitted
     * at tuples matching {@code values} in the same order.
     * <P>
     * <b>Note:</b> Since 1.11 {@code getResult()} from the returned {@code Condition} is
     * deprecated and tests should not rely on it returning the tuple count seen on the stream.
     * <strike>
     * <BR>
     * The {@link Condition#getResult() result} of the returned {@code Condition} is the
     * tuples seen on {@code stream} so far.
     * </strike>
     * </P>
     * 
     * @param stream
     *            Stream to be tested.
     * @param values
     *            Expected tuples on {@code stream}.
     * @return Tuple contents condition.
     */
    Condition<List<Tuple>> tupleContents(SPLStream stream, Tuple... values);

    /**
     * Return a condition that evaluates if {@code stream} has submitted
     * at tuples matching {@code values} in any order.
     * <P>
     * <b>Note:</b> Since 1.11 {@code getResult()} from the returned {@code Condition} is
     * deprecated and tests should not rely on it returning the tuple count seen on the stream.
     * <strike>
     * <BR>
     * The {@link Condition#getResult() result} of the returned {@code Condition} is the
     * tuples seen on {@code stream} so far.
     * </strike>
     * </P>
     * 
     * @param stream
     *            Stream to be tested.
     * @param values
     *            Expected tuples on {@code stream}.
     * @return Unordered tuple contents condition..
     */
    Condition<List<String>> stringContentsUnordered(TStream<String> stream, String... values);
    
    /**
     * Return a condition that evaluates if every tuple on {@code stream}
     * evaluates to {@code true} with {@code tester}.
     * 
     * @param stream Stream to be tested.
     * @param tester Predicate that will be executed against each tuple.
     * @return Condition whose result is the first tuple to fail the condition
     * (when the result is available).
     */
    Condition<String> stringTupleTester(TStream<String> stream, Predicate<String> tester);
    
    /**
     * Create a condition that randomly resets consistent regions.
     * 
     * The condition becomes valid when each consistent region in
     * the application under test has been reset {@code minimumResets} times
     * by the tester.
     * <P>
     * A region is reset by initiating a request though the Job Control Plane.
     * The reset is <B>not</B> driven by any injected failure, such as a PE restart.
     * </P>
     * @param minimumResets - Minimum number of resets for each region, defaults to 10.
     * @return Condition with no result object.
     * 
     * @throws IllegalArgumentException {@code minimumResets} less than zero.
     * 
     * @since 1.9 Only supported for Streaming Analytics.
     */
    Condition<Void> resetConsistentRegions(Integer minimumResets);
    
    /**
     * Submit the topology for this tester and wait for it to complete.
     * A topology can only complete if it is executing as embedded or
     * standalone, thus only these stream context types are supported:
     * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#EMBEDDED_TESTER}
     *  and
     * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#STANDALONE_TESTER}.
     * <P>
     * A topology completes when the IBM Streams runtime determines
     * that there is no more processing to be performed, which is typically once
     * all sources have indicated there is no more data to be processed,
     * and all of the source tuples have been fully processed by the topology.
     * <BR>
     * Note that many topologies will never complete, for example those
     * including polling or event sources. In this case a test case
     * should use {@link #complete(StreamsContext, Condition, long, TimeUnit)}.
     * </P>
     * 
     * @param context Context to be used for submission.
     * 
     * @throws Exception Failure submitting or executing the topology.
     * @throws IllegalStateException {@link com.ibm.streamsx.topology.context.StreamsContext#getType()} is not supported.
     */
    void complete(StreamsContext<?> context) throws Exception ;
    
    /**
     * Submit the topology for this tester and wait for it to complete,
     * or reach an end condition. If the topology does not complete
     * or reach its end condition before {@code timeout} then it is
     * terminated.
     * <BR>
     * This is suitable for testing topologies that never complete
     * or any topology running in a {@link com.ibm.streamsx.topology.context.StreamsContext.Type#DISTRIBUTED_TESTER distributed} context.
     * <P>
     * End condition is usually a {@link Condition} returned from
     * {@link #atLeastTupleCount(TStream, long)} or {@link #tupleCount(TStream, long)}
     * so that this method returns once the stream has submitted a sufficient number of tuples.
     * <BR>
     * Note that the condition will be only checked periodically up to {@code timeout},
     * so that if the condition is only valid for a brief period of time, then its
     * valid state may not be seen, and thus this method will wait for the timeout period.
     * </P>
     * 
     * @param context Context to be used for submission.
     * @param endCondition Condition that will cause this method to return if it is true.
     * @param timeout Maximum time to wait for the topology to complete or reach its end condition.
     * @param unit Unit for {@code timeout}.
     * @return The value of {@code endCondition.valid()}.
     * 
     * @throws Exception Failure submitting or executing the topology.
     * @throws IllegalStateException {@link com.ibm.streamsx.topology.context.StreamsContext#getType()} is not supported.
     * 
     * @see #complete(StreamsContext)
     */
    boolean complete(StreamsContext<?> context, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception;
    boolean complete(StreamsContext<?> context, Map<String, Object> config, Condition<?> endCondition, long timeout, TimeUnit unit) throws Exception;
    
    Condition<List<String>> completeAndTestStringOutput(StreamsContext<?> context, TStream<?> output, long timeout, TimeUnit unit, String...contents) throws Exception;
    Condition<List<String>> completeAndTestStringOutput(StreamsContext<?> context, Map<String, Object> config, TStream<?> output, long timeout, TimeUnit unit, String...contents) throws Exception;

}
