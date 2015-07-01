/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.function7.Consumer;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.function7.Predicate;
import com.ibm.streamsx.topology.function7.UnaryOperator;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tuple.Keyable;

/**
 * A {@code TStream} is a declaration of a continuous sequence of tuples. A
 * connected topology of streams and functional transformations is built using
 * {@link Topology}. <BR>
 * Generic methods on this interface provide the ability to
 * {@link #filter(Predicate) filter}, {@link #transform(Function, Class)
 * transform} or {@link #sink(Consumer) sink} this declared stream using a
 * function. <BR>
 * Utility methods in the {@code com.ibm.streams.topology.streams} package
 * provide specific source streams, or transformations on streams with specific
 * types.
 * 
 * @param <T>
 *            Tuple type, any instance of {@code T} at runtime must be
 *            serializable.
 */
public interface TStream<T> extends TopologyElement {

    /**
     * Enumeration for routing tuples to parallel channels.
     * @see TStream#parallel(int, Routing)
     */
	public enum Routing {
	   /**
	    * Tuples will be routed to parallel channels such that an even 
	    * distribution is maintained.
	    */
	    ROUND_ROBIN, 
	    /**
	     * Tuples will be consistently routed to the same channel based upon 
	     * their value.
	     * <br>
	     * If the tuple implements the {@link Keyable} interface, then the tuple
	     * is routed to a parallel channel according to the 
	     * {@code hashCode()} of the object returned by {@link Keyable#getKey()}. 
	     * <BR>
	     * If the tuple does not 
	     * implement the {@link Keyable} interface, then the tuple is routed to a parallel 
	     * channel according to the {@code hashCode()} of the tuple object itself.
	     */
	    PARTITIONED};
	
    /**
     * Declare a new stream that filters tuples from this stream. Each tuple
     * {@code t} on this stream will appear in the returned     stream if
     * {@link Predicate#test(Object) filter.test(t)} returns {@code true}. If
     * {@code filter.test(t)} returns {@code false} then then {@code t} will not
     * appear in the returned stream.
     * <P>
     * Example of filtering out all empty strings from stream {@code s} of type
     * {@code String}
     * 
     * <pre>
     * <code>
     * TStream&lt;String> s = ...
     * TStream&lt;String> filtered = s.filter(new Predicate&lt;String>() {
     *             &#64;Override
     *             public boolean test(String t) {
     *                 return !t.isEmpty();
     *             }} );
     * </code>
     * </pre>
     * 
     * </P>
     * 
     * @param filter
     *            Filtering logic to be executed against each tuple.
     * @return Filtered stream
     */
    TStream<T> filter(Predicate<T> filter);

    /**
     * Declare a new stream that transforms each tuple from this stream into one
     * (or zero) tuple of a different type {@code U}. For each tuple {@code t}
     * on this stream, the returned stream will contain a tuple that is the
     * result of {@code transformer.apply(t)} when the return is not {@code null}.
     * If {@code transformer.apply(t)} returns {@code null} then no tuple
     * is submitted to the returned stream for {@code t}.
     * 
     * <P>
     * Example of transforming a stream containing numeric values as
     * {@code String} objects into a stream of {@code Double} values.
     * 
     * <pre>
     * <code>
     * TStream&lt;String> strings = ...
     * TStream&lt;Double> doubles = strings.transform(new Function<String, Double>() {
     *             &#64;Override
     *             public Double apply(String v) {
     *                 return Double.valueOf(v);
     *             }}, Double.class );
     * </code>
     * </pre>
     * 
     * </P>
     * 
     * @param transformer
     *            Transformation logic to be executed against each tuple.
     * @param tupleTypeClass
     *            Type {@code U} of the returned stream.
     * @return Stream that will contain tuples of type {@code U} transformed from this
     *         stream's tuples.
     */
    <U> TStream<U> transform(Function<T, U> transformer, Class<U> tupleTypeClass);

    /**
     * Declare a new stream that modifies each tuple from this stream into one
     * (or zero) tuple of the same type {@code T}. For each tuple {@code t}
     * on this stream, the returned stream will contain a tuple that is the
     * result of {@code modifier.apply(t)} when the return is not {@code null}.
     * The function may return the same reference as its input {@code t} or
     * a different object of the same type.
     * If {@code modifier.apply(t)} returns {@code null} then no tuple
     * is submitted to the returned stream for {@code t}.
     * 
     * <P>
     * Example of modifying a stream  {@code String} values by adding the suffix '{@code extra}'.
     * 
     * <pre>
     * <code>
     * TStream&lt;String> strings = ...
     * TStream&lt;String> modifiedStrings = strings.modify(new UnaryOperator<String>() {
     *             &#64;Override
     *             public String apply(String v) {
     *                 return v.concat("extra");
     *             }});
     * </code>
     * </pre>
     * 
     * </P>
     * <P>
     * This method is equivalent to
     * {@code transform(Function<T,T> modifier, T.class}).
     * </P
     * 
     * @param modifier
     *            Modifier logic to be executed against each tuple.
     * @return Stream that will contain tuples of type {@code T} modified from this
     *         stream's tuples.
     */
    TStream<T> modify(UnaryOperator<T> modifier);

    /**
     * Declare a new stream that transforms tuples from this stream into one or
     * more (or zero) tuples of a different type {@code U}. For each tuple
     * {@code t} on this stream, the returned stream will contain all tuples in
     * the {@code Iterator<U>} that is the result of {@code transformer.apply(t)}.
     * Tuples will be added to the returned stream in the order the iterator
     * returns them.
     * 
     * <BR>
     * If the return is null or an empty iterator then no tuples are added to
     * the returned stream for input tuple {@code t}.
     * <P>
     * Example of transforming a stream containing lines of text into a stream
     * of words split out from each line. The order of the words in the stream
     * will match the order of the words in the lines.
     * 
     * <pre>
     * <code>
     * TStream&lt;String> lines = ...
     * TStream&lt;String> words = lines.multiTransform(new Function<String, Iterable<String>>() {
     *             &#64;Override
     *             public Iterable<String> apply(String t) {
     *                 return Arrays.asList(t.split(" "));
     *             }}, String.class);
     * </code>
     * </pre>
     * 
     * </P>
     * 
     * @param transformer
     *            Transformation logic to be executed against each tuple.
     * @param tupleTypeClass
     *            Type {@code U} of the returned stream.
     * @return Stream that will contain tuples of type {@code U} transformed from this
     *         stream's tuples.
     */
    <U> TStream<U> multiTransform(Function<T, Iterable<U>> transformer,
            Class<U> tupleTypeClass);

    /**
     * Sink (terminate) this stream. For each tuple {@code t} on this stream
     * {@link Consumer#accept(Object) sinker.accept(t)} will be called. This is
     * typically used to send information to external systems, such as databases
     * or dashboards.
     * <P>
     * Example of terminating a stream of {@code String} tuples by printing them
     * to {@code System.out}.
     * 
     * <pre>
     * <code>
     * TStream&lt;String> values = ...
     * values.sink(new Consumer<String>() {
     *             
     *             &#64;Override
     *             public void accept(String v) {
     *                 System.out.println(v);
     *                 
     *             }
     *         });
     * </code>
     * </pre>
     * 
     * </P>
     * 
     * @param sinker
     *            Logic to be executed against each tuple on this stream.
     */
    void sink(Consumer<T> sinker);

    /**
     * Create a stream that is a union of this stream and {@code other} stream
     * of the same type {@code T}. Any tuple on this stream or {@code other}
     * will appear on the returned stream. <BR>
     * No ordering of tuples across this stream and {@code other} is defined,
     * thus the return stream is unordered.
     * 
     * @param other
     *            Stream to union with this stream.
     * @return Stream that will contain tuples from this stream and
     *         {@code other}.
     */
    TStream<T> union(TStream<T> other);

    /**
     * Create a stream that is a union of this stream and {@code others} streams
     * of the same type {@code T}. Any tuple on this stream or any of
     * {@code others} will appear on the returned stream. <BR>
     * No ordering of tuples across this stream and {@code others} is defined,
     * thus the return stream is unordered. <BR>
     * If others does not contain any streams then {@code this} is returned.
     * 
     * @param others
     *            Streams to union with this stream.
     * @return Stream containing tuples from this stream and {@code others}.
     */
    TStream<T> union(Set<TStream<T>> others);

    /**
     * Print each tuple on {@code System.out}. For each tuple {@code t} on this
     * stream {@code System.out.println(t.toString())} will be called.
     */
    void print();

    Class<T> getTupleClass();

    /**
     * Join this stream with window of type {@code U}. For each tuple on this
     * stream, it is joined with the contents of {@code other}. Each tuple is
     * passed into {@code joiner} and the return value is submitted to the
     * returned stream. If call returns null then no tuple is submitted.
     * 
     * @param joiner
     * @return A stream that is the results of joining this stream with
     *         {@code window}.
     */
    <J, U> TStream<J> join(TWindow<U> window,
            BiFunction<T, List<U>, J> joiner, Class<J> tupleClass);

    /**
     * Declare a {@link TWindow} that represents the last {@code time} seconds
     * of tuples (in the given time {@code unit}) on this stream. <BR>
     * When {@code T} implements {@link Keyable} then the window is partitioned
     * using the value of {@link Keyable#getKey()}. In this case that means each
     * partition independently maintains the last {@code time} seconds of tuples
     * for that key.
     */
    TWindow<T> last(long time, TimeUnit unit);

    /**
     * Declare a {@link TWindow} that represents the last {@code count} tuples
     * on this stream. <BR>
     * When {@code T} implements {@link Keyable} then the window is partitioned
     * using the value of {@link Keyable#getKey()}. In this case that means each
     * partition independently maintains the last {@code count} tuples for that
     * key.
     */
    TWindow<T> last(int count);

    /**
     * Declare a {@link TWindow} that represents the last tuple on this stream. <BR>
     * When {@code T} implements {@link Keyable} then the window is partitioned
     * using the value of {@link Keyable#getKey()}. In this case that means each
     * partition independently maintains the last tuple for that key.
     */
    TWindow<T> last();

    /**
     * Declare a {@link TWindow} on this stream that has the same configuration
     * as another window..
     * 
     * When {@code T} implements {@link Keyable} then the window is partitioned
     * using the value of {@link Keyable#getKey()}. In this case that means each
     * partition independently maintains the list of tuples for that key.
     * 
     * @param configWindow
     *            Window to copy the configuration from.
     */
    TWindow<T> window(TWindow<?> configWindow);

    /**
     * Publish tuples from this stream to allow other applications to consume
     * them using:
     * <UL>
     * <LI>
     * {@link Topology#subscribe(String, Class)} for Java Streams applications.</LI>
     * <LI>
     * {@code com.ibm.streamsx.topology.topic::Subscribe} operator for SPL
     * Streams applications.</LI>
     * </UL>
     * <BR>
     * A subscriber matches to a publisher if:
     * <UL>
     * <LI>
     * The topic is an exact match.</LI>
     * <LI>
     * For Java streams ({@code Stream&lt;T>}), the declared Java type ({@code T}
     * ) of the stream is an exact match.</LI>
     * <LI>
     * For {@link SPLStream SPL streams}, the {@link SPLStream#getSchema() SPL
     * scehma} is an exact match.</LI>
     * </UL>
     * 
     */
    void publish(String topic);

    /**
     * Parallelizes the stream into {@code width} parallel channels. If the 
     * tuple implements {@link Keyable}, the parallel channels are partitioned.
     * Otherwise, the parallel channels are not partitioned, and tuples are routed
     * in a round-robin fashion.
     * <br><br>
     * See the documentation for {@link #parallel(int, Routing)} for more
     * information.
     * @param width
     *            The degree of parallelism in the parallel region.
     * @return A reference to a stream for which subsequent operations will be
     *         part of the parallel region.
     */
    TStream<T> parallel(int width);
    
    
    /**
     * Parallelizes the stream into {@code width} parallel channels. Tuples are routed 
     * to the parallel channels based on the {@link Routing} parameter. If {@code ROUND_ROBIN}
     * is specified, the tuples are routed to parallel channels such that an 
     * even distribution is maintained. If {@code PARTITIONED} is specified and 
     * the tuple implements the {@link Keyable} interface, then the tuple is 
     * routed to a parallel channel according to the {@code hashCode()} of the 
     * object returned by {@code getKey()}. If {@code PARTITIONED} is specified
     * and the tuple does not implement the {@link Keyable} interface, then the 
     * {@code hashCode()} of the tuple is used to route the tuple to a corresponding 
     * channel. 
     * <br><br>
     * Given the following code:
     * 
     * <pre>
     * <code>
     * TStream&lt;String> myStream = ...;
     * TStream&lt;String> parallel_start = myStream.parallel(3, TStream.Routing.ROUND_ROBIN);
     * TStream&lt;String> in_parallel = parallel_start.filter(...).transform(...);
     * TStream&lt;String> joined_parallel_streams = in_parallel.unparallel();
     * </code>
     * </pre>
     * 
     * a visual representation a visual representation for parallel() would be
     * as follows:
     * 
     * <pre>
     * <code>
     *                  |----filter----transform----|
     *                  |                           |
     * ---myStream------|----filter----transform----|--joined_parallel_streams
     *                  |                           |
     *                  |----filter----transform----|
     * </code>
     * </pre>
     * 
     * Each parallel channel can be thought of as being assigned its own thread.
     * As such, each parallelized stream function (filter and transform, in this
     * case) operate independently from one another. <br>
     * <br>
     * parallel() will only parallelize the stream operations performed <b>after</b>
     * the call to parallel() and before the call to unparallel().
     * 
     * In the above example, the parallel() was invoked on {@code myStream}, so
     * its subsequent functions, filter() and transform(), were parallelized. <br>
     * <br>
     * Parallel regions aren't required to have an output stream, and thus may be
     * used as sinks. The following would be an example of a parallel sink:
     * <pre>
     * <code>
     * TStream&lt;String> myStream = ...;
     * TStream&lt;String> myParallelStream = myStream.parallel(6);
     * myParallelStream.print();
     * </code>
     * </pre>
     * {@code myParallelStream} will be printed to output in parallel. In other
     * words, a parallel sink is created by calling {@link #parallel(int)} and 
     * creating a sink operation (such as {@link TStream#sink(Consumer)}). <b>
     * It is not necessary to invoke {@link #unparallel()} on parallel sinks.</b>
     * <br><br>
     * Limitations of parallel() are as follows: <br>
     * Nested parallelism is not currently supported. A call to parallel()
     * should never be made immediately after another call to parallel() without
     * having an unparallel() in between. <br>
     * <br>
     * Parallel() should not be invoked immediately after another call to
     * parallel(). The following is invalid:
     * 
     * <pre>
     * <code>
     * myStream.parallel(2).parallel(2);
     * </pre>
     * 
     * </code>
     * 
     * There must be at least one stream function between a parallel() and
     * unparallel() invocation. The following is invalid:
     * 
     * <pre>
     * <code>
     * myStream.parallel(2).unparallel();
     * </pre>
     * 
     * </code>
     * 
     * Every call to unparallel() must have a call to parallel preceding it. The
     * following is invalid:
     * 
     * <pre>
     * <code>
     * Stream<String> myStream = topology.strings("a","b","c");
     * myStream.unparallel();
     * </pre>
     * 
     * </code>
     * 
     * A parallelized Stream cannot be joined with another window, and a
     * parallelized Window cannot be joined with a Stream. The following is
     * invalid:
     * 
     * <pre>
     * <code>
     * Window<String> numWindow = topology.strings("1","2","3").last(3);
     * Stream<String> stringStream = topology.strings("a","b","c");
     * Stream<String> paraStringStream = myStream.parallel(5);
     * Stream<String> filtered = myStream.filter(...);
     * filtered.join(numWindow, ...);
     * </pre>
     * 
     * </code>
     * 
     * @param width The degree of parallelism. see {@link #parallel(int width)}
     * for more details.
     * @param routing A TStream enum: ROUND_ROBIN or PARTITIONED. If PARTITIONED
     * is specified, and the tuple doesn't implement Keyable, it will 
     * partition based on the tuple's Object hashCode().
     * @return A reference to a TStream<> at the beginning of the parallel
     * region.
     */
    TStream<T> parallel(int width, Routing routing);
    
    /**
     * unparallel() merges the parallel channels of a parallelized stream.
     * returns a Stream&lt;T> for which subsequent operations will not be
     * performed in parallel. Additionally, it merges any partitions which may
     * have been present in the parallel channels. <br>
     * <br>
     * For additional documentation, see {@link TStream#parallel(int)}
     * 
     * @return A Stream&lt;T> for which subsequent operations are no longer
     * parallelized.
     */
    TStream<T> unparallel();

    /**
     * Return a stream that is a random sample of this stream.
     * 
     * @param fraction
     *            Fraction of the data to return.
     * @return Stream containing a random sample of this stream.
     * @throws IllegalArgumentException
     *             {@code fraction} is less that or equal to zero or greater
     *             than 1.0.
     */
    TStream<T> sample(double fraction);
    
    /**
     * Add docs
     * @return isolate tstream
     */
    TStream<T> isolate();
    
    /**
     * Start low latency region
     * @return Low latency tstream
     */
    TStream<T> lowLatency();
    
    /**
     * end low latency region
     * @return Non-low latency tstream.
     */
    TStream<T> endLowLatency();

    /**
     * Throttle a stream by ensuring any tuple is submitted with least
     * {@code delay} from the previous tuple.
     * 
     * @param delay
     *            Maximum amount to delay a tuple.
     * @param unit
     *            Unit of {@code delay}.
     * @return Stream containing all tuples on this stream. but throttled.
     */
    TStream<T> throttle(long delay, TimeUnit unit);

    /**
     * Internal method.
     * <BR>
     * Not intended to be called by applications, may be removed at any time.
     */
    BOutput output();

    /**
     * Internal method.
     * Connect this stream to a downstream operator. If input is null then a new
     * input port will be created, otherwise it will be used to connect to this
     * stream. Returns input or the new port if input was null.
     * 
     * <BR>
     * Not intended to be called by applications, may be removed at any time.
     */
    BInputPort connectTo(BOperatorInvocation receivingBop, boolean functional, BInputPort input);
}
