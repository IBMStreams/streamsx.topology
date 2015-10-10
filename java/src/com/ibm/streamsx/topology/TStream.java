/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.spl.SPLStream;

/**
 * A {@code TStream} is a declaration of a continuous sequence of tuples. A
 * connected topology of streams and functional transformations is built using
 * {@link Topology}. <BR>
 * Generic methods on this interface provide the ability to
 * {@link #filter(Predicate) filter}, {@link #transform(Function)
 * transform} or {@link #sink(Consumer) sink} this declared stream using a
 * function. <BR>
 * Utility methods in the {@code com.ibm.streams.topology.streams} package
 * provide specific source streams, or transformations on streams with specific
 * types.
 * <P>
 * {@code TStream} implements {@link Placeable} to allow placement
 * directives against the processing that produced this stream.
 * For example, calling a {@code Placeable} method on the stream
 * returned from {@link #filter(Predicate)} will apply to the
 * container that is executing the {@code Predicate} passed into {@code filter()}.
 * </P>
 * 
 * @param <T>
 *            Tuple type, any instance of {@code T} at runtime must be
 *            serializable.
 */
public interface TStream<T> extends TopologyElement, Placeable<TStream<T>>  {

    /**
     * Enumeration for routing tuples to parallel channels.
     * @see TStream#parallel(Supplier, Routing)
     */
	public enum Routing {
	   /**
	    * Tuples will be routed to parallel channels such that an even 
	    * distribution is maintained.
	    */
	    ROUND_ROBIN, 
	    
	    /**
	     * Tuples will be consistently routed to the same channel based upon 
	     * their key. The key is obtained through:
	     * <UL>
	     * <LI>A function called against each tuple when using {@link TStream#parallel(Supplier, Function)}</LI>
	     * <LI>The {@link com.ibm.streamsx.topology.logic.Logic#identity() identity function} when using {@link TStream#parallel(Supplier, Routing)}</LI>
	     * </UL>
	     * The key for a {@code t} is the return from {@code keyer.apply(t)}.
	     * <BR>
	     * Any two tuples {@code t1} and {@code t2} will appear on
	     * the same channel if for their keys  {@code k1} and {@code k2}
	     * {@code k1.equals(k2)} is true.
	     * <BR>
	     * If {@code k1} and {@code k2} are not equal then there is
	     * no guarantee about which channels {@code t1} and {@code t2}
	     * will appear on, they may end up on the same or different channels. 
	     * <BR>
	     * The assumption is made that
	     * the key classes correctly implement the contract for {@code equals} and
	     * {@code hashCode()}.
	     */
	    KEY_PARTITIONED,
	    
	    /**
	     * Tuples will be consistently routed to the same channel based upon 
             * their {@code hashCode()}.
	     */
	    HASH_PARTITIONED	    
	};
	
    /**
     * Declare a new stream that filters tuples from this stream. Each tuple
     * {@code t} on this stream will appear in the returned     stream if
     * {@link Predicate#test(Object) filter.test(t)} returns {@code true}. If
     * {@code filter.test(t)} returns {@code false} then then {@code t} will not
     * appear in the returned stream.
     * <P>
     * Examples of filtering out all empty strings from stream {@code s} of type
     * {@code String}
     * 
     * <pre>
     * <code>
     * // Java 8 - Using lambda expression
     * TStream&lt;String> s = ...
     * TStream&lt;String> filtered = s.filter(t -> !t.isEmpty());
     *             
     * // Java 7 - Using anonymous class
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
     * @see #split(int, ToIntFunction)
     */
    TStream<T> filter(Predicate<T> filter);
    
    /**
     * Distribute a stream's tuples among {@code n} streams
     * as specified by a {@code splitter}.
     * 
     * <P>
     * For each tuple on the stream, {@code splitter.applyAsInt(tuple)} is called.
     * The return value {@code r} determines the destination stream:
     * <pre>
     * if r < 0 the tuple is discarded
     * else it is sent to the stream at position (r % n) in the returned array.
     * </pre>
     * </P>
     *
     * <P>
     * Each split {@code TStream} is exposed by the API. The user
     * has full control over the each stream's processing pipeline. 
     * Each stream's pipeline must be declared explicitly.
     * Each stream can have different processing pipelines.  
     * </P>
     * <P>
     * An N-way {@code split()} is logically equivalent to a
     * collection of N {@code filter()} invocations, each with a
     * {@code predicate} to select the tuples for its stream.
     * {@code split()} is more efficient. Each tuple is analyzed only once
     * by a single {@code splitter} instance to identify the destination stream.
     * For example, these are logically equivalent:
     * <pre>
     * List&lt;TStream&lt;String>> streams = stream.split(2, mySplitter());
     * 
     * TStream&lt;String> stream0 = stream.filter(myPredicate("ch0")); 
     * TStream&lt;String> stream1 = stream.filter(myPredicate("ch1")); 
     * </pre>
     * </P>
     * <P>
     * {@link #parallel(Supplier, Routing)} also distributes a stream's
     * tuples among N-channels but it presents a different usage model.  
     * The user specifies a single logical processing pipeline and
     * the logical pipeline is transparently replicated for each of the channels. 
     * The API does not provide access to the individual channels in
     * the logical stream.
     * {@link #endParallel()} declares the end of the parallel pipeline and combines
     * all of the channels into a single resulting stream.
     * </P>
     * <P>
     * Example of splitting a stream of tuples by their severity
     * attribute:
     * <pre>
     * <code>
     * interface MyType { String severity; ... };
     * TStream&lt;MyType> s = ...
     * List&lt;&lt;TStream&lt;MyType>> splits = s.split(3, new ToIntFunction&lt;MyType>() {
     *             &#64;Override
     *             public int applyAsInt(MyType tuple) {
     *                 if(tuple.severity.equals("high"))
     *                     return 0;
     *                 else if(tuple.severity.equals("low"))
     *                     return 1;
     *                 else
     *                     return 2;
     *             }} );
     * splits.get(0). ... // high severity processing pipeline
     * splits.get(1). ... // low severity processing pipeline
     * splits.get(2). ... // "other" severity processing pipeline
     * </code>
     * </pre>
     * </P>
     * @param n the number of output streams
     * @param splitter the splitter function
     * @return List of {@code n} streams
     * 
     * @throws IllegalArgumentException if {@code n <= 0}
     * @see #parallel(Supplier, Routing)
     */
    List<TStream<T>> split(int n, ToIntFunction<T> splitter);

    /**
     * Declare a new stream that transforms each tuple from this stream into one
     * (or zero) tuple of a different type {@code U}. For each tuple {@code t}
     * on this stream, the returned stream will contain a tuple that is the
     * result of {@code transformer.apply(t)} when the return is not {@code null}.
     * If {@code transformer.apply(t)} returns {@code null} then no tuple
     * is submitted to the returned stream for {@code t}.
     * 
     * <P>
     * Examples of transforming a stream containing numeric values as
     * {@code String} objects into a stream of {@code Double} values.
     * 
     * <pre>
     * <code>
     * // Java 8 - Using lambda expression
     * TStream&lt;String> strings = ...
     * TStream&lt;Double> doubles = strings.transform(v -> Double.valueOf(v));
     * 
     * // Java 8 - Using method reference
     * TStream&lt;String> strings = ...
     * TStream&lt;Double> doubles = strings.transform(Double::valueOf);
     * 
     * // Java 7 - Using anonymous class
     * TStream&lt;String> strings = ...
     * TStream&lt;Double> doubles = strings.transform(new Function<String, Double>() {
     *             &#64;Override
     *             public Double apply(String v) {
     *                 return Double.valueOf(v);
     *             }});
     * </code>
     * </pre>
     * 
     * </P>
     * @param transformer
     *            Transformation logic to be executed against each tuple.
     * @return Stream that will contain tuples of type {@code U} transformed from this
     *         stream's tuples.
     */
    <U> TStream<U> transform(Function<T, U> transformer);

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
     *             public String apply(String tuple) {
     *                 return tuple.concat("extra");
     *             }});
     * </code>
     * </pre>
     * 
     * </P>
     * <P>
     * This method is equivalent to
     * {@code transform(Function<T,T> modifier}).
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
     * {@code t} on this stream, the returned stream will contain all non-null tuples in
     * the {@code Iterator<U>} that is the result of {@code transformer.apply(t)}.
     * Tuples will be added to the returned stream in the order the iterator
     * returns them.
     * 
     * <BR>
     * If the return is null or an empty iterator then no tuples are added to
     * the returned stream for input tuple {@code t}.
     * <P>
     * Examples of transforming a stream containing lines of text into a stream
     * of words split out from each line. The order of the words in the stream
     * will match the order of the words in the lines.
     * 
     * <pre>
     * <code>
     * // Java 8 - Using lambda expression
     * TStream&lt;String> lines = ...
     * TStream&lt;String> words = lines.multiTransform(
     *                     line -> Arrays.asList(line.split(" ")));
     *             
     * // Java 7 - Using anonymous class
     * TStream&lt;String> lines = ...
     * TStream&lt;String> words = lines.multiTransform(new Function<String, Iterable<String>>() {
     *             &#64;Override
     *             public Iterable<String> apply(String line) {
     *                 return Arrays.asList(line.split(" "));
     *             }});
     * </code>
     * </pre>
     * 
     * </P>
     * 
     * @param transformer
     *            Transformation logic to be executed against each tuple.     
     * @return Stream that will contain tuples of type {@code U} transformed from this
     *         stream's tuples.
     */
    <U> TStream<U> multiTransform(Function<T, Iterable<U>> transformer);
    
    
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
     *             public void accept(String tuple) {
     *                 System.out.println(tuple);
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
     * @return the sink element
     */
    TSink sink(Consumer<T> sinker);

    /**
     * Create a stream that is a union of this stream and {@code other} stream
     * of the same type {@code T}. Any tuple on this stream or {@code other}
     * will appear on the returned stream. <BR>
     * No ordering of tuples across this stream and {@code other} is defined,
     * thus the return stream is unordered.
     * <BR>
     * If {@code other} is this stream or keyed version of this stream
     * then {@code this} is returned as a stream cannot be unioned with itself.
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
     * <BR>
     * A stream or a keyed version of a stream cannot be unioned with itself,
     * so any stream that is represented multiple times in {@code others}
     * or this stream will be reduced to a single copy of itself.
     * <BR>
     * In the case that no stream is to be unioned with this stream
     * then {@code this} is returned (for example, {@code others}
     * is empty or only contains the same logical stream as {@code this}.
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
    TSink print();

    /**
     * Class of the tuples on this stream, if known.
     * Will be the same as {@link #getTupleType()}
     * if it is a {@code Class} object.
     * @return Class of the tuple on this stream, {@code null}
     * if {@link #getTupleType()} is not a {@code Class} object.
     */
    Class<T> getTupleClass();
    
    /**
     * Type of the tuples on this stream.
     * Can be null if no type knowledge can be determined.
     * 
     * @return Type of the tuples on this stream,
     *     {@code null} if no type knowledge could be determined
     */
    Type getTupleType();
    
    /**
     * Join this stream with window of type {@code U}. For each tuple on this
     * stream, it is joined with the contents of {@code window}. Each tuple is
     * passed into {@code joiner} and the return value is submitted to the
     * returned stream. If call returns null then no tuple is submitted.
     * 
     * @param joiner Join function.
     * @return A stream that is the results of joining this stream with
     *         {@code window}.
     */
    <J, U> TStream<J> join(TWindow<U,?> window,
            BiFunction<T, List<U>, J> joiner);
    
    /**
     * Join this stream with a partitioned window of type {@code U} with key type {@code K}.
     * For each tuple on this stream, it is joined with the contents of {@code window}
     * for the key {@code keyer.apply(tuple)}. Each tuple is
     * passed into {@code joiner} and the return value is submitted to the
     * returned stream. If call returns null then no tuple is submitted.
     * 
     * @param keyer Key function for this stream to match the window's key.
     * @param window Keyed window to join this stream with.
     * @param joiner Join function.
     * @return A stream that is the results of joining this stream with
     *         {@code window}.
     */
    <J, U, K> TStream<J> join(
            Function<T,K> keyer,
            TWindow<U,K> window,
            BiFunction<T, List<U>, J> joiner);
    
    /**
     * Join this stream with the last tuple seen on a stream of type {@code U}
     * with partitioning.
     * For each tuple on this
     * stream, it is joined with the last tuple seen on {@code lastStream}
     * with a matching key (of type {@code K}).
     * <BR>
     * Each tuple {@code t} on this stream will match the last tuple
     * {@code u} on {@code lastStream} if
     * {@code keyer.apply(t).equals(lastStreamKeyer.apply(u))}
     * is true.
     * <BR>
     * The assumption is made that
     * the key classes correctly implement the contract for {@code equals} and
     * {@code hashCode()}.
     * <P>Each tuple is
     * passed into {@code joiner} and the return value is submitted to the
     * returned stream. If call returns null then no tuple is submitted.
     * </P>
     * @param keyer Key function for this stream
     * @param lastStream Stream to join with.
     * @param lastStreamKeyer Key function for {@code lastStream}
     * @param joiner Join function.
     * @return A stream that is the results of joining this stream with
     *         {@code lastStream}.
     */
    <J,U,K> TStream<J> joinLast(
            Function<? super T, ? extends K> keyer,
            TStream<U> lastStream,
            Function<? super U, ? extends K> lastStreamKeyer,
            BiFunction<T, U, J> joiner);
 
    /**
     * Join this stream with the last tuple seen on a stream of type {@code U}.
     * For each tuple on this
     * stream, it is joined with the last tuple seen on {@code lastStream}. Each tuple is
     * passed into {@code joiner} and the return value is submitted to the
     * returned stream. If call returns null then no tuple is submitted.
     * <BR>
     * This is a simplified version of {@link #join(TWindow, BiFunction)}
     * where instead the window contents are passed as a single tuple of type {@code U}
     * rather than a list containing one tuple. If no tuple has been seen on {@code lastStream}
     * then {@code null} will be passed as the second argument to {@code joiner}.
     * 
     * @param lastStream Stream to join with.
     * @param joiner Join function.
     * @return A stream that is the results of joining this stream with
     *         {@code lastStream}.
     */
    <J,U> TStream<J> joinLast(
            TStream<U> lastStream,
            BiFunction<T, U, J> joiner);

    /**
     * Declare a {@link TWindow} that continually represents the last {@code time} seconds
     * of tuples (in the given time {@code unit}) on this stream.
     * If no tuples have been seen on the stream in the last {@code time} seconds
     * then the window will be empty.
     * <BR>
     * The window has a single partition that always contains the
     * last {@code time} seconds of tuples seen on this stream
     * <BR>
     * A key based partitioned window can be created from the returned window
     * using {@link TWindow#key(Function)} or {@link TWindow#key()}.
     * When the window is partitioned each partition independently maintains the last {@code time}
     * seconds of tuples for each key seen on this stream.
     * 
     * @param time Time size of the window
     * @param unit Unit for {@code time}
     * @return Window on this stream representing the last {@code time} seconds.
     */
    TWindow<T,Object> last(long time, TimeUnit unit);

    /**
     * Declare a {@link TWindow} that continually represents the last {@code count} tuples
     * seen on this stream. If the stream has not yet seen {@code count}
     * tuples then it will contain all of the tuples seen on the stream,
     * which will be less than {@code count}. If no tuples have been
     * seen on the stream then the window will be empty.
     * <BR>
     * The window has a single partition that always contains the
     * last {@code count} tuples seen on this stream.
     * <BR>
     * The window has a single partition that always contains the last tuple seen
     * on this stream.
     * <BR>
     * A key based partitioned window can be created from the returned window
     * using {@link TWindow#key(Function)} or {@link TWindow#key()}.
     * When the window is partitioned each partition independently maintains the
     * last {@code count} tuples for each key seen on this stream.
     * 
     * @param count Tuple size of the window
     * @return Window on this stream representing the last {@code count} tuples.
     */
    TWindow<T,Object> last(int count);

    /**
     * Declare a {@link TWindow} that continually represents the last tuple on this stream.
     * If no tuples have been seen on the stream then the window will be empty.
     * <BR>
     * The window has a single partition that always contains the last tuple seen
     * on this stream.
     * <BR>
     * A key based partitioned window can be created from the returned window
     * using {@link TWindow#key(Function)} or {@link TWindow#key()}.
     * When the window is partitioned each partition independently maintains the
     * last tuple for each key seen on this stream.
     * 
     * @return Window on this stream representing the last tuple.
     */
    TWindow<T,Object> last();

    /**
     * Declare a {@link TWindow} on this stream that has the same configuration
     * as another window.
     * <BR>
     * The window has a single partition.
     * <BR>
     * A key based partitioned window can be created from the returned window
     * using {@link TWindow#key(Function)} or {@link TWindow#key()}.
     * 
     * @param configWindow
     *            Window to copy the configuration from.
     * @return Window on this stream with the same configuration as {@code configWindow}.
     */
    TWindow<T,Object> window(TWindow<?,?> configWindow);

    /**
     * Publish tuples from this stream for consumption by other IBM Streams applications.
     * 
     * Applications consume published streams using:
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
     * The topic is an exact match, and:</LI>
     * <LI>
     * For JSON streams ({@code TStream<JSONObject>}) the subscription is to
     * a JSON stream.
     * </LI>
     * <LI>
     * For Java streams ({@code TStream<T>}) the declared Java type ({@code T}
     * ) of the stream is an exact match.</LI>
     * <LI>
     * For {@link SPLStream SPL streams} the {@link SPLStream#getSchema() SPL
     * schema} is an exact match.</LI>
     * </UL>
     * 
     * @see Topology#subscribe(String, Class)
     * @see com.ibm.streamsx.topology.spl.SPLStreams#subscribe(TopologyElement, String, com.ibm.streams.operator.StreamSchema)
     */
    void publish(String topic);

    /**
     * Parallelizes the stream into a a fixed
     * number of parallel channels using round-robin distribution.
     * <BR>
     * Tuples are routed to the parallel channels in a
     * {@link Routing#ROUND_ROBIN round-robin fashion}.
     * <BR>
     * Subsequent transformations on the returned stream will be executed
     * {@code width} channels until {@link #endParallel()} is called or
     * the stream terminates.
     * <br>
     * See {@link #parallel(Supplier, Routing)} for more information.
     * @param width
     *            The degree of parallelism in the parallel region.
     * @return A reference to a stream for which subsequent transformations will be
     *         executed in parallel using {@code width} channels.
     */
    TStream<T> parallel(int width);
    
    /**
     * Parallelizes the stream into {@code width} parallel channels.
     * Same as {@link #parallel(int)} except the {@code width} is
     * specified with a {@code Supplier<Integer>} such as one created
     * by {@link Topology#createSubmissionParameter(String, Class)}.
     * 
     * @param width
     *            The degree of parallelism in the parallel region.
     * @return A reference to a stream for which subsequent transformations will be
     *         executed in parallel using {@code width} channels.
     */
    TStream<T> parallel(Supplier<Integer> width);
    
    /**
     * Parallelizes the stream into {@code width} parallel channels. Tuples are routed 
     * to the parallel channels based on the {@link Routing} parameter.
     * <BR>
     * If {@link Routing#ROUND_ROBIN}
     * is specified the tuples are routed to parallel channels such that an 
     * even distribution is maintained.
     * <BR>
     * If {@link Routing#HASH_PARTITIONED} is specified then the 
     * {@code hashCode()} of the tuple is used to route the tuple to a corresponding 
     * channel, so that all tuples with the same hash code are sent to the same channel.
     * <BR>
     * If {@link Routing#KEY_PARTITIONED} is specified each tuple is
     * is taken to be its own key and is
     * routed so that all tuples with the same key are sent to the same channel.
     * This is equivalent to calling {@link #parallel(Supplier, Function)} with
     * an identity function.
     * <br>
     * If parallel is invoked when submitting to an embedded context, the flow
     * will execute as though parallel had not been called.
     * <br>
     * Given the following code:
     * 
     * <pre>
     * <code>
     * TStream&lt;String> myStream = ...;
     * TStream&lt;String> parallel_start = myStream.parallel(of(3), TStream.Routing.ROUND_ROBIN);
     * TStream&lt;String> in_parallel = parallel_start.filter(...).transform(...);
     * TStream&lt;String> joined_parallel_streams = in_parallel.endParallel();
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
     * case) are separate instances and operate independently from one another.
     * <br>
     * {@code parallel(...)} will only parallelize the stream operations performed <b>after</b>
     * the call to {@code parallel(...)} and before the call to {@code endParallel()}.
     * 
     * In the above example, the {@code parallel(3)} was invoked on {@code myStream}, so
     * its subsequent functions, {@code filter(...)} and {@code transform(...)}, were parallelized. <br>
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
     * It is not necessary to invoke {@link #endParallel()} on parallel sinks.</b>
     * <br><br>
     * Limitations of parallel() are as follows: <br>
     * Nested parallelism is not currently supported. A call to {@code parallel(...)}
     * should never be made immediately after another call to {@code parallel(...)} without
     * having an {@code endParallel()} in between. <br>
     * <br>
     * {@code parallel()} should not be invoked immediately after another call to
     * {@code parallel()}. The following is invalid:
     * 
     * <pre>
     * <code>
     * myStream.parallel(2).parallel(2);
     * </pre>
     * 
     * </code>
     * 
     * Every call to {@code endParallel()} must have a call to {@code parallel(...)} preceding it. The
     * following is invalid:
     * 
     * <pre>
     * <code>
     * TStream<String> myStream = topology.strings("a","b","c");
     * myStream.endParallel();
     * </pre>
     * 
     * </code>
     * 
     * A parallelized stream cannot be joined with another window, and a
     * parallelized window cannot be joined with a stream. The following is
     * invalid:
     * 
     * <pre>
     * <code>
     * TWindow<String> numWindow = topology.strings("1","2","3").last(3);
     * TStream<String> stringStream = topology.strings("a","b","c");
     * TStream<String> paraStringStream = myStream.parallel(5);
     * TStream<String> filtered = myStream.filter(...);
     * filtered.join(numWindow, ...);
     * </pre>
     * 
     * </code>
     * 
     * @param width The degree of parallelism. see {@link #parallel(int width)}
     * for more details.
     * @param routing Defines how tuples will be routed channels.
     * @return A reference to a TStream<> at the beginning of the parallel
     * region.
     * 
     * @throws IllegalArgumentException if {@code width} is null
     * 
     * @see Topology#createSubmissionParameter(String, Class)
     * @see #split(int, ToIntFunction)
     */
    TStream<T> parallel(Supplier<Integer> width, Routing routing);
    
    /**
     * Parallelizes the stream into a number of parallel channels
     * using key based distribution.
     * <BR>
     * For each tuple {@code t} {@code keyer.apply(t)} is called
     * and then the tuples are routed
     * so that all tuples with the
     * {@link Routing#KEY_PARTITIONED same key are sent to the same channel}.
     * 
     * @param width The degree of parallelism.
     * @param keyer Function to obtain the key from each tuple. 
     * @return A reference to a stream with {@code width} channels
     * at the beginning of the parallel region.
     * 
     * @see Routing#KEY_PARTITIONED
     * @see #parallel(Supplier, Routing)
     */
    TStream<T> parallel(Supplier<Integer> width, Function<T,?> keyer);
    
    /**
     * Ends a parallel region by merging the channels into a single stream.
     * 
     * @return A stream for which subsequent transformations are no longer parallelized.
     * @see #parallel(int)
     * @see #parallel(Supplier, Routing)
     * @see #parallel(Supplier, Function)
     */
    TStream<T> endParallel();

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
     * Return a stream whose immediate subsequent processing will execute
     * in a separate operating system process from this stream's processing.
     * <BR>
     * For the following Topology:
     * <pre><code>
     * -->transform1-->.isolate()-->transform2-->transform3-->.isolate()-->sink
     * </code></pre>
     * It is guaranteed that:
     * <UL>
     * <LI>{@code transform1} and {@code transform2} will execute in separate processes.</LI>
     * <LI>{@code transform3} and {@code sink} will execute in separate processes. </LI>
     * </UL>
     * If multiple transformations ({@code t1, t2, t3}) are applied to a stream returned from {@code isolate()}
     * then it is guaranteed that each of them will execute in a separate operating
     * system process to this stream, but no guarantees are made about where {@code t1, t2, t3}
     * are placed in relationship to each other.
     * <br>
     * Only applies for distributed contexts.
     * @return A stream that runs in a separate process from this stream.
     */
    TStream<T> isolate();
    
    /**
     * Return a stream that is guaranteed to run in the same process as the
     * calling TStream. All streams that are created from the returned stream 
     * are also guaranteed to run in the same process until {@link TStream#endLowLatency()}
     * is called.
     * <br><br>
     * For example, for the following topology:
     * <pre><code>
     * ---source---.lowLatency()---filter---transform---.endLowLatency()---
     * </code></pre>
     * It is guaranteed that the filter and transform operations will run in
     * the same process.
     * <br><br>
     * Only applies for distributed contexts.
     * @return A stream that is guaranteed to run in the same process as the 
     * calling stream.
     */
    TStream<T> lowLatency();

    /**
     * Return a TStream that is no longer guaranteed to run in the same process
     * as the calling stream. For example, in the following topology:
     * <pre><code>
     * ---source---.lowLatency()---filter---transform---.endLowLatency()---filter2
     * </code></pre>
     * It is guaranteed that the filter and transform operations will run in
     * the same process, but it is not guaranteed that the transform and
     * filter2 operations will run in the same process.
     * <br><br>
     * Only applies for distributed contexts.
     * @return A stream that is not guaranteed to run in the same process as the 
     * calling stream.
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
     * Return a strongly typed reference to this stream.
     * If this stream is already strongly typed as containing tuples
     * of type {@code tupleClass} then {@code this} is returned.
     * @param tupleTypeClass Class type for the tuples.
     * @return A stream with the same contents as this stream but strongly typed as
     * containing tuples of type {@code tupleClass}.
     */
    TStream<T> asType(Class<T> tupleTypeClass);
    
    /**
     * Return a keyed stream that contains the same tuples as this stream. 
     * A keyed stream is a stream where each tuple has an inherent
     * key, defined by {@code keyFunction}.
     * <P> 
     * A keyed stream provides control over the behavior of
     * downstream processing of tuples with {@link #parallel(int) parallel streams} and
     * {@link TWindow windows}.
     * <BR>
     * With parallel streams all tuples that have the same key
     * will be processed by the same channel.
     * <BR>
     * With windows all tuples that have the same key will
     * be processed as an independent window. For example,
     * with a window created using {@link #last(int) last(3)}
     * then each key has its own window containing the last
     * three tuples with the same key.
     * </P>
     * @param keyFunction Function that gets the key from a tuple.
     * The key function must be stateless.
     * @return Keyed stream containing tuples from this stream.
     * 
     * @see TKeyedStream
     * @see TWindow
     * 
     * @param <K> Type of the key.
     */
    // <K> TKeyedStream<T,K> key(Function<T,K> keyFunction);
    
    /**
     * Return a keyed stream that contains the same tuples as this stream. 
     * The key of each tuple is the tuple itself.
     * <BR>
     * For example, a {@code TStream<String> strings} may be keyed using
     * {@code strings.key()} and thus when made {@link #parallel(int) parallel}
     * all {@code String} objects with the same value will be sent to the
     * same channel.
     * @return this.
     * 
     * @see #key(Function)
     */
    // TKeyedStream<T,T> key();
    
    /**
     * Internal method.
     * <BR>
     * <B><I>Not intended to be called by applications, may be removed at any time.</I></B>
     */
    BOutput output();

    /**
     * Internal method.
     * <BR>
     * <B><I>Not intended to be called by applications, may be removed at any time.</I></B>
     * <BR>
     * Connect this stream to a downstream operator. If input is null then a new
     * input port will be created, otherwise it will be used to connect to this
     * stream. Returns input or the new port if input was null.
     */
    BInputPort connectTo(BOperatorInvocation receivingBop, boolean functional, BInputPort input);
}
