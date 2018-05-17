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
import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;

/**
 * A {@code TStream} is a declaration of a continuous sequence of tuples. A
 * connected topology of streams and functional transformations is built using
 * {@link Topology}. <BR>
 * Generic methods on this interface provide the ability to
 * {@link #filter(Predicate) filter}, {@link #transform(Function)
 * transform} or {@link #forEach(Consumer) sink} this declared stream using a
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
 * <BR>
 * When multiple streams are produced by a method (e.g. {@link #split(int, ToIntFunction)}
 * placement directives are common to all of the produced streams. 
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
	    HASH_PARTITIONED,
	    
	    /**
	     * Tuples are broadcast to all channels.
	     * For example with a width of four each tuple on the stream results
	     * in four tuples, one per channel.
	     * 
	     * @since 1.9
	     */
	    BROADCAST
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
     * <P>
     * This function is equivalent to {@link #map(Function)}.
     * </P>
     * @param transformer
     *            Transformation logic to be executed against each tuple.
     * @return Stream that will contain tuples of type {@code U} transformed from this
     *         stream's tuples.
     */
    <U> TStream<U> transform(Function<T, U> transformer);

    /**
     * Declare a new stream that maps each tuple from this stream into one
     * (or zero) tuple of a different type {@code U}. For each tuple {@code t}
     * on this stream, the returned stream will contain a tuple that is the
     * result of {@code mapper.apply(t)} when the return is not {@code null}.
     * If {@code mapper.apply(t)} returns {@code null} then no tuple
     * is submitted to the returned stream for {@code t}.
     * 
     * <P>
     * Examples of mapping a stream containing numeric values as
     * {@code String} objects into a stream of {@code Double} values.
     * 
     * <pre>
     * <code>
     * // Java 8 - Using lambda expression
     * TStream&lt;String> strings = ...
     * TStream&lt;Double> doubles = strings.map(v -> Double.valueOf(v));
     * 
     * // Java 8 - Using method reference
     * TStream&lt;String> strings = ...
     * TStream&lt;Double> doubles = strings.map(Double::valueOf);
     * 
     * // Java 7 - Using anonymous class
     * TStream&lt;String> strings = ...
     * TStream&lt;Double> doubles = strings.map(new Function<String, Double>() {
     *             &#64;Override
     *             public Double apply(String v) {
     *                 return Double.valueOf(v);
     *             }});
     * </code>
     * </pre>
     * 
     * </P>
     * <P>
     * This function is equivalent to {@link #transform(Function)}.
     * The typical term in most apis is {@code map}.
     * </P>
     * @param mapper
     *            Mapping logic to be executed against each tuple.
     * @return Stream that will contain tuples of type {@code U} mapped from this
     *         stream's tuples.
     *
     * @since 1.7 
     */
    <U> TStream<U> map(Function<T, U> mapper);

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
     * </P>
     * 
     * @param modifier
     *            Modifier logic to be executed against each tuple.
     * @return Stream that will contain tuples of type {@code T} modified from this
     *         stream's tuples.
     */
    TStream<T> modify(UnaryOperator<T> modifier);

    /**
     * Declare a new stream that maps tuples from this stream into one or
     * more (or zero) tuples of a different type {@code U}. For each tuple
     * {@code t} on this stream, the returned stream will contain all non-null tuples in
     * the {@code Iterator<U>} that is the result of {@code mapper.apply(t)}.
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
     * @param mapper
     *            Mapper logic to be executed against each tuple.     
     * @return Stream that will contain tuples of type {@code U} mapped from this
     *         stream's tuples.
     *         
     * @since 1.7
     */
    <U> TStream<U> flatMap(Function<T, Iterable<U>> mapper);
    
    /**
     * Declare a new stream that maps tuples from this stream into one or
     * more (or zero) tuples of a different type {@code U}.
     * <P>
     * This function is equivalent to {@link #flatMap(Function)}.
     * </P>
     * @param transformer Mapper logic to be executed against each tuple.  
     * @return Stream that will contain tuples of type {@code U} mapped from this
     *         stream's tuples.
     */
    <U> TStream<U> multiTransform(Function<T, Iterable<U>> transformer);
      
    /**
     * Sink (terminate) this stream. For each tuple {@code t} on this stream
     * {@link Consumer#accept(Object) action.accept(t)} will be called. This is
     * typically used to send information to external systems, such as databases
     * or dashboards.
     * <P>
     * Example of terminating a stream of {@code String} tuples by printing them
     * to {@code System.out}.
     * 
     * <pre>
     * <code>
     * TStream&lt;String> values = ...
     * values.forEach(new Consumer<String>() {
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
     * @param action
     *            Action to be executed against each tuple on this stream.
     * @return the sink element
     * 
     * @since 1.7
     */
    TSink forEach(Consumer<T> action);
        
    /**
     * Terminate this stream.
     * <P>
     * This function is equivalent to {@link #forEach(Consumer)}.
     * </P>
     * @param sinker Action to be executed against each tuple on this stream.
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
     * <LI>
     * {@code com.ibm.streamsx.topology.topic::FilteredSubscribe} operator for SPL
     * Streams applications subscribing to a subset of the published tuples.</LI>

     * </UL>
     * <BR>
     * A subscriber matches to a publisher if:
     * <UL>
     * <LI>
     * The topic name is an exact match, and:</LI>
     * <LI>
     * For JSON streams ({@code TStream<JSONObject>}) the subscription is to
     * a JSON stream.
     * </LI>
     * <LI>
     * For Java streams ({@code TStream<T>}) the declared Java type ({@code T}
     * ) of the stream is an exact match.</LI>
     * <LI>
     * For {@link com.ibm.streamsx.topology.spl.SPLStream SPL streams} the {@link com.ibm.streamsx.topology.spl.SPLStream#getSchema() SPL
     * schema} is an exact match.</LI>
     * </UL>
     * <BR>
     * This method is identical to {@link #publish(String, boolean) publish(topic, false)}.
     * <P>
     * A topic name:
     * <UL>
     * <LI>must not be zero length</LI>
     * <LI>must not contain the nul character ({@code \u0000})</LI>
     * <LI>must not contain wild card characters number sign ({@code ‘#’ \u0023})
     * or the plus sign ({@code ‘+’ \u002B})</LI>
     * </UL>
     * The forward slash ({@code ‘/’ \u002F}) is used to separate each level within a topic
     * tree and provide a hierarchical structure to the topic names.
     * The use of the topic level separator is significant when either of the
     * two wildcard characters is encountered in topic filters specified
     * by subscribing applications. Topic level separators can appear anywhere
     * in a topic filter or topic name. Adjacent topic level separators indicate
     * a zero length topic level.
     * </P>
     * <p>
     * The type of the stream must be known to ensure that
     * {@link Topology#subscribe(String, Class) subscribers}
     * match on the Java type. Where possible the type of a
     * stream is determined automatically, but due to Java type
     * erasure this is not always possible. A stream can be
     * assigned its type using {@link #asType(Class)}.
     * For example, with a stream containing tuples of type
     * {@code Location} it can be correctly published as follows:
     * <pre>
     * <code>
     * TStream&lt;Location> locations = ...
     * locations.asType(Location.class).publish("location/bus");
     * </code>
     * </pre>
     * </p>
     * 
     * @param topic Topic name to publish tuples to.
     *
     * @exception IllegalStateException Type of the stream is not known.
     * 
     * @see Topology#subscribe(String, Class)
     * @see com.ibm.streamsx.topology.spl.SPLStreams#subscribe(TopologyElement, String, com.ibm.streams.operator.StreamSchema)
     */
    void publish(String topic);
    
    /**
     * Publish tuples from this stream for consumption by other IBM Streams applications.
     * 
     * Differs from {@link #publish(String)} in that it
     * supports {@code topic} as a submission time parameter, for example
     * using the topic defined by the submission parameter {@code eventTopic}:
     * 
     * <pre>
     * <code>
     * TStream<String> events = ...
     * Supplier<String> topicParam = topology.createSubmissionParameter("eventTopic", String.class);
     * topology.publish(topicParam);
     * </code>
     * </pre>
     * 
     * @param topic Topic name to publish tuples to.
     * 
     * @see #publish(String)
     * 
     * @since 1.8
     */
    void publish(Supplier<String> topic);
    
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
     * These tuple types allow publish-subscribe across IBM Streams applications
     * implemented in different languages:
     * <UL>
     * <LI>{@code TStream<JSONObject>} - JSON tuples,
     * SPL schema of {@link com.ibm.streamsx.topology.spl.SPLSchemas#JSON Json}.</LI>
     * <LI>{@code TStream<String>} - String tuples,
     * SPL schema of {@link com.ibm.streamsx.topology.spl.SPLSchemas#STRING String}.</LI>
     * <LI>{@code TStream<com.ibm.streams.operator.types.XML>},
     * SPL schema of {@link com.ibm.streamsx.topology.spl.SPLSchemas#XML Xml}. </LI>
     * <LI>{@code TStream<com.ibm.streams.operator.types.Blob>},
     * SPL schema of {@link com.ibm.streamsx.topology.spl.SPLSchemas#BLOB Blob}. </LI>
     * </UL>
     * <P>
     * <BR>
     * A subscriber matches to a publisher if:
     * <UL>
     * <LI>
     * The topic name is an exact match, and:</LI>
     * <LI>
     * For JSON streams ({@code TStream<JSONObject>}) the subscription is to
     * a JSON stream.
     * </LI>
     * <LI>
     * For Java streams ({@code TStream<T>}) the declared Java type ({@code T}
     * ) of the stream is an exact match.</LI>
     * <LI>
     * For {@link com.ibm.streamsx.topology.spl.SPLStream SPL streams} the {@link com.ibm.streamsx.topology.spl.SPLStream#getSchema() SPL
     * schema} is an exact match.</LI>
     * </UL>
     * </P>
     * <P>
     * {@code allowFilter} specifies if SPL filters can be pushed from a subscribing
     * application to the publishing application. Executing filters on the publishing
     * side reduces network communication between the publisher and the subscriber.
     * <BR>
     * When {@code allowFilter} is {@code false} SPL filters cannot be pushed to
     * the publishing application.
     * <BR>
     * When {@code allowFilter} is {@code true} SPL filters are executed in the
     * publishing applications.
     * <BR>
     * Regardless of the setting of {@code allowFilter} an invocation of
     * {@link Topology#subscribe(String, Class)} or
     * {@code com.ibm.streamsx.topology.topic::Subscribe}
     * subscribes to all published tuples.
     * <BR>
     * {@code allowFilter} can only be set to true for:
     * <UL>
     * <LI>This stream is an instance of {@link com.ibm.streamsx.topology.spl.SPLStream}.</LI>
     * <LI>This stream is an instance of {@code TStream<String>}.</LI>
     * </UL>
     * </P>
     * <P>
     * A topic name:
     * <UL>
     * <LI>must not be zero length</LI>
     * <LI>must not contain the nul character ({@code \u0000})</LI>
     * <LI>must not contain wild card characters number sign ({@code ‘#’ \u0023})
     * or the plus sign ({@code ‘+’ \u002B})</LI>
     * </UL>
     * The forward slash ({@code ‘/’ \u002F}) is used to separate each level within a topic
     * tree and provide a hierarchical structure to the topic names.
     * The use of the topic level separator is significant when either of the
     * two wildcard characters is encountered in topic filters specified
     * by subscribing applications. Topic level separators can appear anywhere
     * in a topic filter or topic name. Adjacent topic level separators indicate
     * a zero length topic level.
     * </P>
     * <p>
     * The type of the stream must be known to ensure that
     * {@link Topology#subscribe(String, Class) subscribers}
     * match on the Java type. Where possible the type of a
     * stream is determined automatically, but due to Java type
     * erasure this is not always possible. A stream can be
     * assigned its type using {@link #asType(Class)}.
     * For example, with a stream containing tuples of type
     * {@code Location} it can be correctly published as follows:
     * <pre>
     * <code>
     * TStream&lt;Location> locations = ...
     * locations.asType(Location.class).publish("location/bus");
     * </code>
     * </pre>
     * </p>
     * 
     * @param topic Topic name to publish tuples to.
     * @param allowFilter Allow SPL filters specified by SPL application to be executed
     * in the publishing application.
     *
     * @exception IllegalStateException Type of the stream is not known.
     * 
     * @see Topology#subscribe(String, Class)
     * @see #asType(Class)
     * @see com.ibm.streamsx.topology.spl.SPLStreams#subscribe(TopologyElement, String, com.ibm.streams.operator.StreamSchema)
     */
    void publish(String topic, boolean allowFilter);
    
    /**
     * Publish tuples from this stream for consumption by other IBM Streams applications.
     * 
     * Differs from {@link #publish(String, boolean)} in that it
     * supports {@code topic} as a submission time parameter, for example
     * using the topic defined by the submission parameter {@code eventTopic}:
     * 
     * <pre>
     * <code>
     * TStream<String> events = ...
     * Supplier<String> topicParam = topology.createSubmissionParameter("eventTopic", String.class);
     * topology.publish(topicParam, false);
     * </code>
     * </pre>
     * 
     * @param topic Topic name to publish tuples to.
     * @param allowFilter Allow SPL filters specified by SPL application to be executed.
     * 
     * @see #publish(String, boolean)
     * 
     * @since 1.8
     */
    void publish(Supplier<String> topic, boolean allowFilter);

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
     * <BR><BR>
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
     * <br><br>
     * Source operations may be parallelized as well, refer to {@link TStream#setParallel(Supplier)} for more information.
     * <br><br>
     * Given the following code:
     * <pre>
     * <code>
     * TStream&lt;String> myStream = topology.source(...);
     * TStream&lt;String> parallelStart = myStream.parallel(of(3), TStream.Routing.ROUND_ROBIN);
     * TStream&lt;String> inParallel = parallelStart.map(...);
     * TStream&lt;String> joinedParallelStreams = inParallel.endParallel();
     * joinedParallelStreams.print();
     * </code>
     * </pre>
     * 
     * The following graph is created:
     * <br>
     * <img src="doc-files/Diagram2.jpg" width = 500/>
     * <br>
     * <br>
     * Calling {@code parallel(3)} creates three parallel channels. Each of the 3 channels contains separate 
     * instantiations of the operations (in this case, just <b>map</b>) declared in the region. Such stream operations are 
     * run in parallel as follows:
     * <br>
     * 
     * <style>
            table, th, td {
                border: 1px solid black;
                border-collapse: collapse;
            }
            th, td {
                padding: 5px;
            }
            th {
                text-align: left;
            }
     * </style>
     * <table>
     * <tr><th>Execution Context</th><th>Parallel Behavior</th></tr>
     * <tr><td>Standalone</td><td>Each parallel channel is separately executed by one or more threads. 
     * The number of threads executing a channel is exactly 1 thread per input to the channel.</td></tr>
     * <tr><td>Distributed</td><td>A parallel channel is never run in the same process as another parallel channel.
     * Furthermore, a single parallel channel may executed across multiple processes, as determined by the Streams runtime.</td></tr>
     * <tr><td>Streaming Analytics service</td><td>Same as Distributed.</td></tr>
     * <tr><td>Embedded</td><td>All parallel information is ignored, and the application is executed without any added parallelism.</td></tr>
     * </table>
     * 
     * <br>
     * 
     * A parallel region can have multiple inputs and multiple outputs. An input to a parallel 
     * region is a stream on which {@link TStream#parallel(int)} has been called, and an output
     * is a stream on which {@link TStream#endParallel()} has been called.
     * A parallel region with multiple inputs is created if a stream in one parallel region connects with a stream in another
     * parallel region. 
     * <br><br>
     * Two streams "connect" if:
     * <ul>
     * <li>One stream invokes {@link TStream#union(TStream)} using the other as a parameter.</li>
     * <li>Both streams are used as inputs to an SPL operator created through 
     * {@link com.ibm.streamsx.topology.SPL#invokeOperator(String, com.ibm.streamsx.topology.spl.SPLInput, StreamSchema, java.util.Map) invokeOperator}
     * which has multiple input ports.</li> 
     * </ul>
     * <br>
     * For example, the following code connects two separate parallel regions into a single parallel region with
     * multiple inputs:
     * 
     * <pre>
     * <code>
     * TStream&lt;String> firstStream = topology.source(...);
     * TStream&lt;String> secondStream = topology.source(...);
     * TStream&lt;String> firstParallelStart = firstStream.parallel(of(3), TStream.Routing.ROUND_ROBIN);
     * TStream&lt;String> secondParallelStart = secondStream.parallel(of(3), TStream.Routing.ROUND_ROBIN);
     * TStream&lt;String> fistMapOutput = firstParallelStart.map(...);
     * TStream&lt;String> unionedStreams = firstMapOutput.union(secondParallelStart);
     * TStream&lt;String> secondMapOutput = unionedStreams.map(...);
     * TStream&lt;String> nonParallelStream = secondMapOutput.endParallel();
     * nonParallelStream.print();
     * </code>
     * </pre>
     * 
     * This code results in the following graph:
     * <br>
     * <img src="doc-files/Diagram3.jpg" width=500/>
     * <br><br>
     * When creating a parallel region with multiple inputs, the different inputs must all have the same value
     * for the degree of parallelism. For example, it can not be the case that one parallel region input 
     * specifies a width of 4, while another input to the same region specifies a width of 5. Additionally,
     * if a submission time parameter is used to specify the width of a parallel region, then different inputs 
     * to that region must all use the same submission time parameter object.
     * <br><br>
     * A parallel region may contain a sink; it is not required that a parallel region have an output stream.
     * The following defines a sink in a parallel region:
     * <pre>
     * <code>
     * TStream&lt;String> myStream = topology.source(...);
     * TStream&lt;String> myParallelStream = myStream.parallel(6);
     * myParallelStream.print();
     * </code>
     * </pre>
     * In the above code, the parallel region is implicitly ended by the sink, without calling 
     * {@link TStream#endParallel()}
     * 
     * This results in the following graph:
     * <br>
     * <img src="doc-files/Diagram1.jpg" width=500>
     * <br><br>
     * 
     * A parallel region with multiple output streams can be created by invoking {@link TStream#endParallel()}
     * on multiple streams within the same parallel region. For example, the following code defines a parallel
     * region with multiple output streams:
     * <pre>
     * <code>
     * TStream&lt;String> myStream = topology.source(...);
     * TStream&lt;String> parallelStart = myStream.parallel(of(3), TStream.Routing.ROUND_ROBIN);
     * TStream&lt;String> firstInParallel = parallelStart.map(...);
     * TStream&lt;String> secondInParallel = parallelStart.map(...);
     * TStream&lt;String> firstParallelOutput = firstInParallel.endParallel();
     * TStream&lt;String> secondParallelOutput = secondInParallel.endParallel();
     * </code>
     * </pre>
     * The above code would yield the following graph:
     * <br>
     * <img src="doc-files/Diagram4.jpg" width=500/>
     * <br><br>
     * 
     * When a stream outside of a parallel region connects to a stream inside of a parallel region,
     * the outside stream and all of its prior operations implicitly become part of the parallel region.
     * 
     * For example, the following code connects a stream outside a parallel 
     * region to a stream inside of a parallel region.
     * 
     * <pre>
     * <code>
     * TStream&lt;String> firstStream = topology.source(...);
     * TStream&lt;String> secondStream = topology.source(...);
     * TStream&lt;String> parallelStream = firstStream.parallel(of(3), TStream.Routing.ROUND_ROBIN);
     * TStream&lt;String> secondParallelStart = 
     * TStream&lt;String> firstInParallel = firstParallelStart.map(...);
     * TStream&lt;String> secondInParallel = secondParallelStart.map(...);
     * TStream&lt;String> unionStream = firstInParallel.union(secondInParallel);
     * TStream&lt;String> nonParallelStream = unionStream.endParallel();
     * nonParallelStream.print();
     * </code>
     * </pre>
     * 
     * Once connected, the stream outside of the parallel region (and all of its prior operations)
     * becomes part of the parallel region:
     * 
     * <img src="doc-files/Diagram5.jpg" width=500/>
     * 
     * <br><br>
     * 
     * The Streams runtime supports the nesting of parallel regions inside of another parallel region.
     * A parallel region will become nested inside of another parallel region in one of two
     * cases:
     * 
     * <ul>
     * <li>
     * If {@link TStream#parallel(int)} is invoked on a stream which is already inside of a parallel region.
     * </li>
     * <li>
     * A stream inside of a parallel region becomes connected to a stream that has a parallel region in its
     * previous operations.
     * </li>
     * </ul>
     * 
     * For example, calling {@link TStream#parallel(int)} twice on the same stream creates a nested 
     * parallel region as follows: 
     * 
     * <pre>
     * <code>
     * TStream&lt;String> stream = topology.source(...);
     * stream.parallel(3).map(...).parallel(3).map(...).endParallel().endParallel();
     * </code>
     * </pre>
     * 
     * Results in a graph of the following structure:
     * <br>
     * <img src="doc-files/Diagram7.jpg" width=500/>
     * <br><br>
     * 
     * Whereas the first map operation is instantiated exactly 3 times due to {@code parallel(3)},
     * the second map operation is instantiated a total of 9 times since each of the 3 enclosing 
     * parallel channels holds 3 nested parallel channels. The {@link TStream#Routing} configurations
     * of the enclosing and nested parallel regions do not need to match.
     * <br><br>
     * As previously mentioned, nesting also occurs when a stream inside of a parallel region becomes 
     * connected to a stream that has a parallel region in its previous operations. The following
     * code creates such a situation:
     * 
     * <pre>
     * <code>
     * TStream&lt;String> streamToJoin = topology.source(...);
     * streamToJoin.setParallel();
     * streamToJoin = streamToJoin.map(...).endParallel();
     * 
     * TStream&lt;String> parallelStream = topology.source(...);
     * parallelStream = parallelStream.parallel(4);
     * 
     * parallelStream = parallelStream.union(streamToJoin);
     * parallelStream.map(...).endParallel().print();
     * </code>
     * </pre>
     * 
     * This results in the following graph structure:
     * <br>
     * <img src="doc-files/Diagram6.jpg" width=500>
     * <br><br>
     * 
     * Limitations of parallel() are as follows: <br>
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
     * Sets the current stream as the start of a parallel region.
     * 
     * @param width The degree of parallelism.
     * @see #parallel(int)
     * @see #parallel(Supplier, Routing)
     * @see #parallel(Supplier, Function)
     * @since v1.9
     */
    TStream<T> setParallel(Supplier<Integer> width);
    
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
     * Return a stream matching this stream whose subsequent
     * processing will execute in an autonomous region.
     * By default IBM Streams processing
     * is executed in an autonomous region where any checkpointing of
     * operator state is autonomous (independent) of other operators.
     * <BR>
     * This method may be used to end a consistent region
     * by starting an autonomous region. This may be called
     * even if this stream is in an autonomous region.
     * <BR>
     * Autonomous is not applicable when a topology is submitted
     * to {@link com.ibm.streamsx.topology.context.StreamsContext.Type#EMBEDDED embedded}
     * and {@link com.ibm.streamsx.topology.context.StreamsContext.Type#STANDALONE standalone}
     * contexts and will be ignored.
     * 
     * @since v1.5
     */
    TStream<T> autonomous();
    
    /**
     * Set the operator that is the source of this stream to be the start of a
     * consistent region to support at least once and exactly once
     * processing.
     * IBM Streams calculates the boundaries of the consistent region
     * based on the reachability graph of this stream. A region
     * can be bounded though use of {@link #autonomous()}.
     * 
     * <P>
     * Consistent regions are only supported in distributed contexts.
     * </P>
     * <P>
     * This must be called on a stream directly produced by an
     * SPL operator that supports consistent regions.
     * Source streams produced by methods on {@link Topology}
     * do not support consistent regions.
     * </P>

     * @since 1.5 API added
     * @since 1.8 Working implementation.
     * 
     * @return this
     * 
     * @see com.ibm.streamsx.topology.consistent.ConsistentRegionConfig
     */
    TStream<T> setConsistent(ConsistentRegionConfig config);
    
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
