/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.function.Function;

/**
 * Declares a window of tuples for a {@link TStream}. Logically a {@code Window}
 * represents an continuously updated ordered list of tuples according to the
 * criteria that created it. For example {@link TStream#last(int) s.last(10)}
 * declares a window that at any time contains the last ten tuples seen on
 * stream {@code s}, while
 * {@link TStream#last(long, java.util.concurrent.TimeUnit) s.last(5,
 * TimeUnit.SECONDS)} is a window that always contains all tuples present on stream
 * {@code s} in the last five seconds.
 * <P>
 * Typically windows are partitioned by a key which means the window's configuration
 * is independently maintained for each key seen on the stream.
 * For example with a window created using {@link TStream#last(int) last(3)}
 * then each key has its own window partition containing the last
 * three tuples with the same key.
 * <BR>
 * A partitioned window is created by calling {@link #key(Function)}
 * or {@link #key()}.
 * <BR>
 * When a window is not partitioned it acts as though it has
 * a single partition with a constant key with the value {@code Integer.valueOf(0)}.
 * 
 * @param <T>
 *            Tuple type, any instance of {@code T} at runtime must be
 *            serializable.
 * @param <K> Key type.
 * 
 * @see TStream#last()
 * @see TStream#last(int)
 * @see TStream#last(long, java.util.concurrent.TimeUnit)
 * @see TStream#window(TWindow)
 */
public interface TWindow<T,K> extends TopologyElement {

    /**
     * Declares a stream that containing tuples that represent an aggregation of
     * this window. Each time the contents of the window is updated by a new
     * tuple being added to it, or a tuple being evicted from the window
     * {@code aggregator.call(tuples)} is called, where {@code tuples} is an
     * {@code List} that containing all the tuples in the current window.
     * The {@code List} is stable during the method call, and returns the
     * tuples in order of insertion into the window, from oldest to newest. <BR>
     * Thus the returned stream will contain a sequence of tuples that where the
     * most recent tuple represents the most up to date aggregation of this
     * window or window partition.
     * 
     * @param aggregator
     *            Logic to aggregation the complete window contents.
     * @return A stream that contains the latest aggregations of this window.
     */
    <A> TStream<A> aggregate(Function<List<T>, A> aggregator);

    /**
     * Declares a stream that containing tuples that represent an aggregation of
     * this window. Approximately every {@code period} (with unit {@code unit})
     * {@code aggregator.call(tuples)} is called, where {@code tuples} is an
     * {@code List} that containing all the tuples in the current window.
     * The {@code List} is stable during the method call, and returns the
     * tuples in order of insertion into the window, from oldest to newest. <BR>
     * Thus the returned stream will contain a new tuple every {@code period}
     * seconds (according to {@code unit}) aggregation of this window or window
     * partition.
     * 
     * @param aggregator
     *            Logic to aggregation the complete window contents.
     * @param period
     *            Approximately how often to perform the aggregation.
     * @param unit
     *            Time unit for {@code period}.
     * @return A stream that contains the latest aggregations of this window.
     */
    <A> TStream<A> aggregate(Function<List<T>, A> aggregator, long period,
            TimeUnit unit);

    /**
     * Class of the tuples in this window. WIll be the same as {@link #getTupleType()}
     * is a {@code Class} object.
     * @return Class of the tuple in this window, {@code null}
     * if {@link #getTupleType()} is not a {@code Class} object.
     */
    Class<T> getTupleClass();
    
    /**
     * Type of the tuples in this window.
     * @return Type of the tuples in this window.
     */
    Type getTupleType();

    /**
     * Get this window's stream.
     * 
     * @return This window's stream.
     */
    TStream<T> getStream();
    
    /**
     * Return a keyed (partitioned) window that has the same
     * configuration as this window with the each tuple's
     * key defined by a function. 
     * A keyed window is a window where each tuple has an inherent
     * key, defined by {@code keyFunction}.
     * <P> 
     * All tuples that have the same key will
     * be processed as an independent window. For example,
     * with a window created using {@link TStream#last(int) last(3)}
     * then each key has its own window containing the last
     * three tuples with the same key.
     * </P>
     * @param keyFunction Function that gets the key from a tuple.
     * The key function must be stateless.
     * @return Keyed window with the same configuration as this window.
     * 
     * @param <U> Type of the key.
     */
    <U> TWindow<T,U> key(Function<? super T, ? extends U> keyFunction);
    
    /**
     * Return a keyed (partitioned) window that has the same
     * configuration as this window with each tuple being the key.
     * The key of each tuple is the tuple itself.
     * @return Keyed window with the same configuration as this window.
     * 
     * @see #key(Function)
     */
    TWindow<T,T> key();
    
    /**
     * Is the window keyed.
     * @return {@code true} if the window is keyed, {@code false} if it is not keyed.
     * 
     * @see #key(Function)
     * @see #key()
     */
    boolean isKeyed();
}
