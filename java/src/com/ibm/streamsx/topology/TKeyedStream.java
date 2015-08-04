/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.UnaryOperator;

/**
 * A {@code TStream} is a declaration of a continuous sequence of tuples
 * where each tuple has an inherent key. The key is obtained from a
 * tuple on the stream using {@link #getKeyFunction()}.
 * 
 * <BR>
 * A keyed stream is obtained from a {@code TStream} using 
 * {@link TStream#key(Function)} or {@link TStream#key()}.
 * <BR>
 * Most methods on {@code TKeyedStream} that return a stream of the
 * same type ({@code T}) return an instance of {@code TKeyedStream}
 * using the same key function as this stream. For example, through a pipeline
 * of {@link TKeyedStream#filter(Predicate) filter} and
 * {@link TKeyedStream#modify(UnaryOperator) modify} functions
 * the stream remains keyed.
 *
 * @param <T> Tuple type, any instance of {@code T} at runtime must be serializable.
 * @param <K> Key type.
 * 
 * @see TStream#key(Function)
 * @see TStream#key()
 */
public interface TKeyedStream<T,K> extends TStream<T> {
    
    /**
     * Get the function that obtains the key from a tuple.
     * @return function that obtains the key from a tuple.
     */
    public Function<T,K> getKeyFunction();
    
    /**
     * {@inheritDoc}
     */
    @Override
    TWindow<T,K> last(long time, TimeUnit unit);

    /**
     * {@inheritDoc}
     */
    @Override
    TWindow<T,K> last(int count);

    /**
     * {@inheritDoc}
     */
    @Override
    TWindow<T,K> last();

    /**
     * {@inheritDoc}
     */
    TWindow<T,K> window(TWindow<?,?> configWindow);
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> endLowLatency();
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> filter(Predicate<T> filter);
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> lowLatency();
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> modify(UnaryOperator<T> modifier);
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> sample(double fraction);
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> throttle(long delay, TimeUnit unit);
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> parallel(int width);
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> parallel(int width,
            com.ibm.streamsx.topology.TStream.Routing routing);
    
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> unparallel();
  
    /**
     * {@inheritDoc}
     */
    @Override
    TKeyedStream<T,K> isolate();
    
}
