package com.ibm.streamsx.topology;

import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.tuple.Keyable;


public interface TKeyedStream<T,K> extends TStream<T> {
    
    /**
     * Declare a {@link TWindow} that continually represents the last {@code time} seconds
     * of tuples (in the given time {@code unit}) on this stream.
     * If no tuples have been seen on the stream in the last {@code time} seconds
     * then the window will be empty.
     * <BR>
     * When {@code T} implements {@link Keyable} then the window is partitioned
     * using the value of {@link Keyable#getKey()}. In this case that means each
     * partition independently maintains the last {@code time} seconds of tuples
     * for that key.
     * 
     * @param time Time size of the window
     * @param unit Unit for {@code time}
     * @return Window on this stream for the last {@code time} seconds.
     */
    @Override
    TWindow<T,K> last(long time, TimeUnit unit);

    /**
     * Declare a {@link TWindow} that continually represents the last {@code count} tuples
     * seen on this stream. If the stream has not yet seen {@code count}
     * tuples then it will contain all of the tuples seen on the stream,
     * which will be less than {@code count}. If no tuples have been
     * seen on the stream then the window will be empty.
     * <BR>
     * When {@code T} implements {@link Keyable} then the window is partitioned
     * using the value of {@link Keyable#getKey()}. In this case that means each
     * partition independently maintains the last {@code count} tuples for that
     * key.
     * 
     * @param count Tuple size of the window
     * @return Window on this stream for the last {@code count} tuples.
     */
    @Override
    TWindow<T,K> last(int count);

    /**
     * Declare a {@link TWindow} that continually represents the last tuple on this stream.
     * If no tuples have been seen on the stream then the window will be empty.
     * <BR>
     * When {@code T} implements {@link Keyable} then the window is partitioned
     * using the value of {@link Keyable#getKey()}. In this case that means each
     * partition independently maintains the last tuple for that key.
     * 
     * @return Window on this stream for the last tuple.
     */
    @Override
    TWindow<T,K> last();

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
    TKeyedStream<T,K> unparallel();
}
