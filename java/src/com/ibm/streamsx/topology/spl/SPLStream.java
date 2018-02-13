/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import java.util.concurrent.TimeUnit;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;

/**
 * A {@code SPLStream} is a declaration of a continuous sequence of tuples with
 * a defined SPL schema. A {@code SPLStream} is a TStream&lt;Tuple> thus may be
 * handled using any functional logic where each tuple will be an instance of
 * {@code com.ibm.streams.operator.Tuple}.
 */
public interface SPLStream extends TStream<Tuple>, SPLInput {

    /**
     * SPL schema of this stream.
     * 
     * @return SPL schema of this stream.
     */
    StreamSchema getSchema();

    /**
     * Transform SPL tuples into JSON. Each tuple from this stream is converted
     * into a JSON representation.
     * <UL>
     * <LI>
     * If {@link #getSchema()} returns
     * {@link com.ibm.streamsx.topology.json.JSONSchemas#JSON}
     * then each tuple is taken as a serialized JSON and deserialized.
     * If the serialized JSON is an array,
     * then a JSON object is created, with
     * a single attribute {@code payload} containing the deserialized
     * value.
     * </LI>
     * <LI>
     * Otherwise the tuple is converted to JSON using the
     * encoding provided by the SPL Java Operator API
     * {@code com.ibm.streams.operator.encoding.JSONEncoding}.
     * </LI>
     * </UL>
     * 
     * @return A stream with each tuple as a {@code JSONObject}.
     */
    TStream<JSONObject> toJSON();

    /**
     * Convert SPL tuples into Java objects. This call is equivalent to
     * {@code transform(converter)}.
     * 
     * @param convertor
     *            Function to convert
     * @return Stream containing tuples of type {@code T} transformed from this
     *         stream's SPL tuples.
     * 
     * @see TStream#transform(Function)
     */
    <T> TStream<T> convert(Function<Tuple, T> convertor);

    /**
     * Create a stream that converts each input tuple on this stream to its SPL
     * character representation representation.
     * 
     * @return Stream containing SPL character representations of this stream's
     *         SPL tuples.
     */
    TStream<String> toTupleString();

    /**
     * Create a TStream&lt;Tuple> from this stream. This {@code SPLStream} must
     * have a schema of {@link SPLSchemas#STRING}.
     * 
     * @return This stream declared as a TStream&lt;Tuple>.
     * @throws IllegalStateException
     *             Stream does not have a value schema for TStream&lt;Tuple>.
     * 
     * @see SPLStreams#stringToSPLStream(TStream)
     */
    TStream<String> toStringStream();
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream endLowLatency();
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream filter(Predicate<Tuple> filter);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream lowLatency();
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream isolate();
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream modify(UnaryOperator<Tuple> modifier);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream sample(double fraction);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream throttle(long delay, TimeUnit unit);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream parallel(int width);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream parallel(Supplier<Integer> width,
            com.ibm.streamsx.topology.TStream.Routing routing);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream parallel(Supplier<Integer> width,
            Function<Tuple, ?> keyFunction);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream setParallel(Supplier<Integer> width);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream endParallel();
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream autonomous();
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream setConsistent(ConsistentRegionConfig config);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream colocate(Placeable<?>... elements);
    
    /**
     * {@inheritDoc}
     */
    @Override
    SPLStream invocationName(String name);
}
