/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function7.Function;

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
     * {@code transform(converter, tupleTypeClass)}.
     * 
     * @param convertor
     *            Function to convert
     * @param tupleTypeClass
     *            Type {@code T} of the returned stream.
     * @return Stream containing tuples of type {@code T} transformed from this
     *         stream's SPL tuples.
     * 
     * @see TStream#transform(Function, Class)
     */
    <T> TStream<T> convert(Function<Tuple, T> convertor, Class<T> tupleTypeClass);

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
}
