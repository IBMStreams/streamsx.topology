/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.internal.core.JavaFunctional;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionConvertToSPL;
import com.ibm.streamsx.topology.internal.spljava.Schemas;

/**
 * Utilities for SPL attribute schema streams.
 * 
 */
public class SPLStreams {
    /**
     * Subscribe to an {@link SPLStream} published by topic.
     * 
     * @param te
     *            Topology the stream will be contained in.
     * @param topic
     *            Topic to subscribe to.
     * @param schema
     *            SPL Schema of the published stream.
     * @return Stream containing tuples for the published topic.
     */
    public static SPLStream subscribe(TopologyElement te, String topic,
            StreamSchema schema) {

        Map<String, Object> params = new HashMap<>();

        params.put("topic", topic);
        params.put("streamType", schema);

        SPLStream stream = SPL.invokeSource(te,
                "com.ibm.streamsx.topology.topic::Subscribe",
                params, schema);

        return stream;
    }

    /**
     * Convert a {@code Stream} to an {@code SPLStream}. For each tuple
     * {@code t} on {@code stream}, the returned stream will contain a tuple
     * that is the result of {@code transformer.call(t, outTuple)} when the
     * return is not {@code null}. {@code outTuple} is a newly created, empty,
     * {@code OutputTuple}, the {@code convert.call()} method populates
     * {@code outTuple} from {@code t}.
     * 
     * <P>
     * Example of transforming a stream containing a {@code Sensor} object to an
     * SPL schema of {@code tuple<rstring id, float64 reading>}.
     * 
     * <pre>
     * <code>
     * Stream&lt;Sensor> sensors = ...
     * StreamSchema schema = Type.Factory.getStreamSchema("tuple&lt;rstring id, float64 reading>");
     * SPLStream splSensors = SPLStreams.convertStream(sensors,
     *   new Function2&lt;Sensor, OutputTuple, OutputTuple>() {
     *             &#64;Override
     *             public OutputTuple call(Sensor sensor, OutputTuple outTuple) {
     *                 outTuple.setString("id", sensor.getId());
     *                 outTuple.setDouble("reading", sensor.getReading());
     *                 return outTuple;
     *             }}, schema);
     * </code>
     * </pre>
     * 
     * </P>
     * 
     * @param stream
     *            Stream to be converted.
     * @param converter
     *            Converter used to populate the SPL tuple.
     * @param schema
     *            Schema of returned stream.
     * @return Stream containing the converted tuples.
     * 
     * @see SPLStream#convert(com.ibm.streamsx.topology.function7.Function,
     *      Class)
     */
    public static <T> SPLStream convertStream(TStream<T> stream,
            BiFunction<T, OutputTuple, OutputTuple> converter,
            StreamSchema schema) {
        
        String opName = converter.getClass().getSimpleName();
        if (opName.isEmpty()) {
            opName = "SPLConvert" + stream.getTupleClass().getSimpleName();
        }

        BOperatorInvocation convOp = JavaFunctional.addFunctionalOperator(
                stream, opName, FunctionConvertToSPL.class, converter);
        BInputPort inputPort = stream.connectTo(convOp, true, null);

        BOutputPort convertedTuples = convOp.addOutput(schema);
        return new SPLStreamImpl(stream, convertedTuples);
    }

    public static TStream<String> toStringStream(SPLStream stream) {

        return stream.convert(new Function<Tuple, String>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(Tuple tuple) {
                return tuple.getString(0);
            }
        }, String.class);
    }

    /**
     * Represent {@code stream} as an {@link SPLStream} with schema
     * {@link SPLSchemas#STRING}.
     * 
     * @param stream
     *            Stream to be represented as an {@code SPLStream}.
     * @return {@code SPLStream} representation of {@code stream}.
     */
    public static SPLStream stringToSPLStream(TStream<String> stream) {
        return convertStream(stream,
                new BiFunction<String, OutputTuple, OutputTuple>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public OutputTuple apply(String v1, OutputTuple v2) {
                        // TODO Auto-generated method stub
                        v2.setString(0, v1);
                        return v2;
                    }
                }, Schemas.STRING);
    }

    /**
     * Convert window to an SPL window with a count based trigger policy.
     * 
     * @param window
     *            Window to be converted.
     * @param count
     *            Count trigger policy value
     * @return SPL window with that will trigger every {@code count} tuples.
     */
    public static SPLWindow triggerCount(TWindow<Tuple> window, int count) {
        return new SPLWindowImpl(window, count);
    }

    /**
     * Convert window to an SPL window with a time based trigger policy.
     * 
     * @param window
     *            Window to be converted.
     * @param time
     *            TIme trigger policy value.
     * @param unit
     *            Unit for {@code time}.
     * @return SPL window with that will trigger periodically according to
     *         {@code time}.
     */
    public static SPLWindow triggerTime(TWindow<Tuple> window, long time,
            TimeUnit unit) {
        return new SPLWindowImpl(window, time, unit);
    }
}
