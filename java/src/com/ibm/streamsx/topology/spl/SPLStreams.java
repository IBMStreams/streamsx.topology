/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import static com.ibm.streamsx.topology.spl.SPLStreamImpl.newSPLStream;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.core.JavaFunctional;
import com.ibm.streamsx.topology.internal.core.JavaFunctionalOps;
import com.ibm.streamsx.topology.internal.logic.LogicUtils;
import com.ibm.streamsx.topology.internal.messages.Messages;

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
    	return _subscribe(te, topic, schema);
    }

    /**
     * Subscribe to an {@link SPLStream} published by topic.
     * 
     * Supports {@code topic} as a submission time parameter, for example
     * using the topic defined by the submission parameter {@code eventTopic}.:
     * 
     * <pre>
     * <code>
     * Supplier<String> topicParam = topology.createSubmissionParameter("eventTopic", String.class);
     * SPLStream events = SPLStreams.subscribe(topology, topicParam, schema);
     * </code>
     * </pre>
     * 
     * @param te
     *            Topology the stream will be contained in.
     * @param topic
     *            Topic to subscribe to.
     * @param schema
     *            SPL Schema of the published stream.
     * @return Stream containing tuples for the published topic.
     * 
     * @see Topology#createSubmissionParameter(String, Class)
     * @see Topology#createSubmissionParameter(String, Object)
     * 
     * @since 1.8
     */
    public static SPLStream subscribe(TopologyElement te, Supplier<String> topic, StreamSchema schema) {
    	return _subscribe(te, topic, schema);
    }

    private static SPLStream _subscribe(TopologyElement te, Object topic, StreamSchema schema) {
        Map<String, Object> params = new HashMap<>();
                
        params.put("topic", requireNonNull(topic));
        params.put("streamType", requireNonNull(schema));

        SPLStream stream = SPL.invokeSource(te,
                "com.ibm.streamsx.topology.topic::Subscribe",
                params, schema);

        return stream;
    }
    
    /**
     * Convert a {@code Stream} to an {@code SPLStream}. For each tuple
     * {@code t} on {@code stream}, the returned stream will contain a tuple
     * that is the result of {@code converter.apply(t, outTuple)} when the
     * return is not {@code null}. {@code outTuple} is a newly created, empty,
     * {@code OutputTuple}, the {@code converter.apply()} method populates
     * {@code outTuple} from {@code t}.
     * 
     * <P>
     * Example of converting a stream containing a {@code Sensor} object to an
     * SPL schema of {@code tuple<rstring id, float64 reading>}.
     * 
     * <pre>
     * <code>
     * Stream&lt;Sensor> sensors = ...
     * StreamSchema schema = Type.Factory.getStreamSchema("tuple&lt;rstring id, float64 reading>");
     * SPLStream splSensors = SPLStreams.convertStream(sensors,
     *   new BiFunction&lt;Sensor, OutputTuple, OutputTuple>() {
     *             &#64;Override
     *             public OutputTuple apply(Sensor sensor, OutputTuple outTuple) {
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
     *            Schema of returned SPLStream.
     * @return SPLStream containing the converted tuples.
     * 
     * @see SPLStream#convert(com.ibm.streamsx.topology.function.Function)
     */
    public static <T> SPLStream convertStream(TStream<T> stream,
            BiFunction<T, OutputTuple, OutputTuple> converter,
            StreamSchema schema) {
        
        String opName = LogicUtils.functionName(converter);

        BOperatorInvocation convOp = JavaFunctional.addFunctionalOperator(
                stream, opName, JavaFunctionalOps.CONVERT_SPL_KIND, converter);
        @SuppressWarnings("unused")
        BInputPort inputPort = stream.connectTo(convOp, true, null);

        return newSPLStream(stream, convOp, schema, true);
    }

    /**
     * Convert an {@link SPLStream} to a TStream&lt;String>
     * by taking its first attribute.
     * The returned stream will contain a {@code String} tuple for
     * each tuple {@code T} on {@code stream}, the value of the
     * {@code String} tuple is {@code T.getString(0)}.
     * A runtime error will occur if the first attribute (index 0)
     * can not be converted using {@code Tuple.getString(0)}.
     * @param stream Stream to be converted to a TStream&lt;String>.
     * @return Stream that will contain the first attribute of tuples from {@code stream}
     */
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
        });
    }
    
    /**
     * Convert an {@link SPLStream} to a TStream&lt;String>
     * by taking a specific attribute.
     * The returned stream will contain a {@code String} tuple for
     * each tuple {@code T} on {@code stream}, the value of the
     * {@code String} tuple is {@code T.getString(attributeName)}.
     * A runtime error will occur if the attribute
     * can not be converted using {@code Tuple.getString(attributeName)}.
     * @param stream Stream to be converted to a TStream&lt;String>.
     * @return Stream that will contain a single attribute of tuples from {@code stream}
     */
    public static TStream<String> toStringStream(SPLStream stream, String attributeName) {

        Attribute attribute = stream.getSchema().getAttribute(attributeName);
        if (attribute == null) {
            throw new IllegalArgumentException(Messages.getString("SPL_ATTRIBUTE_NOT_PRESENT", attributeName, stream.getSchema().getLanguageType()));
        }
        final int attributeIndex = attribute.getIndex();
        return stream.convert(new Function<Tuple, String>() {
           private static final long serialVersionUID = 1L;

            @Override
            public String apply(Tuple tuple) {
                return tuple.getString(attributeIndex);
            }
        });
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
                        v2.setString(0, v1);
                        return v2;
                    }
                }, SPLSchemas.STRING);
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
    public static SPLWindow triggerCount(TWindow<Tuple,?> window, int count) {
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
    public static SPLWindow triggerTime(TWindow<Tuple,?> window, long time,
            TimeUnit unit) {
        return new SPLWindowImpl(window, time, unit);
    }
}
