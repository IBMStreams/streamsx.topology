/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.CharacterEncoding;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.internal.core.StreamImpl;
import com.ibm.streamsx.topology.internal.json4j.JSONTopoRuntime;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.json.JSONSchemas;

class SPLStreamImpl extends StreamImpl<Tuple> implements SPLStream {

    private final StreamSchema schema;
    
    static SPLStream newSPLStream(TopologyElement te, BOperatorInvocation op, StreamSchema schema,
            boolean singleOutput) {
        return new SPLStreamImpl(te, schema,
                op.addOutput(schema.getLanguageType(),
                        singleOutput ? Optional.of(op.name()) : Optional.empty()));
    }
    
    private SPLStreamImpl(TopologyElement te, StreamSchema schema, BOutput stream) {
        super(te, stream, Tuple.class, Optional.empty());
        this.schema = schema;
    }

    @Override
    public SPLStream getStream() {
        return this;
    }

    @Override
    public StreamSchema getSchema() {
        return schema;
    }

    @Override
    public TStream<JSONObject> toJSON() {
        return transform(
                JSONSchemas.JSON.equals(getSchema()) ?
                        new JSONTopoRuntime.JsonString2JSON() : new JSONTopoRuntime.Tuple2JSON());
    }

    @Override
    public <T> TStream<T> convert(Function<Tuple, T> convertor) {
        return transform(convertor);
    }

    @Override
    public TStream<String> toTupleString() {
        return transform(new TupleToString(getSchema()));
    }

    @Override
    public TStream<String> toStringStream() {
        if (!SPLSchemas.STRING.equals(getSchema()))
            throw new IllegalStateException(getSchema().getLanguageType());

        return new StreamImpl<String>(this, output(), String.class);
    }
    
    @Override
    public SPLStream filter(Predicate<Tuple> filter) {
        return asSPL(super.filter(filter));       
    }
    @Override
    public SPLStream isolate() {
        return asSPL(super.isolate());
    }
    @Override
    public SPLStream modify(UnaryOperator<Tuple> modifier) {
        return asSPL(super.modify(modifier));
    }
    @Override
    public SPLStream sample(double fraction) {
        return asSPL(super.sample(fraction));
    }
    @Override
    public SPLStream throttle(long delay, TimeUnit unit) {
        return asSPL(super.throttle(delay, unit));
    }
    @Override
    public SPLStream lowLatency() {
        return asSPL(super.lowLatency());
    }
    @Override
    public SPLStream endLowLatency() {
        return asSPL(super.endLowLatency());
    }
    @Override
    public SPLStream autonomous() {
    	return asSPL(super.autonomous());
    }
    @Override
    public SPLStream setConsistent(ConsistentRegionConfig config) {
        super.setConsistent(config);
        return this;
    }
       
    private SPLStream asSPL(TStream<Tuple> tupleStream) {
        // must have been created from addMatchingOutput or addMatchingStream
        return (SPLStream) tupleStream;
    }
    
    protected SPLStream addMatchingOutput(BOperatorInvocation bop, Type tupleType) {
        return new SPLStreamImpl(this, schema, bop.addOutput(getSchema().getLanguageType())); 
    }
    protected SPLStream addMatchingStream(BOutput output) {
        return new SPLStreamImpl(this, getStreamSchema(output._type()), output);
    }
    
    @Override
    public SPLStream parallel(int width) {
        return asSPL(super.parallel(width));
    }
    
    @Override 
    public SPLStream setParallel(Supplier<Integer> width){
    	return asSPL(super.setParallel(width));
    }
    
    @Override
    public SPLStream parallel(Supplier<Integer> width,
            com.ibm.streamsx.topology.TStream.Routing routing) {
        
        switch (requireNonNull(routing)) {
        case ROUND_ROBIN:
        case BROADCAST:
            break;
        default:
            throw new IllegalArgumentException(Messages.getString("SPL_PARTITIONING_NOT_SUPPORTED"));
        }

        return asSPL(super.parallel(width, routing));
    }
    @Override
    public SPLStream parallel(Supplier<Integer> width,
            Function<Tuple, ?> keyer) {
        throw new IllegalArgumentException(Messages.getString("SPL_PARTITIONING_NOT_SUPPORTED"));
    }
    
    @Override
    public SPLStream endParallel() {
        return asSPL(super.endParallel());
    }
    
    @Override
    protected void _publish(Object topic, boolean allowFilter) {
        
        Map<String,Object> publishParms = new HashMap<>();
        publishParms.put("topic", topic);
        publishParms.put("allowFilter", allowFilter);
        
        SPL.invokeSink("com.ibm.streamsx.topology.topic::Publish", this, publishParms);
    }
    
    @Override
    public SPLStream colocate(Placeable<?>... elements) {
        return asSPL(super.colocate(elements));
    }
    @Override
    public SPLStream invocationName(String name) {
        return asSPL(super.invocationName(name));
    }

    public static class TupleToString implements Function<Tuple, String> {
        private static final long serialVersionUID = 1L;

        private final StreamSchema schema;
        private transient CharacterEncoding encoding;

        TupleToString(StreamSchema schema) {
            this.schema = schema;
            setEncoding();
        }

        private void setEncoding() {
            encoding = schema.newCharacterEncoding();
        }

        private void readObject(ObjectInputStream in)
                throws ClassNotFoundException, IOException {
            in.defaultReadObject();
            setEncoding();
        }

        @Override
        public String apply(Tuple tuple) {
            return encoding.encodeTuple(tuple);
        }
    }
}
