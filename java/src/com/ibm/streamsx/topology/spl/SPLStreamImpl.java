/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.CharacterEncoding;
import com.ibm.streams.operator.encoding.EncodingFactory;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.internal.core.StreamImpl;
import com.ibm.streamsx.topology.internal.spljava.Schemas;
import com.ibm.streamsx.topology.json.JSONSchemas;
import com.ibm.streamsx.topology.json.JSONStreams.DeserializeJSON;

class SPLStreamImpl extends StreamImpl<Tuple> implements SPLStream {

    public SPLStreamImpl(TopologyElement te, BOutput stream) {
        super(te, stream, Tuple.class);
    }

    @Override
    public SPLStream getStream() {
        return this;
    }

    @Override
    public StreamSchema getSchema() {
        return output().schema();
    }

    @Override
    public TStream<JSONObject> toJSON() {
        return transform(
                JSONSchemas.JSON.equals(getSchema()) ?
                        new JsonString2JSON() : new Tuple2JSON());
    }

    public static class Tuple2JSON implements Function<Tuple, JSONObject> {
        private static final long serialVersionUID = 1L;

        @Override
        public JSONObject apply(Tuple tuple) {
            return EncodingFactory
                    .getJSONEncoding().encodeTuple(tuple);
        }
    }
    
    /**
     * Deserialize from tuple<rstring jsonString>
     *
     */
    public static class JsonString2JSON implements Function<Tuple, JSONObject> {
        private static final long serialVersionUID = 1L;
        
        private final DeserializeJSON deserializer = new DeserializeJSON();

        @Override
        public JSONObject apply(Tuple tuple) {
            return deserializer.apply(tuple.getString(0));
        }
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
        if (!Schemas.STRING.equals(getSchema()))
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
       
    private SPLStream asSPL(TStream<Tuple> tupleStream) {
        // must have been created from addMatchingOutput or addMatchingStream
        return (SPLStream) tupleStream;
    }
    
    protected SPLStream addMatchingOutput(BOperatorInvocation bop, Type tupleType) {
        return new SPLStreamImpl(this, bop.addOutput(getSchema())); 
    }
    protected SPLStream addMatchingStream(BOutput output) {
        return new SPLStreamImpl(this, output);
    }
    
    @Override
    public SPLStream parallel(int width) {
        return asSPL(super.parallel(width));
    }
    
    @Override
    public SPLStream parallel(Supplier<Integer> width,
            com.ibm.streamsx.topology.TStream.Routing routing) {
        if(routing != TStream.Routing.ROUND_ROBIN){
            throw new IllegalArgumentException("Partitioning is not currently "
                    + "supported with SPLStream.");
        }
        return asSPL(super.parallel(width, routing));
    }
    @Override
    public SPLStream parallel(Supplier<Integer> width,
            Function<Tuple, ?> keyer) {
        throw new IllegalArgumentException("Partitioning is not currently "
                + "supported with SPLStream.");
    }
    
    @Override
    public SPLStream endParallel() {
        return asSPL(super.endParallel());
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
