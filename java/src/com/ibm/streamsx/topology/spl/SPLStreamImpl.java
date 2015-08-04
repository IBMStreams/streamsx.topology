/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import java.io.IOException;
import java.io.ObjectInputStream;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.CharacterEncoding;
import com.ibm.streams.operator.encoding.EncodingFactory;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.core.StreamImpl;
import com.ibm.streamsx.topology.internal.spljava.Schemas;
import com.ibm.streamsx.topology.json.JSONSchemas;
import com.ibm.streamsx.topology.json.JSONStreams.DeserializeJSON;

class SPLStreamImpl extends StreamImpl<Tuple> implements SPLStream {

    public SPLStreamImpl(TopologyElement te, BOutputPort stream) {
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
    public <T> TStream<T> convert(Function<Tuple, T> convertor,
            Class<T> tupleTypeClass) {
        return transform(convertor, tupleTypeClass);
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
    public TStream<Tuple> parallel(int width,
            com.ibm.streamsx.topology.TStream.Routing routing) {
        if(routing != TStream.Routing.ROUND_ROBIN){
            throw new IllegalArgumentException("Partitioning is not currently "
                    + "supported with SPLStreams.");
        }
        return super.parallel(width, routing);
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
