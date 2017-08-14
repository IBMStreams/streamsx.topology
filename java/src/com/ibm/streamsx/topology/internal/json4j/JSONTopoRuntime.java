/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017
 */
package com.ibm.streamsx.topology.internal.json4j;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.EncodingFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.json.JSONStreams.DeserializeJSON;

/**
 * Classes used by topology api that depend on JSON4J
 * and are used at runtime.
 * 
 * Note these are not part of the public api.
 *
 */
public class JSONTopoRuntime {

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

    public static class Tuple2JSON implements Function<Tuple, JSONObject> {
        private static final long serialVersionUID = 1L;
    
        @Override
        public JSONObject apply(Tuple tuple) {
            return EncodingFactory
                    .getJSONEncoding().encodeTuple(tuple);
        }
    }

}
