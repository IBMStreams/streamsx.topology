/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.json;

import java.io.IOException;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * Utilities for JSON related streams.
 * 
 */
public class JSONStreams {

    /**
     * Function to deserialize a String to a JSONObject.
     */
    public static final class DeserializeJSON implements
            Function<String, JSONObject> {
        private static final long serialVersionUID = 1L;

        @Override
        public JSONObject apply(String v1) {
            try {
                return JSONObject.parse(v1);
            } catch (IOException e) {
                return null;
            }
        }
    }

    /**
     * Function to serialize a JSONObject to a String.
     */
    public static final class SerializeJSON implements
            Function<JSONObject, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String apply(JSONObject v1) {
            try {
                return v1.serialize();
            } catch (IOException e) {
                return null;
            }
        }
    }
    
    /**
     * Function to serialize a JSONObject to a String.
     */
    public static final class ToJSON<T extends JSONAble> implements
            Function<T, JSONObject> {
        private static final long serialVersionUID = 1L;

        @Override
        public JSONObject apply(JSONAble v1) {
            return v1.toJSON();
        }
    }

    /**
     * Create a stream of serialized JSON objects as String tuples.
     * 
     * @param stream
     *            Stream containing the JSON objects.
     * @return Stream that will contain the serialized JSON values.
     */
    public static TStream<String> serialize(TStream<JSONObject> stream) {
        return stream.transform(new SerializeJSON(), String.class);
    }

    /**
     * Create a stream of JSON objects from a stream of serialized JSON tuples.
     * 
     * @param stream
     *            Stream containing the JSON serialized values.
     * @return Stream that will contain the JSON objects.
     */
    public static TStream<JSONObject> deserialize(TStream<String> stream) {
        return stream.transform(new DeserializeJSON(), JSONObject.class);
    }
    
    public static <T extends JSONAble> TStream<JSONObject> toJSON(TStream<T> stream) {
        return stream.transform(new ToJSON<T>(), JSONObject.class);
    }
}
