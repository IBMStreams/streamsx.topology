/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.json;

import java.io.IOException;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * Utilities for JSON streams.
 * 
 */
public class JSONStreams {
    
    /**
     * When the JSON is an array the approach is to use an
     * {@code JSONObject} with a single attribute {@value}
     * containing the value.
     */
    public static final String PAYLOAD = "payload";

    /**
     * Function to deserialize a String to a JSONObject.
     * If the serialized JSON is an array,
     * then a JSON object is created, with
     * a single attribute {@code payload} containing the deserialized
     * value.
      */
    public static final class DeserializeJSON implements
            Function<String, JSONObject> {
        private static final long serialVersionUID = 1L;

        @Override
        public JSONObject apply(String tuple) {
            try {
                JSONArtifact artifact = JSON.parse(tuple);
                if (artifact instanceof JSONObject)
                    return (JSONObject) artifact;
                JSONObject wrapper = new JSONObject();
                wrapper.put(PAYLOAD, artifact);
                return wrapper;
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
     * Function to convert a {@link JSONAble}
     * tuple to a {@code JSONObject}, using {@link JSONAble#toJSON()}. 
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
     * Convert a JSON stream to an SPLStream.
     * @param stream JSON stream to be converted.
     * @return SPLStream with schema {@link JSONSchemas#JSON}.
     */
    public static SPLStream toSPL(TStream<JSONObject> stream) {
        
        return SPLStreams.convertStream(stream, 
                new BiFunction<JSONObject, OutputTuple, OutputTuple>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public OutputTuple apply(JSONObject v1, OutputTuple v2) {
                        try {
                            v2.setString(0, v1.serialize());
                            return v2;
                        } catch (IOException e) {
                            return null;
                        }
                    }
        }, JSONSchemas.JSON);
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
     * Declare a stream of JSON objects from a stream of serialized JSON tuples.
     * If the serialized JSON is a simple value or an array,
     * then a JSON object is created, with
     * a single attribute {@code payload} containing the deserialized
     * value.
     * @param stream
     *            Stream containing the JSON serialized values.
     * @return Stream that will contain the JSON objects.
     */
    public static TStream<JSONObject> deserialize(TStream<String> stream) {
        return stream.transform(new DeserializeJSON(), JSONObject.class);
    }
    
    /**
     * Declare a stream of JSON objects from a stream
     * of Java objects that implement {@link JSONAble}.  
     * @param stream Stream containing {@code JSONAble} tuples.
     * @return Stream that will contain the JSON objects.
     */
    public static <T extends JSONAble> TStream<JSONObject> toJSON(TStream<T> stream) {
        return stream.transform(new ToJSON<T>(), JSONObject.class);
    }
}
