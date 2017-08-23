/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * Utilities for JSON streams.
 * 
 * A JSON stream is a stream of JSON objects represented
 * by the class {@code com.ibm.json.java.JSONObject}.
 * When a JSON value that is an array or value (not an object)
 * needs to be present on the stream, the approach is to
 * represent it as a object with the key {@link #PAYLOAD payload}
 * containing the array or value.
 * <BR>
 * A JSON stream can be {@link TStream#publish(String) published}
 * so that IBM Streams applications implemented in different languages
 * can subscribe to it.
 * 
 * @see <a href="http://www.json.org/">http://www.json.org - JSON (JavaScript Object Notation) is a lightweight data-interchange format.</a>
 */
public class JSONStreams {
    
    /**
     * JSON key for arrays and values.
     * When JSON is an array or a value (not an object)
     * it is represented as a {@code JSONObject} tuple
     * with an attribute (key) {@value}
     * containing the array or value.
     */
    public static final String PAYLOAD = "payload";

    /**
     * Function to deserialize a String to a JSONObject.
     * If the serialized JSON is an array,
     * then a JSON object is created, with
     * a single key {@code payload} containing the deserialized
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
                throw new RuntimeException(e);
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
        return stream.transform(new SerializeJSON());
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
        return stream.transform(new DeserializeJSON());
    }
    
    /**
     * Declare a stream of JSON objects from a stream
     * of Java objects that implement {@link JSONAble}.  
     * @param stream Stream containing {@code JSONAble} tuples.
     * @return Stream that will contain the JSON objects.
     */
    public static <T extends JSONAble> TStream<JSONObject> toJSON(TStream<T> stream) {
        return stream.transform(new ToJSON<T>());
    }
    
    /**
     * Declare a stream that flattens an array present in the input tuple.
     * For each tuple on {@code stream} the key {@code arrayKey} and its
     * value are extracted and if it is an array then each element in
     * the array will be present on the returned stream as an individual tuple.
     * <BR>
     * If an array element is a JSON object it will be placed on returned stream,
     * otherwise a JSON object will be placed on the returned stream with
     * the key {@link #PAYLOAD payload} containing the element's value.
     * <BR>
     * If {@code arrayKey} is not present, is not an array or is an empty array
     * then no tuples will result on the returned stream.
     * <P>
     * Any additional keys ({@code additionalKeys}) that are specified are
     * copied (with their value) into each JSON object on the returned stream
     * from the input tuple, unless the flattened tuple already contains a value for the key.
     * <BR>
     * If an addition key is not in the input tuple, then it is not copied into
     * the flattened tuples. 
     * </P>
     * <P>
     * For example, with a JSON object input tuple of:
     * <pre>
     * <code>
     * {"ts":"13:28:07", "sensors":
     *   [
     *     {"temperature": 34.2},
     *     {"rainfall": 12.96}
     *   ]
     * }
     * </code>
     * </pre>
     * and a call of {@code flattenArray(stream, "sensors", "ts")} would result in two tuples:
     * <pre>
     * <code>
     * {"temperature": 34.2, "ts": "13:28:07"}
     * {"rainfall": 12.96, "ts": "13:28:07"}
     * </code>
     * </pre>
     * </P>
     * <P>
     * With a JSON input tuple containing an array of simple values:
     * <pre>
     * <code>
     * {"ts":"13:43:09", "unit": "C", "readings":
     *   [
     *     33.9,
     *     33.8,
     *     34.1
     *   ]
     * }
     * </code>
     * </pre>
     * and a call of {@code flattenArray(stream, "readings", "ts", "unit")}
     * would result in three tuples:
     * <pre>
     * <code>
     * {"payload": 33.9, "ts": "13:43:09", "unit":"C"}
     * {"payload": 33.8, "ts": "13:43:09", "unit":"C"}
     * {"payload": 34.1, "ts": "13:43:09", "unit":"C"}
     * </code>
     * </pre>
     * </P>
     * </P>
     * @param stream Steam containing tuples with an array to be flattened.
     * @param arrayKey Key of the array in each input tuple.
     * @param additionalKeys Additional keys that copied from the input tuple into each resultant tuple from the array
     * @return Stream containing tuples flattened from input tuple.
     */
    public static TStream<JSONObject> flattenArray(TStream<JSONObject> stream,
            final String arrayKey, final String... additionalKeys) {
        return stream.flatMap(
                new Function<JSONObject, Iterable<JSONObject>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<JSONObject> apply(JSONObject tuple) {
                        Object oa = tuple.get(arrayKey);
                        if (!(oa instanceof JSONArray))
                            return null;

                        JSONArray ja = (JSONArray) oa;

                        if (ja.isEmpty())
                            return null;
                        
                        JSONObject additional = null;
                        if (additionalKeys.length != 0) {
                            additional = new JSONObject();
                            for (String addKey : additionalKeys) {
                                Object akv = tuple.get(addKey);
                                if (akv != null)
                                    additional.put(addKey, akv);
                            }
                            if (additional.isEmpty())
                                additional = null;
                        }

                        List<JSONObject> tuples = new ArrayList<>(ja.size());
                        for (Object av : ja) {
                            JSONObject flattened;
                            if (av instanceof JSONObject) {
                                flattened = (JSONObject) av;
                            } else {
                                flattened = new JSONObject();
                                flattened.put(PAYLOAD, av);
                            }
                            if (additional != null) {
                                for (Object addKey : additional.keySet()) {
                                    if (!flattened.containsKey(addKey))
                                        flattened.put(addKey, additional.get(addKey));
                                }
                            }

                            tuples.add(flattened);
                        }

                        return tuples;
                    }
                });
    }
}
