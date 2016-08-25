/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.inet;

import java.util.HashMap;
import java.util.Map;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.json.JSONStreams;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.spl.SPLWindow;
import com.ibm.streamsx.topology.tuple.JSONAble;

/**
 * Adds a HTTP REST server to a topology to provide external interaction with
 * the application.
 * 
 * @see <a
 *      href="http://ibmstreams.github.io/streamsx.inet/">com.ibm.streamsx.inet</a>
 */
public class RestServer {
    
    private final Topology topology;
    private final int port;
    private Placeable<?> firstInvocation;
    
    /**
     * Create a REST server that will listen on port 8080.
     * 
     * @param te
     *            Topology to contain the REST server.
     */
    public RestServer(TopologyElement te) {
        this(te, 8080);
    }

    /**
     * Create a REST server that will listen on the specified port.
     * 
     * @param te
     *            Topology to contain the REST server.
     * @param port
     *            Port to listen on.
     */
    public RestServer(TopologyElement te, int port) {
        this.topology = te.topology();
        this.port = port;
    }

    /**
     * Get the topology for this REST server.
     * 
     * @return Topology for this server.
     */
    public Topology topology() {
        return topology;
    }

    /**
     * Declare a HTTP REST view of tuples in a window. A HTTP GET to the the
     * server at runtime will return the current contents of the window as an
     * array of JSON objects, with each tuple converted to a JSON object.
     * 
     * <BR>
     * Conversion to JSON depends on the type of stream
     * that populates {@code window}.
     * <UL>
     * <LI>
     * {@code TStream<JSONObject>} - Each tuple is the JSON object.
     * </LI>
     * <LI>
     * {@code TStream<? extends JSONAble>} - Each tuple is converted to a JSON object
     * using {@link JSONAble#toJSON()}.
     * </LI>
     * <LI>
     * {@code TStream<String>} - Each tuple will be converted to a JSON object with a single
     * attribute '{@code string}' with the tuple's value.
     * </LI>
     * <LI>{@link com.ibm.streamsx.topology.spl.SPLStream SPLStream} -
     * Each {@code Tuple} is converted to JSON using the encoding provided
     * by the Java Operator API {@code com.ibm.streams.operator.encoding.JSONEncoding}.
     * </LI>

     * </UL>
     * Other stream types are not supported.
     * 
     * @param window
     *            Window to expose through the HTTP GET
     * @param context
     *            Context path for the URL to access the resource.
     * @param name
     *            Relative name (to {@code context}) of the resource.
     * 
     * @return null (future will be the path to the tuples)
     */
    @SuppressWarnings("unchecked")
    public String viewer(TWindow<?,?> window, String context,
            String name) {
        
        if (window.getStream() instanceof SPLStream)
            return viewerSpl((TWindow<Tuple,?>) window, context, name);

        if (String.class.equals(window.getTupleClass())) {
            return viewerString((TWindow<String,?>) window, context, name);
        }
        
        if (JSONObject.class.equals(window.getTupleClass())) {
            return viewerJSON((TWindow<JSONObject,?>) window, context, name);
        }
        if (JSONAble.class.isAssignableFrom(window.getTupleClass())) {
            return viewerJSONable((TWindow<? extends JSONAble,?>) window, context, name);
        }

        throw new IllegalArgumentException("Stream type not yet supported!:"
                + window.getTupleClass());
    }
    
    private String viewerJSON(TWindow<JSONObject,?> window,
            String context, String name) {
        
        SPLStream splStream = JSONStreams.toSPL(window.getStream());

        return viewerSpl(splStream.window(window), context, name);
    }
    private String viewerJSONable(TWindow<? extends JSONAble,?> window,
            String context, String name) {
        
        TStream<JSONObject> jsonStream = JSONStreams.toJSON(window.getStream());
        
        SPLStream splStream = JSONStreams.toSPL(jsonStream);

        return viewerSpl(splStream.window(window), context, name);
    }
    
    private String viewerString(TWindow<String,?> window,
            String context, String name) {

        SPLStream splStream = SPLStreams.stringToSPLStream(window.getStream());

        return viewerSpl(splStream.window(window), context, name);
    }

    private String viewerSpl(TWindow<Tuple,?> window, String context, String name) {

        assert window.getStream() instanceof SPLStream;
        SPLWindow splWindow = SPLStreams.triggerCount(window, 1);


        Map<String, Object> params = new HashMap<>();
        params.put("port", port);
        params.put("context", context);
        params.put("contextResourceBase", "opt/html/" + context);

        TSink tv = SPL.invokeSink(name, "com.ibm.streamsx.inet.rest::HTTPTupleView",
                splWindow,
                params);
        if (firstInvocation == null)
            firstInvocation = tv;
        else
            firstInvocation.colocate(tv);

        // Always mapping to a single operator per window
        // so currently it's always port 0.
        return name + "/ports/input/0";
    }
}
