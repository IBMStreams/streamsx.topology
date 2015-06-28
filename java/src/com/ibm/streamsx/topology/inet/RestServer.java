/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.inet;

import java.util.HashMap;
import java.util.Map;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.spl.SPLWindow;

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
     * array of JSON objects.
     * 
     * <BR>
     * Only windows for streams of type {@code SPLStream} and
     * {@code TStream<String>} are currently supported.
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
    public String viewer(TWindow<?> window, String context,
            String name) {
        
        if (window.getStream() instanceof SPLStream)
            return viewerSpl((TWindow<Tuple>) window, context, name);

        if (String.class.equals(window.getTupleClass())) {
            return viewerString((TWindow<String>) window, context, name);
        }
        /*
         * if (JSONAble.class.isAssignableFrom(window.getTupleClass())) {
         * JSONStreams.toJSON(stream) }
         */

        throw new IllegalArgumentException("Stream type not yet supported!:"
                + window.getTupleClass());
    }
    
    private String viewerString(TWindow<String> window,
            String context, String name) {

        SPLStream splStream = SPLStreams.stringToSPLStream(window.getStream());
        TWindow<Tuple> tWindow = splStream.window(window);

        return viewerSpl(tWindow, context, name);
    }

    private String viewerSpl(TWindow<Tuple> window, String context, String name) {

        assert window.getStream() instanceof SPLStream;
        SPLWindow splWindow = SPLStreams.triggerCount(window, 1);


        Map<String, Object> params = new HashMap<>();
        params.put("port", port);
        params.put("context", context);
        params.put("contextResourceBase", "opt/html/" + context);

        SPL.invokeSink(name, "com.ibm.streamsx.inet.rest::HTTPTupleView",
                splWindow,
                params);

        return null;
    }
}
