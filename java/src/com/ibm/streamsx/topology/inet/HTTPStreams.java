/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.inet;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.json.JSONSchemas;
import com.ibm.streamsx.topology.json.JSONStreams;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

/**
 * Access to data in web servers or services using HTTP.
 *
 * @see <a
 *      href="http://ibmstreams.github.io/streamsx.inet/">com.ibm.streamsx.inet</a>
 */
public class HTTPStreams {

    /**
     * Periodically poll a web service using HTTP {@code GET} for
     * {@code application/json} data. Declares a source stream that will contain
     * a single tuple for each successful {@code GET}. The tuple is the complete
     * JSON ({@code application/json} content) returned by the request.
     * 
     * @param te
     *            Topology the source stream will be contained in.
     * @param url
     *            URL to poll.
     * @param period
     *            Polling period.
     * @param unit
     *            Unit for {@code period}.
     * @return Stream that will contain the JSON tuples from periodic HTTP
     *         {@code GET} requests.
     */
    public static TStream<JSONObject> getJSON(TopologyElement te, String url,
            long period,
            TimeUnit unit) {

        Map<String, Object> params = new HashMap<>();
        params.put("url", url);
        double dperiod = (unit.toMillis(period) / 1000.0);
        params.put("period", dperiod);

        SPLStream rawJson = SPL.invokeSource(
                te,
                "com.ibm.streamsx.inet.http::HTTPGetJSONContent",
 params,
                JSONSchemas.JSON);
        
        TStream<String> string = SPLStreams.toStringStream(rawJson);

        return JSONStreams.deserialize(string);
    }

}
