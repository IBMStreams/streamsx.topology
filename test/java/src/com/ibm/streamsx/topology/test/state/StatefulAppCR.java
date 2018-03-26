/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018 
 */
package com.ibm.streamsx.topology.test.state;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;

/**
 * Testing stateful app that uses consistent region.
 * 
 * As well as being used as a test it can be launched manually
 * using this class as the Java main class.
 *
 * Each sequence metric must be equal to 512,345 once tuples
 * stop flowing regardless of the number of failures.
 *
 */
public class StatefulAppCR {

    public static void main(String[] args) throws InterruptedException, ExecutionException, Exception {
        
        String contextType = Type.DISTRIBUTED.name();
        
        if (args.length == 1)
            contextType = args[0];
        
        Topology topology = new Topology();
        
        createApp(topology, true);
        
        Map<String,Object> config = new HashMap<>();

        @SuppressWarnings("unchecked")
        StreamsContext<BigInteger> context =
             (StreamsContext<BigInteger>) StreamsContextFactory.getStreamsContext(contextType);
        
        BigInteger jobId = context.submit(topology, config).get();

        System.out.println("Submitted job with jobId=" + jobId);
    }
    
    
    public static TStream<Long> createApp(Topology topology, boolean fail) { 
                
        Map<String,Object> params = new HashMap<>();
        params.put("period", 0.0005);
        params.put("iterations", 512_345);
        SPLStream b = SPL.invokeSource(topology, "spl.utility::Beacon", params,
                com.ibm.streams.operator.Type.Factory.getStreamSchema("tuple<int64 a>"));
        
        b.setConsistent(ConsistentRegionConfig.periodic(30));
        
        TStream<Long> s = b.map(t -> t.getLong(0));
        s = s.isolate().filter(new StatefulApp.Stateful.Filter(fail));
        s = s.isolate().map(new StatefulApp.Stateful.Map(fail));
        s.isolate().forEach(new StatefulApp.Stateful.ForEach(fail));
        
        return s;
    }
}
