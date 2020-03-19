/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.flow.handlers.StreamCollector;
import com.ibm.streams.flow.handlers.StreamCounter;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TStream.Routing;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * endParallel() on non-parallel stream should not cause sc error
 */
public class EndParallelTest extends TestTopology {
    
    @Before
    public void checkOK() {
        assumeSPLOk();
    }

    @BeforeClass
    public static void maybeSkip() {
        assumeTrue(false); // TODO remove to enable the test class for error analysis
    }
  
    @Test
    public void testInvalidEndParallel() throws Exception {

        Topology topology = newTopology("endParallel");

        TStream<String> stream = topology.constants(Arrays.asList("Boom!"));
        stream.endParallel(); // endParallel() on a stream that's not parallel

        // should not generate invalid SPL
        this.getConfig().put(com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS, Boolean.TRUE);
        StreamsContextFactory
                .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
                .submit(topology).get();
    }   

}
