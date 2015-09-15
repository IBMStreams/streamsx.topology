/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.AllowAll;
import com.ibm.streamsx.topology.test.TestTopology;

public class ToolkitTest extends TestTopology {

    @Before
    public void runOnce() {
        assumeTrue(isMainRun());
    }

    @Test
    public void testParallel() throws Exception {

        final Topology topology = new Topology("TKParallel");
        TStream<Number> s1 = topology.numbers(1, 2, 3, 94, 5, 6).parallel(6)
                .filter(new AllowAll<Number>()).endParallel();
        @SuppressWarnings("unused")
        TStream<String> sp = StringStreams.toString(s1);

        @SuppressWarnings("unchecked")
        StreamsContext<File> tkContext = (StreamsContext<File>) StreamsContextFactory
                .getStreamsContext(Type.TOOLKIT);

        File tkRoot = tkContext.submit(topology).get();

        assertNotNull(tkRoot);
        assertTrue(tkRoot.exists());
    }

}
