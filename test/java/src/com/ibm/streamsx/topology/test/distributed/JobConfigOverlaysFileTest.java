/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Test submission using JobConfig API
 */
public class JobConfigOverlaysFileTest extends TestTopology {
    
    @Before
    public void checkIsStandalone() {
        checkMinimumVersion("JobConfigOverlays", 4, 2);
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.STANDALONE_TESTER);
    }
    
    @SuppressWarnings("unchecked")
    private StreamsContext<File> bundler() {
        return (StreamsContext<File>) StreamsContextFactory.getStreamsContext(Type.BUNDLE);
    }
    /**
     * Assert the returned sab is as expected.
     * @param sab
     * @param namespace
     * @param name
     * @return
     */
    private File assertSabGetJco(File sab, Topology topology) {
        assertNotNull(sab);
        assertTrue(sab.getName().endsWith(".sab"));
        assertTrue(sab.exists());
        assertTrue(sab.isFile());
        
        String sabName = "com.ibm.streamsx.topology.test." + topology.getName() + ".sab";
        assertEquals(sabName, sab.getName());
        
        return null;
    }

    @Test
    public void testNoConfig() throws Exception {
        
        // Just a simple graph, which won't be executed.
        Topology topology = newTopology("testNoConfig");
        topology.constants(Collections.emptyList());
        
        File sab = bundler().submit(topology).get();
        assertSabGetJco(sab, topology);
        
    }
}
