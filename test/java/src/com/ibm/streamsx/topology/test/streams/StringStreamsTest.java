/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.streams;

import static com.ibm.streamsx.topology.test.api.TopologyTest.checkPrintEmbedded;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;

public class StringStreamsTest extends TestTopology {

    @Test
    public void testContains() throws Exception {
        final Topology f = new Topology("Contains");
        TStream<String> source = f.strings("abc", "bcd", "cde");
        TStream<String> filtered = StringStreams.contains(source, "bc");
        assertNotNull(filtered);
        
        completeAndValidate(filtered, 10, "abc", "bcd");
    }

    @Test
    public void testContainsNoMatch() throws Exception {
        assumeTrue(isEmbedded());  // checkPrint() forces embedded context
        final Topology f = new Topology("ContainsNoMatch");
        TStream<String> source = f.strings("abc", "bcd", "cde");
        TStream<String> filtered = StringStreams.contains(source, "xyz");
        assertNotNull(filtered);
        filtered.print();
        checkPrintEmbedded(f);
    }
}
