/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.test.TestTopology;

/**
 * Tests to verify that the correct source information is determined for a stream.
 *
 */
public class SourceInfoTest extends TestTopology {

    @Before
    public void onlyMainRun() {
        assumeTrue(isMainRun());
    }
    
    @Test
    public void testSourceInfo() {
        
        Topology topo = new Topology();
        
        TStream<?> s = topo.constants(Collections.emptyList());
        int line = check(s, "constants", 0);
        
        s = topo.endlessSource(() -> 3);
        line = check(s, "endlessSource", line);
        
    }
    
    private int check(TStream<?> s, String api, int lastLine) {
        
        BOperatorInvocation op = s.operator();
        assertTrue(op._json().has("sourcelocation"));
        assertTrue(op._json().get("sourcelocation").isJsonArray());
        
        JsonArray sls = op._json().get("sourcelocation").getAsJsonArray();
        
        assertEquals(1, sls.size());        
        assertTrue(sls.get(0).isJsonObject());
        
        JsonObject sl = sls.get(0).getAsJsonObject();
        
        System.err.println(sl);
        
        assertEquals(api, sl.get("api.method").getAsString());
        
        assertTrue(sl.get("file").getAsString(), sl.get("file").getAsString().endsWith("SourceInfoTest.java"));
        assertEquals(getClass().getName(), sl.get("class").getAsString());
        assertEquals("testSourceInfo", sl.get("method").getAsString());
        
        assertTrue(sl.get("line").isJsonPrimitive());
        int thisLine = sl.get("line").getAsInt();
        
        assertTrue(thisLine + " > " + lastLine, thisLine > lastLine);
        return thisLine;
    }
}
