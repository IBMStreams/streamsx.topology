/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.test.TestTopology;

@SuppressWarnings("serial")
public class BlobTupleTest extends TestTopology {
    
    @BeforeClass
    public static void checkHasStreamsInstall() {
        // Requires Java Operator types
        assumeTrue(hasStreamsInstall());
    }
   
    @Test
    public void testConstant() throws Exception {
        final Topology topology = newTopology();
        String sdata = "YY" + BlobTupleTest.class.getName();
        byte[] data = sdata.getBytes(StandardCharsets.UTF_8);
        Blob blob = ValueFactory.newBlob(data, 0, data.length);
        TStream<Blob> source = topology.constants(Collections.singletonList(blob)).asType(Blob.class);
        assertNotNull(source);
        assertEquals(Blob.class, source.getTupleClass());
        assertEquals(Blob.class, source.getTupleType());
        
        TStream<String> out = convertBlobToString(source);
        completeAndValidate(out, 10,  sdata);
    }

    private static TStream<String> convertBlobToString(TStream<Blob> source) {
        TStream<String> out = source.transform(new Function<Blob,String>() {

            @Override
            public String apply(Blob v) {
                return new String(v.getData(), StandardCharsets.UTF_8);
            }});
        return out;
    }
}
