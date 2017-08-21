/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streams.operator.types.XML;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.test.TestTopology;

@SuppressWarnings("serial")
public class XMLTupleTest extends TestTopology {
    @BeforeClass
    public static void checkHasStreamsInstall() {
        // Requires Java Operator types
        assumeTrue(hasStreamsInstall());
    }
   
    @Test
    public void testConstant() throws Exception {
        
        final Topology topology = newTopology();
        String sdata = "<book><title>Dracula</title><author>Bram Stoker</author></book>";
        byte[] data = sdata.getBytes(StandardCharsets.UTF_8);
        XML xml = ValueFactory.newXML(new ByteArrayInputStream(data));
        TStream<XML> source = topology.constants(Collections.singletonList(xml)).asType(XML.class);
        assertNotNull(source);
        assertEquals(XML.class, source.getTupleClass());
        
        TStream<String> out = convertXMLToString(source);
        completeAndValidate(out, 10,  sdata);
    }

    private static TStream<String> convertXMLToString(TStream<XML> source) {
        TStream<String> out = source.transform(new Function<XML,String>() {

            @Override
            public String apply(XML v) {
                
                StreamSource ss = v.getStreamSource();
                
                try {
                    Transformer transformer = TransformerFactory.newInstance().newTransformer();
                    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                    
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    
                    transformer.transform(ss, new StreamResult(out));
                    return new String(out.toByteArray(), StandardCharsets.UTF_8);
                } catch (Exception e) {
                    return null;
                }
            }});
        return out;
    }
}
