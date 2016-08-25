/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.junit.Test;

import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streams.operator.types.XML;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.test.TestTopology;

@SuppressWarnings("serial")
public class XMLTupleTest extends TestTopology {
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */
   
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
