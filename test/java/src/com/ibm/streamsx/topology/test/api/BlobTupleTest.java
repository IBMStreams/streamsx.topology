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

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.junit.Test;

import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.test.TestTopology;

@SuppressWarnings("serial")
public class BlobTupleTest extends TestTopology {
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
