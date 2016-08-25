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
/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.spl;

import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.ibm.streams.operator.version.Version;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.spl.JavaPrimitive;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLSchemas;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.test.TestTopology;

public class SPLJavaPrimitiveTest extends TestTopology {
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
    public void testSimpleInvoke() throws Exception {
        Topology t = new Topology("testSimpleInvoke"); 
        SPL.addToolkit(t, new File(getTestRoot(), "spl/testtk"));

        SPLStream spl = SPLStreams.stringToSPLStream(t.strings("hello"));
        
        SPLStream splResult = JavaPrimitive.invokeJavaPrimitive(
                                testjava.NoOpJavaPrimitive.class, 
                                spl, SPLSchemas.STRING, null);

        TStream<String> result = splResult.toStringStream();
        
        completeAndValidate(result, 10, "hello");
    }
    
    @Test(expected=java.lang.Exception.class)
    public void testIncompatVmArgs() throws Exception {
        // incompat args only happens with SPL code
        assumeTrue(SC_OK);
        assumeTrue(!isEmbedded());
        
        skipVersion("vmargs", 4, 2);
                
        // the SPL compiler catches/enforces that all fused Java ops
        // have the same vmArgs.  Since this java op invocation
        // specifies a vmArg value and it's fused with other
        // (functional) Java ops, expect it to fail with CDISP0789E.
        
        Topology t = new Topology("testIncompatVmArgs"); 
        SPL.addToolkit(t, new File(getTestRoot(), "spl/testtk"));
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);

        SPLStream spl = SPLStreams.stringToSPLStream(t.strings("hello"));
        
        Map<String,Object> params = new HashMap<>();
        params.put("vmArg", "-DXYZZY");
        SPLStream splResult = JavaPrimitive.invokeJavaPrimitive(
                                testjava.NoOpJavaPrimitive.class, 
                                spl, SPLSchemas.STRING, params);

        TStream<String> result = splResult.toStringStream();
        
        completeAndValidate(result, 1, "Shouldn't get anything");
    }
    
    @Test
    public void testIsolatedVmArgs2() throws Exception {
        // isolation only works in DISTRIBUTED
        assumeTrue(getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
        assumeTrue(SC_OK);
        
        Topology t = new Topology("testIsolatedVmArgs1"); 
        SPL.addToolkit(t, new File(getTestRoot(), "spl/testtk"));
        // getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);

        SPLStream spl = SPLStreams.stringToSPLStream(t.strings("hello"));

        // isolate the JavaPrimitive to avoid the restriction/failure
        // demonstrated in testIncompatVmArgs
        
        spl = spl.isolate();
        Map<String,Object> params = new HashMap<>();
        params.put("vmArg", new String[] { "-DXYZZY", "-DFOOBAR" });
        SPLStream splResult = JavaPrimitive.invokeJavaPrimitive(
                                testjava.NoOpJavaPrimitive.class, 
                                spl, SPLSchemas.STRING, params);
        splResult = splResult.isolate();

        TStream<String> result = splResult.toStringStream();
        
        completeAndValidate(result, 10, "hello");
    }
    
}

