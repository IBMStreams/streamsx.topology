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
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.test.TestTopology;

public class VMArgTest extends TestTopology {
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
    public void testSettingSystemProperty() throws Exception {
        
        assumeTrue(!isEmbedded());
        
        final Topology topology = newTopology("testSettingSystemProperty");
        
        final String propertyName = "tester.property.921421";
        final String propertyValue = "abcdef832124";
        final String vmArg = "-D" + propertyName + "=" + propertyValue;
        
        TStream<String> source = topology.limitedSource(new ReadProperty(propertyName), 1);

        final Map<String,Object> config = getConfig();
        @SuppressWarnings("unchecked")
        List<String> vmArgs = (List<String>) config.get(ContextProperties.VMARGS);
        vmArgs.add(vmArg);
        
        // config.put(ContextProperties.KEEP_ARTIFACTS, Boolean.TRUE);
        completeAndValidate(config, source, 10, propertyValue);
    }
    
    @SuppressWarnings("serial")
    public static class ReadProperty implements Supplier<String> {

        private final String property;
        ReadProperty(String property) {
           this.property = property;
        }
        @Override
        public String get() {
            return System.getProperty(property);
        }       
    }
}
