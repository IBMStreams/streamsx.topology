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

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.test.TestTopology;

import ThirdParty.ThirdPartyResource;
import org.junit.Test;

public class ThirdPartyTest extends TestTopology {
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
    public void includeThirdPartyJar() throws Exception {
	String resourceDir = System.getProperty("topology.test.resource_dir");

        final Topology topology = newTopology("BasicStream");
        topology.addJarDependency(resourceDir + "/ThirdPartyResource.jar");
        TStream<String> source = topology.strings("1", "2", "3");
        TStream<String> thirdPartyOutput = source.transform(thirdPartyStaticTransform());
        
        completeAndValidate(thirdPartyOutput, 20,
                "This was returned from a third-party method1",
                "This was returned from a third-party method2",
                "This was returned from a third-party method3");
    }

    @Test
    public void includeThirdPartyClass() throws Exception {
        final Topology topology = newTopology("BasicStream");
        topology.addClassDependency(ThirdPartyResource.class);
        TStream<String> source = topology.strings("1", "2", "3");
        TStream<String> thirdPartyOutput = source.transform(thirdPartyTransform());
        
        completeAndValidate(thirdPartyOutput, 20,
                "This string was set.1",
                "This string was set.2",
                "This string was set.3");
    }

    @SuppressWarnings("serial")
    private static Function<String,String> thirdPartyStaticTransform(){
	return new Function<String, String>(){
            @Override
            public String apply(String v) {
                return ThirdPartyResource.thirdPartyStaticMethod() + v;
            }
        };
    }

    @SuppressWarnings("serial")
    private static Function<String,String> thirdPartyTransform(){
	return new Function<String, String>(){
            private ThirdPartyResource tpr = new ThirdPartyResource("This string was set.");
            
	    @Override
	    public String apply(String v) {            
                return tpr.thirdPartyMethod() + v;
            }
        };
    }
}
