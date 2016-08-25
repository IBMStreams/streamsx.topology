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
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.streams.InvokeStandalone;

public class StandaloneStreamsContext extends BundleUserStreamsContext<Integer> {
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
    public StandaloneStreamsContext() {
        super(true);
    }

    @Override
    public Type getType() {
        return Type.STANDALONE;
    }

    /**
     * Build the SPL bundle, execute as a standalone application and remove the
     * bundle. Note, when this returns the application likely will still be
     * running.
     */
    @Override
    public Future<Integer> submit(Topology app, Map<String, Object> config)
            throws Exception {

        File bundle = bundler.submit(app, config).get();

        InvokeStandalone invokeStandalone = new InvokeStandalone(bundle);

        preInvoke();
        Future<Integer> future = invokeStandalone.invoke(config);

        // bundle.delete();
        return future;
    }

    void preInvoke() {
    }
    
    @Override
    public Future<Integer> submit(JSONObject json) throws Exception {

    	File bundle = bundler.submit(json).get();
        InvokeStandalone invokeStandalone = new InvokeStandalone(bundle);

        preInvoke();
        Map<String, Object> config = Collections.emptyMap();
        Future<Integer> future = invokeStandalone.invoke(config);

        return future;
    }
}
