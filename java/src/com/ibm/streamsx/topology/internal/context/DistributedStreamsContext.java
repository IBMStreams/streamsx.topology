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
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeSubmit;

public class DistributedStreamsContext extends
        BundleUserStreamsContext<BigInteger> {
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

    public DistributedStreamsContext() {
        super(false);
    }

    @Override
    public Type getType() {
        return Type.DISTRIBUTED;
    }

    @Override
    public Future<BigInteger> submit(Topology app, Map<String, Object> config)
            throws Exception {

        preBundle();
        File bundle = bundler.submit(app, config).get();
        
        return submitBundle(bundle, config);
    }
    
    private Future<BigInteger> submitBundle(File bundle, Map<String, Object> config) throws InterruptedException, Exception {
        preInvoke();
        InvokeSubmit submitjob = new InvokeSubmit(bundle);

        BigInteger jobId = submitjob.invoke(config);
        
        return new CompletedFuture<BigInteger>(jobId);
    }
    
    void preInvoke() {
    }
    
    void preBundle() {
        // fail early if invoke preconditions aren't met
        InvokeSubmit.checkPreconditions();
    }
    
    /**
     * Submit directly from a JSON representation of a topology.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Future<BigInteger> submit(JSONObject json) throws Exception {

    	File bundle = bundler.submit(json).get();
    	
    	Map<String, Object> config = Collections.emptyMap();
    	if (json.containsKey(SUBMISSION_DEPLOY)) {
            JSONObject deploy = (JSONObject) json.get(SUBMISSION_DEPLOY);
            if (!deploy.isEmpty()) {
                config = deploy;
            }
    	}
        
        return submitBundle(bundle, config);
    }
}
