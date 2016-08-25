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
# Copyright IBM Corp. 2016 
 */
package com.ibm.streamsx.topology.internal.streams;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.jobconfig.JobConfig;

/**
 * Utility code to generate a Job Config Overlay,
 * supported in IBM Streams 4.2 & later.
 *
 */
public class JobConfigOverlay {
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
    
    private final JobConfig jobConfig;
    
    public JobConfigOverlay(JobConfig jobConfig) {  
        this.jobConfig = jobConfig;
    }
    
    
    public String fullOverlay() {
        Gson gson = new Gson();
             
        JsonObject overlay = new JsonObject();
        
        // JobConfig
        JsonObject jsonJobConfig = gson.toJsonTree(jobConfig).getAsJsonObject();
        if (!jsonJobConfig.entrySet().isEmpty()) {             
             overlay.add("jobConfig", jsonJobConfig);
        }
        
        // DeploymentConfig
        JsonObject deploy = new JsonObject();
        deploy.addProperty("fusionScheme", "legacy");
        overlay.add("deploymentConfig", deploy);
        
        // Create the top-level structure.
        JsonObject fullJco = new JsonObject();
        JsonArray jcos = new JsonArray();
        jcos.add(overlay);
        fullJco.add("jobConfigOverlays", jcos);
               
        return gson.toJson(fullJco);
    }
}
