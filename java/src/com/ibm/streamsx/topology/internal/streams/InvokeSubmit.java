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
# Copyright IBM Corp. 2015, 2016 
 */
package com.ibm.streamsx.topology.internal.streams;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Logger;

import com.ibm.streams.operator.version.Product;
import com.ibm.streams.operator.version.Version;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.jobconfig.SubmissionParameter;

public class InvokeSubmit {
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

    static final Logger trace = Topology.STREAMS_LOGGER;

    private final File bundle;

    public InvokeSubmit(File bundle) {
        super();
        this.bundle = bundle;
    }
    
    public static void checkPreconditions() throws IllegalStateException {
        Util.checkInvokeStreamtoolPreconditions();
    }

    public BigInteger invoke() throws Exception, InterruptedException {
        Map<String,Object> config = Collections.emptyMap(); 
        return invoke(config);
    }

    public BigInteger invoke(Map<String, ? extends Object> config) throws Exception, InterruptedException {
        String si = Util.getStreamsInstall();
        File sj = new File(si, "bin/streamtool");
        
        checkPreconditions();
        
        File jobidFile = Files.createTempFile("streamsjobid", "txt").toFile();

        List<String> commands = new ArrayList<>();
        
        final JobConfig jobConfig = JobConfig.fromProperties(config);

        commands.add(sj.getAbsolutePath());
        commands.add("submitjob");
        commands.add("--outfile");
        commands.add(jobidFile.getAbsolutePath());
        
        // Fot IBM Streams 4.2 or later use the job config overlay
        // V.R.M.F
        File jcoFile = null;
        Version ver = Product.getVersion();
        if (ver.getVersion() > 4 ||
                (ver.getVersion() ==4 && ver.getRelease() >= 2))
            jcoFile = fileJobConfig(commands, jobConfig);
        else
            explicitJobConfig(commands, jobConfig);
        
        if (jobConfig.getOverrideResourceLoadProtection() != null) {
            
            if (jobConfig.getOverrideResourceLoadProtection()) {
                commands.add("--override");
                commands.add("HostLoadProtection");
            }
        }
        
        commands.add(bundle.getAbsolutePath());

        trace.info("Invoking streamtool submitjob " + bundle.getAbsolutePath());
        trace.info(Util.concatenate(commands));

        ProcessBuilder pb = new ProcessBuilder(commands);

        try {
            Process sjProcess = pb.start();
            ProcessOutputToLogger.log(trace, sjProcess);
            sjProcess.getOutputStream().close();
            int rc = sjProcess.waitFor();
            trace.info("streamtool submitjob complete: return code=" + rc);
            if (rc != 0)
                throw new Exception("streamtool submitjob failed!");
            
            try (Scanner jobIdScanner = new Scanner(jobidFile)) {
                if (!jobIdScanner.hasNextBigInteger())
                    throw new Exception("streamtool failed to supply a job identifier!");
                
                BigInteger jobId = jobIdScanner.nextBigInteger();
                trace.info("Bundle: " + bundle.getName() + " submitted with jobid: " + jobId);            
                return jobId;
            }
            
            
        } finally {
            jobidFile.delete();
            if (jcoFile != null)
                jcoFile.delete();
        }
    }

    /**
     * Set the job configuration as explicit streamtool submitjob arguments.
     * Used for 4.1 and older.
     */
    private void explicitJobConfig(List<String> commands, final JobConfig jobConfig) {
        if (jobConfig.getTracing() != null) {
            commands.add("--config");
            commands.add("tracing="+jobConfig.getStreamsTracing());
        }
        if (jobConfig.getJobName() != null) {
            commands.add("--jobname");
            commands.add(jobConfig.getJobName());
        }
        if (jobConfig.getJobGroup() != null) {
            commands.add("--jobgroup");
            commands.add(jobConfig.getJobGroup());
        }

        if (jobConfig.getPreloadApplicationBundles() != null) {
            commands.add("--config");
            commands.add("preloadApplicationBundles="+jobConfig.getPreloadApplicationBundles());
        }
        if (jobConfig.getDataDirectory() != null) {
            commands.add("--config");
            commands.add("data-directory="+jobConfig.getDataDirectory());
        }
        if (jobConfig.hasSubmissionParameters()) {
            for (SubmissionParameter param : jobConfig.getSubmissionParameters()) {
                 // note: this "streamtool" execution path does NOT correctly
                // handle / preserve the semantics of escaped \t and \n.
                // e.g., "\\n" is treated as a newline 
                // rather than the two char '\','n'
                // This seems to be happening internal to streamtool.
                // Adjust accordingly.
                commands.add("-P");
                commands.add(param.getName()+"="+param.getValue()
                                                .replace("\\", "\\\\\\"));
            }
        }
    }
    
    /**
     * Set the job configuration as explicit streamtool submitjob arguments.
     * Used for 4.1 and older.
     */
    private File fileJobConfig(List<String> commands, final JobConfig jobConfig) throws IOException {
        
        JobConfigOverlay jco = new JobConfigOverlay(jobConfig);

        String jcoJson = jco.fullOverlay();
        if (jcoJson == null)
            return null;
                     
        File jcoFile = File.createTempFile("streamsjco", ".json");
        Files.write(jcoFile.toPath(), jcoJson.getBytes(StandardCharsets.UTF_8));
        
        trace.info("JobConfig: " + jcoJson);
        
        commands.add("--jobConfig");
        commands.add(jcoFile.getAbsolutePath());
        
        return jcoFile;      
    }
}
