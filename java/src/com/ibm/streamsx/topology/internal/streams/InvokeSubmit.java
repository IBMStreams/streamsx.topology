/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015, 2016 
 */
package com.ibm.streamsx.topology.internal.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.copyJobConfigOverlays;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.jobconfig.SubmissionParameter;
import com.ibm.streamsx.topology.internal.messages.Messages;

public class InvokeSubmit {

    static final Logger trace = Util.STREAMS_LOGGER;

    private final File bundle;

    public InvokeSubmit(File bundle) {
        super();
        this.bundle = bundle;
    }
    
    public static void checkPreconditions() throws IllegalStateException {
        Util.checkInvokeStreamtoolPreconditions();
    }

    public BigInteger invoke(JsonObject deploy) throws Exception, InterruptedException {
        String si = Util.getStreamsInstall();
        File sj = new File(si, "bin/streamtool");
        
        checkPreconditions();
        
        File jobidFile = Files.createTempFile("streamsjobid", "txt").toFile();

        List<String> commands = new ArrayList<>();

        commands.add(sj.getAbsolutePath());
        commands.add("submitjob");
        String user = System.getenv(Util.STREAMS_USERNAME);
        if (user != null) {
            commands.add("--User");
            commands.add(user);
        }
        commands.add("--outfile");
        commands.add(jobidFile.getAbsolutePath());
                
        final JobConfig jobConfig = JobConfigOverlay.fromFullOverlay(deploy);
        
        // For IBM Streams 4.2 or later use the job config overlay
        // V.R.M.F
        File jcoFile = null;
        if (Util.versionAtLeast(4, 2, 0)) {
            jcoFile = fileJobConfig(commands, deploy);
        } else {         
            explicitJobConfig(commands, jobConfig);
        }
        
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
                throw new Exception(Messages.getString("STREAMS_STREAMTOOL_SUBMITJOB_FAILED"));
            
            try (Scanner jobIdScanner = new Scanner(jobidFile)) {
                if (!jobIdScanner.hasNextBigInteger())
                    throw new Exception(Messages.getString("STREAMS_STREAMTOOL_FAILED_TO_SUPPLY"));
                
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
     * Set the job configuration as a job config overlay
     * Used for 4.2 and later.
     */
    private File fileJobConfig(List<String> commands, final JsonObject deploy) throws IOException {
        
        JsonObject jobConfigInfo = copyJobConfigOverlays(deploy);

        String jcoJson = GsonUtilities.toJson(jobConfigInfo);
                     
        File jcoFile = File.createTempFile("streamsjco", ".json");
        Files.write(jcoFile.toPath(), jcoJson.getBytes(StandardCharsets.UTF_8));
        
        trace.info("Job Config Overlays: " + jcoJson);
        
        commands.add("--jobConfig");
        commands.add(jcoFile.getAbsolutePath());
        
        return jcoFile;      
    }
}
