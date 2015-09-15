/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;

public class InvokeSubmit {

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

        List<String> commands = new ArrayList<String>();

        commands.add(sj.getAbsolutePath());
        commands.add("submitjob");
        commands.add("--outfile");
        commands.add(jobidFile.getAbsolutePath());
        if (config.containsKey(ContextProperties.TRACING_LEVEL)) {
            Level level = (Level) Util.getConfigEntry(config, 
                                ContextProperties.TRACING_LEVEL,
                                Level.class);
            commands.add("--config");
            commands.add("tracing="+toTracingLevel(level));
        }
        if (config.containsKey(JobProperties.NAME)) {
            String name = (String) Util.getConfigEntry(config, 
                                    JobProperties.NAME, String.class);
            commands.add("--jobname");
            commands.add(name);
        }
        if (config.containsKey(JobProperties.GROUP)) {
            String group = (String) Util.getConfigEntry(config, 
                                    JobProperties.GROUP, String.class);
            commands.add("--jobgroup");
            commands.add(group);
        }
        if (config.containsKey(JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION)) {
            Boolean override = (Boolean) Util.getConfigEntry(config, 
                                JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION,
                                Boolean.class);
            if (override) {
                commands.add("--override");
                commands.add("HostLoadProtection");
            }
        }
        if (config.containsKey(JobProperties.PRELOAD_APPLICATION_BUNDLES)) {
            Boolean value = (Boolean) Util.getConfigEntry(config,
                                JobProperties.PRELOAD_APPLICATION_BUNDLES,
                                Boolean.class);
            commands.add("--config");
            commands.add("preloadApplicationBundles="+value);
        }
        if (config.containsKey(JobProperties.DATA_DIRECTORY)) {
            String value = (String) Util.getConfigEntry(config,
                                JobProperties.DATA_DIRECTORY,
                                String.class);
            commands.add("--config");
            commands.add("data-directory="+value);
        }
        if (config.containsKey(ContextProperties.SUBMISSION_PARAMS)) {
            @SuppressWarnings("unchecked")
            Map<String,Object> params = (Map<String,Object>) config.get(ContextProperties.SUBMISSION_PARAMS); 
            for(Map.Entry<String,Object> e :  params.entrySet()) {
                // note: this "streamtool" execution path does NOT correctly
                // handle / preserve the semantics of escaped \t and \n.
                // e.g., "\\n" is treated as a newline 
                // rather than the two char '\','n'
                // This seems to be happening internal to streamtool.
                // Adjust accordingly.
                commands.add("-P");
                commands.add(e.getKey()+"="+e.getValue().toString()
                                                .replace("\\", "\\\\\\"));
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
        }
    }
    
    public static String toTracingLevel(Level level) {
        int tli = level.intValue();
        String tls;
        if (tli == Level.OFF.intValue())
            tls = "off";
        else if (tli == Level.ALL.intValue())
            tls = "debug";
        else if (tli >= TraceLevel.ERROR.intValue())
            tls = "error";
        else if (tli >= TraceLevel.WARN.intValue())
            tls = "warn";
        else if (tli >= TraceLevel.INFO.intValue())
            tls = "info";
        else if (tli >= TraceLevel.DEBUG.intValue())
            tls = "debug";
        else if (tli >= TraceLevel.TRACE.intValue())
            tls = "trace";
        else
            tls = "trace";
        return tls;
    }
}
