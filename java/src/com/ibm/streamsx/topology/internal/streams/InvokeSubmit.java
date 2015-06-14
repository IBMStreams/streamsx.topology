/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;

public class InvokeSubmit {

    static final Logger trace = Topology.STREAMS_LOGGER;

    private final File bundle;

    public InvokeSubmit(File bundle) {
        super();
        this.bundle = bundle;
    }

    public BigInteger invoke() throws Exception, InterruptedException {
        String si = System.getenv("STREAMS_INSTALL");
        File sj = new File(si, "bin/streamtool");
        
        File jobidFile = Files.createTempFile("streamsjobid", "txt").toFile();

        List<String> commands = new ArrayList<String>();

        commands.add(sj.getAbsolutePath());
        commands.add("submitjob");
        commands.add("--outfile");
        commands.add(jobidFile.getAbsolutePath());
        commands.add(bundle.getAbsolutePath());

        trace.info("Invoking streamtool submitjob " + bundle.getAbsolutePath());

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
}
