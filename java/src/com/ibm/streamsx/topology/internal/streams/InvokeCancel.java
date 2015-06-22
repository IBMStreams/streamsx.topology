/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;

public class InvokeCancel {

    static final Logger trace = Topology.STREAMS_LOGGER;

    private final BigInteger jobId;

    public InvokeCancel(BigInteger jobId) {
        super();
        this.jobId = jobId;
    }

    public void invoke() throws Exception, InterruptedException {
        String si = Util.getStreamsInstall();
        File sj = new File(si, "bin/streamtool");
        
        Util.checkInvokeStreamtoolPreconditions();

        List<String> commands = new ArrayList<String>();

        commands.add(sj.getAbsolutePath());
        commands.add("canceljob");
        commands.add(jobId.toString());

        trace.info("Invoking streamtool canceljob " + jobId);

        ProcessBuilder pb = new ProcessBuilder(commands);

        Process sjProcess = pb.start();
        ProcessOutputToLogger.log(trace, sjProcess);
        sjProcess.getOutputStream().close();
        int rc = sjProcess.waitFor();
        trace.info("streamtool canceljob complete: return code=" + rc);
        if (rc != 0)
            throw new Exception("streamtool canceljob failed!");

    }
}
