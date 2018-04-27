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

import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;
import com.ibm.streamsx.topology.internal.messages.Messages;

public class InvokeCancel {

    static final Logger trace = Util.STREAMS_LOGGER;

    private final String domainId;
    private final String instanceId;
    private final BigInteger jobId;
    private final String userName;

    public InvokeCancel(String domainId, String instanceId, BigInteger jobId, String userName) {
        super();
        this.domainId = domainId;
        this.instanceId = instanceId;
        this.jobId = jobId;
        this.userName = userName;

    }
    public InvokeCancel(BigInteger jobId, String userName) {
        this(null, null, jobId, userName);
    }
    public InvokeCancel(BigInteger jobId) {
        this(jobId, null);
    }

    public int invoke(boolean throwOnError) throws Exception, InterruptedException {
        String si = Util.getStreamsInstall();
        File sj = new File(si, "bin/streamtool");
        
        if (domainId == null)
             Util.checkInvokeStreamtoolPreconditions();

        List<String> commands = new ArrayList<>();

        commands.add(sj.getAbsolutePath());
        commands.add("canceljob");
        if (domainId != null)
        {
          commands.add("--domain-id");
          commands.add(domainId);
        }
        if (instanceId != null)
        {
          commands.add("--instance-id");
          commands.add(instanceId);
        }
        if (null != userName)
        {
          commands.add("-U");
          commands.add(userName);
        }

        commands.add(jobId.toString());

        trace.info("Invoking streamtool canceljob " + jobId);

        ProcessBuilder pb = new ProcessBuilder(commands);

        Process sjProcess = pb.start();
        ProcessOutputToLogger.log(trace, sjProcess);
        sjProcess.getOutputStream().close();
        int rc = sjProcess.waitFor();
        trace.info("streamtool canceljob complete: return code=" + rc);
        if (throwOnError && rc != 0)
            throw new Exception(Messages.getString("STREAMS_STREAMTOOL_CANCELJOB_FAILED"));
        return rc;
    }
}
