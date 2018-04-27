/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
 */
package com.ibm.streamsx.topology.internal.streams;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;
import com.ibm.streamsx.topology.internal.messages.Messages;

public class InvokeMakeToolkit {

    static final Logger trace = Util.STREAMS_LOGGER;

    private final File toolkitDir;
    private final String installDir;

    public InvokeMakeToolkit(JsonObject deploy, File toolkitDir) throws URISyntaxException, IOException {
        super();
        this.toolkitDir = toolkitDir;
        
        installDir = Util.getStreamsInstall(deploy, ContextProperties.COMPILE_INSTALL_DIR);
    }

    public void invoke() throws Exception, InterruptedException {
        File mtk = new File(installDir, "bin/spl-make-toolkit");

        List<String> commands = new ArrayList<>();

        commands.add(mtk.getAbsolutePath());
        commands.add("--make-operator");
        commands.add("-i");

        commands.add(toolkitDir.getAbsolutePath());

        trace.info("Invoking spl-make-toolkit");
        trace.info(Util.concatenate(commands));

        ProcessBuilder pb = new ProcessBuilder(commands);
        
        // Force the SPL application to use the Java provided by
        // Streams to ensure the bundle is not dependent on the
        // local JVM install path.
        pb.environment().remove("JAVA_HOME");
        
        // Set STREAMS_INSTALL in case it was overriden for compilation
        pb.environment().put("STREAMS_INSTALL", installDir);
               
        Process mtProcess = pb.start();
        ProcessOutputToLogger.log(trace, mtProcess);
        mtProcess.getOutputStream().close();
        int rc = mtProcess.waitFor();
        trace.info("spl-make-toolkit complete: return code=" + rc);
        if (rc != 0)
            throw new Exception(Messages.getString("STREAMS_SPL_MAKE_TOOLKIT_FAILED"));
    }
}
