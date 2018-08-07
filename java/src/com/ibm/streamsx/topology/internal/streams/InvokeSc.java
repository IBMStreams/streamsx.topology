/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;
import com.ibm.streamsx.topology.internal.messages.Messages;

public class InvokeSc {

    static final Logger trace = Util.STREAMS_LOGGER;

    private Set<File> toolkits = new HashSet<>();
    private final boolean standalone;
    private final String namespace;
    private final String mainComposite;
    private final File applicationDir;
    private final String installDir;

    public InvokeSc(JsonObject deploy, boolean standalone, String namespace, String mainComposite,
            File applicationDir) throws URISyntaxException, IOException {
        super();
       
        this.namespace = namespace;
        this.mainComposite = mainComposite;
        this.applicationDir = applicationDir;
        
        installDir = Util.getStreamsInstall(deploy, ContextProperties.COMPILE_INSTALL_DIR);
        
        // Version 4.2 onwards deprecates standalone compiler option
        // so don't use it to avoid warnings.
        if (Util.getStreamsInstall().equals(installDir)) {
            if (Util.versionAtLeast(4, 2, 0))
                standalone = false;
        } else {
            // TODO: get version of compile install to be used
        }
        this.standalone = standalone;
        
        addFunctionalToolkit();
    }

    public void addToolkit(File toolkitDir) throws IOException {
        toolkits.add(toolkitDir.getCanonicalFile());
    }

    private void addFunctionalToolkit() throws URISyntaxException, IOException {
        URL location = getClass().getProtectionDomain().getCodeSource()
                .getLocation();

        // Assumption it is at lib in the toolkit.
        
        // Allow overriding to support test debug in eclipse,
        // where the tkroot location relationship to this code isn't as
        // isn't as expected during normal execution.
        String tkRootPath = System.getProperty(
                "com.ibm.streamsx.topology.invokeSc.functionalTkRoot");
        if (tkRootPath!=null) {
            File tkRoot = new File(tkRootPath);
            addToolkit(tkRoot);
            return;
        }

        Path functionaljar = Paths.get(location.toURI());
        File tkRoot = functionaljar.getParent().getParent().toFile();
        addToolkit(tkRoot);
    }

    public void invoke() throws Exception, InterruptedException {
        File sc = new File(installDir, "bin/sc");

        List<String> commands = new ArrayList<>();

        String mainCompositeName = namespace + "::" + mainComposite;

        commands.add(sc.getAbsolutePath());
        commands.add("--rebuild-toolkits");
        commands.add("--optimized-code-generation");
        if (standalone)
            commands.add("--standalone");
        
        String tnt = System.getenv("TOPOLOGY_NUM_MAKE_THREADS");
        if (tnt != null && !tnt.isEmpty()) {
            try {
                int count = Integer.valueOf(tnt.trim());
                if (count >= 1)
                    commands.add("--num-make-threads="+Integer.toString(count));
            } catch (NumberFormatException nfe) {}
        }

        commands.add("-M");
        commands.add(mainCompositeName);

        commands.add("-t");
        commands.add(getToolkitPath());

        trace.info("Invoking SPL compiler (sc) for main composite: "
                + mainCompositeName);
        trace.info(Util.concatenate(commands));

        ProcessBuilder pb = new ProcessBuilder(commands);
        
        // Force the SPL application to use the Java provided by
        // Streams to ensure the bundle is not dependent on the
        // local JVM install path.
        pb.environment().remove("JAVA_HOME");
        
        // Set STREAMS_INSTALL in case it was overriden for compilation
        pb.environment().put("STREAMS_INSTALL", installDir);
        
        // Ensure than only the toolkit path set by -t is used.
        pb.environment().remove("STREAMS_SPLPATH");
        
        pb.directory(applicationDir);

        Process scProcess = pb.start();
        ProcessOutputToLogger.log(trace, scProcess);
        scProcess.getOutputStream().close();
        int rc = scProcess.waitFor();
        trace.info("SPL compiler complete: return code=" + rc);
        if (rc != 0)
            throw new Exception(Messages.getString("STREAMS_COMPILATION_FAILED"));
    }

    private String getToolkitPath() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (File tk : toolkits) {
            if (!first)
                sb.append(":");
            else
                first = false;

            sb.append(tk.getAbsolutePath());
        }

        String splPath = System.getenv("STREAMS_SPLPATH");
        if (splPath != null) {
            sb.append(":");
            sb.append(splPath);
        }

        String streamsInstallToolkits = installDir + "/toolkits";
        if (sb.indexOf(streamsInstallToolkits) == -1) {
            sb.append(":");
            sb.append(streamsInstallToolkits);
        }

        trace.info("ToolkitPath:" + sb.toString());
        return sb.toString();
    }

}
