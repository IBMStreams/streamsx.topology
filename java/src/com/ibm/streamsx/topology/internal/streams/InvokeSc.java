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
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;

public class InvokeSc {

    static final Logger trace = Topology.STREAMS_LOGGER;

    private Set<File> toolkits = new HashSet<File>();
    private final boolean standalone;
    private final String namespace;
    private final String mainComposite;
    private final File applicationDir;
    private final String installDir;

    public InvokeSc(Map<String,Object> config, boolean standalone, String namespace, String mainComposite,
            File applicationDir) throws URISyntaxException, IOException {
        super();
        this.standalone = standalone;
        this.namespace = namespace;
        this.mainComposite = mainComposite;
        this.applicationDir = applicationDir;
        
        installDir = Util.getStreamsInstall(config, ContextProperties.COMPILE_INSTALL_DIR);

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

        List<String> commands = new ArrayList<String>();

        String mainCompositeName = namespace + "::" + mainComposite;

        commands.add(sc.getAbsolutePath());
        commands.add("--optimized-code-generation");
        commands.add("--num-make-threads=4");
        if (standalone)
            commands.add("--standalone");

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
            throw new Exception("SPL compilation failed!");
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
        sb.append(":");
        sb.append(streamsInstallToolkits);

        trace.info("ToolkitPath:" + sb.toString());
        return sb.toString();
    }

}
