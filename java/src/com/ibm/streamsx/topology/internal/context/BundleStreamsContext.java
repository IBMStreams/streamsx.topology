/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.core.InternalProperties;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeSc;

public class BundleStreamsContext extends ToolkitStreamsContext {

    static final Logger trace = Topology.TOPOLOGY_LOGGER;

    private final boolean standalone;

    public BundleStreamsContext(boolean standalone) {
        this.standalone = standalone;
    }

    @Override
    public Type getType() {
        return standalone ? Type.STANDALONE_BUNDLE : Type.BUNDLE;
    }

    @Override
    public Future<File> submit(Topology app, Map<String, Object> config)
            throws Exception {

        if (config == null)
            config = new HashMap<>();

        File appDir;
        if (!config.containsKey(ContextProperties.APP_DIR)) {
            appDir = Files.createTempDirectory("apptk").toFile();
            config.put(ContextProperties.APP_DIR, appDir.getAbsolutePath());

        } else {
            appDir = new File((String) (config.get(ContextProperties.APP_DIR)));
        }
        config.put(ContextProperties.TOOLKIT_DIR, appDir.getAbsolutePath());

        super.submit(app, config);
        
        JSONObject jsonApp = app.builder().complete();

        String namespace = (String) jsonApp.get("namespace");
        String name = (String) jsonApp.get("name");

        InvokeSc sc = new InvokeSc(config, standalone, namespace, name, appDir);
        
        // Add the toolkits
        JSONObject graphConfig  = (JSONObject) jsonApp.get("config");
        if (graphConfig != null) {
            JSONObject splConfig = (JSONObject) graphConfig.get("spl");
            if (splConfig != null) {
                JSONArray toolkits = (JSONArray) splConfig.get(InternalProperties.TK_DIRS_JSON);
                if (toolkits != null) {
                    for (Object tkdir : toolkits) {
                        sc.addToolkit(new File(tkdir.toString()));
                    }
                }
            }
        }

        sc.invoke();

        File outputDir = new File(appDir, "output");
        String bundleName = namespace + "." + name + ".sab";
        File bundle = new File(outputDir, bundleName);

        File localBundle = new File(bundle.getName());

        Files.copy(bundle.toPath(), localBundle.toPath(),
                StandardCopyOption.REPLACE_EXISTING);

        deleteToolkit(appDir, config);

        // app.splgraph().setBundle(localBundle);
        config.put(ContextProperties.BUNDLE, localBundle);

        trace.info("Streams Application Bundle produced: "
                + localBundle.getName());

        return new CompletedFuture<File>(localBundle);
    }

    private void deleteToolkit(File appDir, Map<String,Object> config) throws IOException {
        Path tkdir = appDir.toPath();
        
        Boolean keep = (Boolean) config.get(ContextProperties.KEEP_ARTIFACTS);
        if (Boolean.TRUE.equals(keep)) {
            trace.info("Keeping toolkit at: " + tkdir.toString());
            return;
        }

        Files.walkFileTree(tkdir, new FileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir,
                    BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file,
                    BasicFileAttributes attrs) throws IOException {
                file.toFile().delete();
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                    throws IOException {
                dir.toFile().delete();
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
