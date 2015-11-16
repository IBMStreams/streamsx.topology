/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

public class ToolkitStreamsContext extends StreamsContextImpl<File> {

    Map<String, Object> graphItems;

    @Override
    public Type getType() {
        return Type.TOOLKIT;
    }

    @Override
    public Future<File> submit(Topology app, Map<String, Object> config)
            throws Exception {

        if (config == null)
            config = new HashMap<>();

        // If the toolkit path is not given, then create one in the
        // currrent directory.
        if (!config.containsKey(ContextProperties.TOOLKIT_DIR)) {
            config.put(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }

        File toolkitRoot = new File((String) config.get(ContextProperties.TOOLKIT_DIR));

        makeDirectoryStructure(toolkitRoot,
                (String) app.builder().json().get("namespace"));

        graphItems = app.finalizeGraph(getType(), config);
        
        addConfigToJSON(app.builder().getConfig(), config);
        
        JSONObject jsonGraph = app.builder().complete();
        
        return createToolkitFromGraph(toolkitRoot, jsonGraph);
    }
    
    private Future<File> createToolkitFromGraph(File toolkitRoot, JSONObject jsonGraph) throws IOException {
        copyIncludes(toolkitRoot, jsonGraph);
        generateSPL(toolkitRoot, jsonGraph);
        return new CompletedFuture<File>(toolkitRoot);
    }
    
    @Override
    public Future<File> submit(JSONObject json, Map<String, Object> config) throws Exception {
    	if (config == null)
    		config = new HashMap<>();
    	
        if (!config.containsKey(ContextProperties.TOOLKIT_DIR)) {
            config.put(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }

        final File toolkitRoot = new File((String) config.get(ContextProperties.TOOLKIT_DIR));

        makeDirectoryStructure(toolkitRoot,
                json.get("namespace").toString());

        return createToolkitFromGraph(toolkitRoot, json);
    }

    protected void addConfigToJSON(JSONObject graphConfig, Map<String,Object> config) {
        
        for (String key : config.keySet()) {
            Object value = config.get(key);
            
            if (key.equals(ContextProperties.SUBMISSION_PARAMS)) {
                // value causes issues below and no need to add this to json
                continue;
            }
            if (JSONObject.isValidObject(value)) {
                graphConfig.put(key, value);
                continue;
            }
            if (value instanceof Collection) {
                JSONArray ja = new JSONArray();
                @SuppressWarnings("unchecked")
                Collection<Object> coll = (Collection<Object>) value;
                ja.addAll(coll);
                graphConfig.put(key, ja);            
            }
        }
    }
    
    private static JSONObject getGraphConfig(JSONObject json) {
    	return (JSONObject) json.get("config");
    }

    private void generateSPL(File toolkitRoot, JSONObject jsonGraph)
            throws IOException {

        // Create the SPL file, and save a copy of the JSON file.
        SPLGenerator generator = new  SPLGenerator();
        createNamespaceFile(toolkitRoot, jsonGraph, "spl", generator.generateSPL(jsonGraph));
        createNamespaceFile(toolkitRoot, jsonGraph, "json", jsonGraph.serialize());
    }

    private void createNamespaceFile(File toolkitRoot, JSONObject json, String suffix, String content)
            throws IOException {

        String namespace = (String) json.get("namespace");
        String name = (String) json.get("name");

        File f = new File(toolkitRoot,
                namespace + "/" + name + "." + suffix);
        PrintWriter splFile = new PrintWriter(f, "UTF-8");
        // splFile.print(app.splgraph().toSPLString());
        splFile.print(content);
        splFile.flush();
        splFile.close();
    }

    private void makeDirectoryStructure(File toolkitRoot, String namespace)
            throws Exception {

        File tkNamespace = new File(toolkitRoot, namespace);
        File tkImplLib = new File(toolkitRoot, "impl/lib");
        File tkEtc = new File(toolkitRoot, "etc");
        File tkOpt = new File(toolkitRoot, "opt");

        tkImplLib.mkdirs();
        tkNamespace.mkdirs();
        tkEtc.mkdir();
        tkOpt.mkdir();
    }
    
    /**
     * Looks for "includes" in the graph config which will be
     * a list of JSON object representing files or directories to copy
     * into the toolkit, with source being the file or directory path
     * and target being the target directory relative to toolkitRoot.
     * @param toolkitRoot
     * @param json
     * @throws IOException
     * 
     * TODO add support for directories
     */
    private void copyIncludes(File toolkitRoot, JSONObject json) throws IOException {
    	
    	JSONObject config = getGraphConfig(json);
    	JSONArray includes = (JSONArray) config.get("includes");
    	if (includes == null || includes.isEmpty())
    		return;
    	
    	for (Object inco : includes) {
    		JSONObject inc = (JSONObject) inco;
    		
    		String source = inc.get("source").toString();
    		String target = inc.get("target").toString();
    		
    		File srcFile = new File(source);
    		File targetDir = new File(toolkitRoot, target);
    		if (!targetDir.exists())
    			targetDir.mkdirs();
    		if (srcFile.isFile())
    			copyFile(srcFile, targetDir);
    		else if (srcFile.isDirectory())
    			copyDirectoryToDirectory(srcFile, targetDir);
    	}
    }

    private static void copyFile(File srcFile, File targetDir) throws IOException {
        Files.copy(srcFile.toPath(), 
                new File(targetDir, srcFile.getName()).toPath(),
                StandardCopyOption.REPLACE_EXISTING);

    }
    
    /**
     * Copy srcDir tree to a directory of the same name in dstDir.
     * The destination directory is created if necessary.
     * @param srcDir
     * @param dstDir
     */
    private static void copyDirectoryToDirectory(File srcDir, File dstDir)
            throws IOException {
        String dirname = srcDir.getName();
        dstDir = new File(dstDir, dirname);
        copyDirectory(srcDir, dstDir);
    }

    /**
     * Copy srcDir's children, recursively, to dstDir.  dstDir is created
     * if necessary.  Any existing children in dstDir are overwritten.
     * @param srcDir
     * @param dstDir
     */
    private static void copyDirectory(File srcDir, File dstDir) throws IOException {
        final Path targetPath = dstDir.toPath();
        final Path sourcePath = srcDir.toPath();
        Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir,
                    final BasicFileAttributes attrs) throws IOException {
                Files.createDirectories(targetPath.resolve(sourcePath
                        .relativize(dir)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file,
                    final BasicFileAttributes attrs) throws IOException {
                Files.copy(file,
                        targetPath.resolve(sourcePath.relativize(file)));
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
