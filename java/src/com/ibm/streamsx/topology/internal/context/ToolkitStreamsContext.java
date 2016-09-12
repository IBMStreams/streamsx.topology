/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeMakeToolkit;

public class ToolkitStreamsContext extends StreamsContextImpl<File> {

	static final Logger trace = Topology.TOPOLOGY_LOGGER;
	
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
    public Future<File> submit(JSONObject submission) throws Exception {
    	
    	JSONObject deployInfo = (JSONObject) submission.get(SUBMISSION_DEPLOY);
    	if (deployInfo == null)
    		submission.put("deploy", deployInfo = new JSONObject());
    	
        if (!deployInfo.containsKey(ContextProperties.TOOLKIT_DIR)) {
        	deployInfo.put(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }

        final File toolkitRoot = new File((String) deployInfo.get(ContextProperties.TOOLKIT_DIR));
        
        JSONObject jsonGraph = (JSONObject) submission.get(SUBMISSION_GRAPH);

        makeDirectoryStructure(toolkitRoot,
        		jsonGraph.get("namespace").toString());

        Future<File> future = createToolkitFromGraph(toolkitRoot, jsonGraph);

        // process python version information and update toolkit file with the information
        JSONObject jsonPythonVersion = (JSONObject) deployInfo.get(SUBMISSION_PYTHONVERSION);
        if (jsonPythonVersion != null) {

          String pythonVersion = new String(jsonPythonVersion.get("version").toString());
          JSONArray jsonPythonBinaries = (JSONArray) jsonPythonVersion.get("binaries");
          String pygetpythonconfigInfo = new String("echo " + pythonVersion + " ");
          for (Object inco : jsonPythonBinaries) {
            JSONObject inc = (JSONObject) inco;
    		
            String pythonBinary = inc.get("python").toString();
            String pythonConfigBinary = inc.get("pythonconfig").toString();
            pygetpythonconfigInfo += pythonBinary + " " + pythonConfigBinary; 
          }
          Files.write(Paths.get(toolkitRoot+"/opt/python/templates/common/pygetpythonconfig.sh"), pygetpythonconfigInfo.getBytes(), StandardOpenOption.APPEND);
        }
        
        // Invoke spl-make-toolkit
        InvokeMakeToolkit imt = new InvokeMakeToolkit(deployInfo, toolkitRoot);
        imt.invoke();
        
        return future;
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
                        targetPath.resolve(sourcePath.relativize(file)),
                        StandardCopyOption.REPLACE_EXISTING);
                return FileVisitResult.CONTINUE;
            }
        });
    }
    
    public void deleteToolkit(File appDir, JSONObject deployConfig) throws IOException {
        Path tkdir = appDir.toPath();
        
        Boolean keep = (Boolean) deployConfig.get(KEEP_ARTIFACTS);
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
