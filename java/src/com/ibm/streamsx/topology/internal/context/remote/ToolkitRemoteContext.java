/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015, 2016 
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.core.InternalProperties.TOOLKITS_JSON;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.Future;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.internal.file.FileUtilities;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.toolkit.info.DependenciesType;
import com.ibm.streamsx.topology.internal.toolkit.info.DescriptionType;
import com.ibm.streamsx.topology.internal.toolkit.info.IdentityType;
import com.ibm.streamsx.topology.internal.toolkit.info.ObjectFactory;
import com.ibm.streamsx.topology.internal.toolkit.info.ToolkitDependencyType;
import com.ibm.streamsx.topology.internal.toolkit.info.ToolkitInfoModelType;

public class ToolkitRemoteContext implements RemoteContext<File> {

    @Override
    public Type getType() {
        return Type.TOOLKIT;
    }

    @Override
    public Future<File> submit(JsonObject submission) throws Exception {
        
        JsonObject deployInfo = object(submission, SUBMISSION_DEPLOY);
        if (deployInfo == null)
            submission.add(SUBMISSION_DEPLOY, deployInfo = new JsonObject());
                
        if (!deployInfo.has(ContextProperties.TOOLKIT_DIR)) {
            deployInfo.addProperty(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }

        final File toolkitRoot = new File(jstring(deployInfo, ContextProperties.TOOLKIT_DIR));

        JsonObject jsonGraph = object(submission, SUBMISSION_GRAPH);

        makeDirectoryStructure(toolkitRoot, jstring(jsonGraph, "namespace"));
        
        addToolkitInfo(toolkitRoot, jsonGraph);
        
        copyIncludes(toolkitRoot, jsonGraph);
        
        generateSPL(toolkitRoot, jsonGraph);

        return new CompletedFuture<File>(toolkitRoot);
    } 

    private void generateSPL(File toolkitRoot, JsonObject jsonGraph)
            throws IOException {

        // Create the SPL file, and save a copy of the JSON file.
        SPLGenerator generator = new SPLGenerator();
        createNamespaceFile(toolkitRoot, jsonGraph, "spl", generator.generateSPL(jsonGraph));
        createNamespaceFile(toolkitRoot, jsonGraph, "json", jsonGraph.toString());
    }
    
    private void createNamespaceFile(File toolkitRoot, JsonObject json, String suffix, String content)
            throws IOException {

        String namespace = jstring(json, "namespace");
        String name = jstring(json, "name");
        
        Path f = Paths.get(toolkitRoot.getAbsolutePath(), namespace, name + "." + suffix);

        try (PrintWriter splFile = new PrintWriter(f.toFile(), UTF_8.name())) {
            splFile.print(content);
            splFile.flush();
        }
    }

    public static void makeDirectoryStructure(File toolkitRoot, String namespace)
            throws Exception {

        File tkNamespace = new File(toolkitRoot, namespace);
        File tkImplLib = new File(toolkitRoot, Paths.get("impl", "lib").toString());
        File tkEtc = new File(toolkitRoot, "etc");
        File tkOpt = new File(toolkitRoot, "opt");

        tkImplLib.mkdirs();
        tkNamespace.mkdirs();
        tkEtc.mkdir();
        tkOpt.mkdir();
    }
    
    /**
     * Create an info.xml file for the toolkit.
     * @throws URISyntaxException 
     */
    private void addToolkitInfo(File toolkitRoot, JsonObject jsonGraph) throws JAXBException, FileNotFoundException, IOException, URISyntaxException  {
        File infoFile = new File(toolkitRoot, "info.xml");
        
        ToolkitInfoModelType info = new ToolkitInfoModelType();
        
        info.setIdentity(new IdentityType());
        info.getIdentity().setName(toolkitRoot.getName());
        info.getIdentity().setDescription(new DescriptionType());
        info.getIdentity().setVersion("1.0.0." + System.currentTimeMillis());
        info.getIdentity().setRequiredProductVersion("4.0.1.0");
              
        DependenciesType dependencies = new DependenciesType();
        
        List<ToolkitDependencyType> toolkits = dependencies.getToolkit();
        
        GsonUtilities.objectArray(object(jsonGraph, "spl"), TOOLKITS_JSON, tk -> {
            ToolkitDependencyType depTkInfo;
            String root = jstring(tk, "root");
            if (root != null) {
                try {
                    depTkInfo = TkInfo.getTookitDependency(root);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                depTkInfo = new ToolkitDependencyType();
                
                depTkInfo.setName(jstring(tk, "name"));
                depTkInfo.setVersion(jstring(tk, "version"));
            }
            toolkits.add(depTkInfo);
        });
        
        File topologyToolkitRoot = TkInfo.getTopologyToolkitRoot();
        toolkits.add(TkInfo.getTookitDependency(topologyToolkitRoot.getAbsolutePath()));
        
        info.setDependencies(dependencies);
        
        
        JAXBContext context = JAXBContext
                .newInstance(ObjectFactory.class);
        Marshaller m = context.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        try (FileOutputStream out = new FileOutputStream(infoFile)) {
            m.marshal(info, out);
        }
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
    private void copyIncludes(File toolkitRoot, JsonObject json) throws IOException {
        
        JsonObject config = object(json, "config");
                
        if (!config.has("includes"))
            return;
        
        JsonArray includes = array(config, "includes");
        
        for (int i = 0; i < includes.size(); i++) {
            JsonObject inc = includes.get(i).getAsJsonObject();
            
            String source = jstring(inc, "source");
            String target = jstring(inc, "target");
            
            File srcFile = new File(source);
            File targetDir = new File(toolkitRoot, target);
            if (!targetDir.exists())
                targetDir.mkdirs();
            if (srcFile.isFile())
                copyFile(srcFile, targetDir);
            else if (srcFile.isDirectory())
                copyDirectoryToDirectory(srcFile, targetDir);
        };
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
    
    public static boolean deleteToolkit(File appDir, JsonObject deployConfig) throws IOException {       
        if (jboolean(deployConfig, KEEP_ARTIFACTS)) {
            return false;
        }
        
        FileUtilities.deleteDirectory(appDir);
        return true;
    }
}
