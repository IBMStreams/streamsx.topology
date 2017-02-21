/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

public class ZippedToolkitRemoteContext extends ToolkitRemoteContext {
    @Override
    public Type getType() {
        return Type.BUILD_ARCHIVE;
    }
    
    @Override
    public Future<File> submit(JsonObject submission) throws Exception {
        preSubmit(submission);
        File toolkitRoot = super.submit(submission).get();
        return postSubmit(submission, createCodeArchive(toolkitRoot, submission));        
    }
    
    public static Future<File> createCodeArchive(File toolkitRoot, JsonObject submission) throws IOException, URISyntaxException {
        
        JsonObject jsonGraph = object(submission, SUBMISSION_GRAPH);
        String namespace = jstring(jsonGraph, "namespace");
        String name = jstring(jsonGraph, "name");
        String tkName = toolkitRoot.getName();
        
        Path zipOutPath = pack(toolkitRoot.toPath(), namespace, name, tkName);
        
        JsonObject results = new JsonObject();
        results.addProperty("archivePath", zipOutPath.toString());
        submission.add(RemoteContext.SUBMISSION_RESULTS, results);
        
        JsonObject deployInfo = object(submission, SUBMISSION_DEPLOY);
        deleteToolkit(toolkitRoot, deployInfo);
        
        return new CompletedFuture<File>(zipOutPath.toFile());
    }
        
    private static Path pack(final Path folder, String namespace, String name, String tkName) throws IOException, URISyntaxException {
        Path zipFilePath = Paths.get(folder.toAbsolutePath().toString() + ".zip");
        String workingDir = zipFilePath.getParent().toString();
        
        Path topologyToolkit = TkInfo.getTopologyToolkitRoot().getAbsoluteFile().toPath();  
        
        // tkManifest is the list of toolkits contained in the archive
        try (PrintWriter tkManifest = new PrintWriter("manifest_tk.txt", "UTF-8")) {
            tkManifest.println(tkName);
            tkManifest.println("com.ibm.streamsx.topology");
        }
        
        // mainComposite is a string of the namespace and the main composite.
        // This is used by the Makefile
        try (PrintWriter mainComposite = new PrintWriter("main_composite.txt", "UTF-8")) {
            mainComposite.print(namespace + "::" + name);
        }
               
        Path manifest = Paths.get(workingDir, "manifest_tk.txt");
        Path mainComp = Paths.get(workingDir, "main_composite.txt");
        Path makefile = topologyToolkit.resolve(Paths.get("opt", "python", "templates", "common", "Makefile.template"));
        
        Map<Path, String> paths = new HashMap<>();
        paths.put(topologyToolkit, topologyToolkit.getFileName().toString());
        paths.put(manifest, "manifest_tk.txt");
        paths.put(mainComp, "main_composite.txt");
        paths.put(makefile, "Makefile");
        paths.put(folder, folder.getFileName().toString());
        
        addAllToZippedArchive(paths, zipFilePath);  
        manifest.toFile().delete();
        mainComp.toFile().delete();
        
        return zipFilePath;
    }
    
    
    private static void addAllToZippedArchive(Map<Path, String> starts, Path zipFilePath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(zipFilePath.toFile());
                ZipOutputStream zos = new ZipOutputStream(fos)) {
            for (Path start : starts.keySet()) {
                final String rootEntryName = starts.get(start);
                Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        // Skip pyc files.
                        if (file.getFileName().toString().endsWith(".pyc"))
                            return FileVisitResult.CONTINUE;
                        
                        String entryName = rootEntryName;
                        String relativePath = start.relativize(file).toString();
                        // If empty, file is the start file.
                        if(!relativePath.isEmpty()){                          
                            entryName = entryName + "/" + relativePath;
                        }
                        // Zip uses forward slashes
                        entryName = entryName.replace(File.separatorChar, '/');
                        zos.putNextEntry(new ZipEntry(entryName));
                        Files.copy(file, zos);
                        zos.closeEntry();
                        return FileVisitResult.CONTINUE;
                    }

                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        final String dirName = dir.getFileName().toString();
                        // Don't include pyc files or .toolkit 
                        if (dirName.equals("__pycache__"))
                            return FileVisitResult.SKIP_SUBTREE;
                                                
                        zos.putNextEntry(new ZipEntry(rootEntryName + "/" + start.relativize(dir).toString().replace(File.separatorChar, '/') + "/"));
                        zos.closeEntry();
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        }
    }
}
