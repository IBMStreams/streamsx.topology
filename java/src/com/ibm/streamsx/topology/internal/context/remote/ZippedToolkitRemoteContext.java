/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CONFIG;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.graph;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppName;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppNamespace;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectArray;

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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

public class ZippedToolkitRemoteContext extends ToolkitRemoteContext {
    
	private final boolean keepBuildArchive;

	public ZippedToolkitRemoteContext() {
        this.keepBuildArchive = false;
    }
	
    public ZippedToolkitRemoteContext(boolean keepBuildArchive) {
        this.keepBuildArchive = keepBuildArchive;
    }
	
	@Override
    public Type getType() {
        return Type.BUILD_ARCHIVE;
    }
    
    @Override
    public Future<File> _submit(JsonObject submission) throws Exception {
        final File toolkitRoot = super._submit(submission).get();
        return createCodeArchive(toolkitRoot, submission);
    }
    
    public Future<File> createCodeArchive(File toolkitRoot, JsonObject submission) throws IOException, URISyntaxException {
        
        String tkName = toolkitRoot.getName();
        
        Path zipOutPath = pack(toolkitRoot.toPath(), graph(submission), tkName);
        
        if (keepBuildArchive || keepArtifacts(submission)) {
        	final JsonObject submissionResult = GsonUtilities.objectCreate(submission, RemoteContext.SUBMISSION_RESULTS);
        	submissionResult.addProperty(SubmissionResultsKeys.ARCHIVE_PATH, zipOutPath.toString());
        }
        
        JsonObject deployInfo = object(submission, SUBMISSION_DEPLOY);
        deleteToolkit(toolkitRoot, deployInfo);
        
        return new CompletedFuture<File>(zipOutPath.toFile());
    }
        
    private static Path pack(final Path folder, JsonObject graph, String tkName) throws IOException, URISyntaxException {
        String namespace = splAppNamespace(graph);
        String name = splAppName(graph);

        Path zipFilePath = Paths.get(folder.toAbsolutePath().toString() + ".zip");
        String workingDir = zipFilePath.getParent().toString();
        
        Path topologyToolkit = TkInfo.getTopologyToolkitRoot().getAbsoluteFile().toPath();  
        
        // Paths to copy into the toolkit
        Map<Path, String> paths = new HashMap<>();
        
        // tkManifest is the list of toolkits contained in the archive
        try (PrintWriter tkManifest = new PrintWriter("manifest_tk.txt", "UTF-8")) {
            tkManifest.println(tkName);
            tkManifest.println("com.ibm.streamsx.topology");
            
            JsonObject configSpl = object(graph, CONFIG, "spl");
            if (configSpl != null) {
                objectArray(configSpl, "toolkits",
                        tk -> {
                            File tkRoot = new File(jstring(tk, "root"));
                            String tkRootName = tkRoot.getName();
                            tkManifest.println(tkRootName);
                            paths.put(tkRoot.toPath(), tkRootName);
                            }
                        );
            }
        }
        
        // mainComposite is a string of the namespace and the main composite.
        // This is used by the Makefile
        try (PrintWriter mainComposite = new PrintWriter("main_composite.txt", "UTF-8")) {
            mainComposite.print(namespace + "::" + name);
        }
               
        Path manifest = Paths.get(workingDir, "manifest_tk.txt");
        Path mainComp = Paths.get(workingDir, "main_composite.txt");
        Path makefile = topologyToolkit.resolve(Paths.get("opt", "python", "templates", "common", "Makefile.template"));
        
        
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
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(zipFilePath.toFile())) {
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
                        
                        ZipArchiveEntry entry = new ZipArchiveEntry(file.toFile(), entryName);
                        if (Files.isExecutable(file))
                            entry.setUnixMode(0100770);
                        else
                            entry.setUnixMode(0100660);

                        zos.putArchiveEntry(entry);
                        Files.copy(file, zos);
                        zos.closeArchiveEntry();
                        return FileVisitResult.CONTINUE;
                    }

                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        final String dirName = dir.getFileName().toString();
                        // Don't include pyc files or .toolkit 
                        if (dirName.equals("__pycache__"))
                            return FileVisitResult.SKIP_SUBTREE;
                                                
                        ZipArchiveEntry dirEntry = new ZipArchiveEntry(dir.toFile(), rootEntryName + "/" + start.relativize(dir).toString().replace(File.separatorChar, '/') + "/");
                        zos.putArchiveEntry(dirEntry);
                        zos.closeArchiveEntry();
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        }
    }
}
