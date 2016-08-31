package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

public class ZippedToolkitStreamsContext extends ToolkitStreamsContext {
	
	@Override
	public Future<File> submit(Topology app, Map<String, Object> config) throws Exception {

        if (config == null)
            config = new HashMap<>();

        // If the toolkit path is not given, then create one in the
        // currrent directory.
        if (!config.containsKey(ContextProperties.TOOLKIT_DIR)) {
            config.put(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }
        
        File toolkitRoot = super.submit(app, config).get();
        Path zipOutPath = Paths.get(toolkitRoot.getAbsolutePath() + ".zip");
		pack(toolkitRoot.toPath(), zipOutPath);
        return new CompletedFuture<File>(zipOutPath.toFile());
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

        File toolkitRoot = super.submit(submission).get();
        Path zipOutPath = Paths.get(toolkitRoot.getAbsolutePath() + ".zip");
		pack(toolkitRoot.toPath(), zipOutPath);
        return new CompletedFuture<File>(zipOutPath.toFile());
	}
	
	public static void pack(final Path folder, final Path zipFilePath) throws IOException {
		String folderName = folder.getFileName().toString();
	    try (
	            FileOutputStream fos = new FileOutputStream(zipFilePath.toFile());
	            ZipOutputStream zos = new ZipOutputStream(fos)
	    ) {
	        Files.walkFileTree(folder, new SimpleFileVisitor<Path>() {
	            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
	                zos.putNextEntry(new ZipEntry(folderName + "/" + folder.relativize(file).toString()));
	                Files.copy(file, zos);
	                zos.closeEntry();
	                return FileVisitResult.CONTINUE;
	            }

	            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
	                zos.putNextEntry(new ZipEntry(folderName + "/" + folder.relativize(dir).toString() + "/"));
	                zos.closeEntry();
	                return FileVisitResult.CONTINUE;
	            }
	        });
	    }
	}

}
