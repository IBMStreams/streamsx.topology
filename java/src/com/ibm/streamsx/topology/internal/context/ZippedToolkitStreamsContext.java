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
import java.util.Map;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeMakeToolkit;

public class ZippedToolkitStreamsContext extends ToolkitStreamsContext {
	
    @Override
    public Type getType() {
        return Type.ZIPPED_TOOLKIT;
    }
	
	@Override
	public Future<File> submit(Topology app, Map<String, Object> config) throws Exception {        
        File toolkitRoot = super.submit(app, config).get();
        Path zipOutPath = pack(toolkitRoot.toPath());
        
        JSONObject jso = new JSONObject();
        addConfigToJSON(jso, config);  
        
        return deleteToolkitAndProduceCompletedFuture(toolkitRoot, jso, zipOutPath);
	}
	
	@Override
	public Future<File> submit(JSONObject submission) throws Exception {
        File toolkitRoot = super.submit(submission).get();
        Path zipOutPath = pack(toolkitRoot.toPath());
        
        return deleteToolkitAndProduceCompletedFuture(toolkitRoot, (JSONObject)submission.get(SUBMISSION_DEPLOY), zipOutPath);
	}
	
	// Addresses 
	private CompletedFuture<File> deleteToolkitAndProduceCompletedFuture(File toolkitRoot, JSONObject deploy, Path zipOutPath) throws IOException{
		deleteToolkit(toolkitRoot, deploy);
        return new CompletedFuture<File>(zipOutPath.toFile());
	}
	
	public static Path pack(final Path folder) throws IOException {
		Path zipFilePath = Paths.get(folder.toAbsolutePath().toString() + ".zip");
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
	    return zipFilePath;
	}
	
	@Override
    protected void makeToolkit(JSONObject deployInfo, File toolkitRoot) throws InterruptedException, Exception{
        // Do nothing
    }

}
