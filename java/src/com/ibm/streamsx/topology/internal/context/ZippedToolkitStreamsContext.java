package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;

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
        File toolkitRoot = super.submit(app, config).get();
        Path zipOutPath = pack(toolkitRoot.toPath());
        if (!(config.containsKey(KEEP_ARTIFACTS) && (boolean)config.get(KEEP_ARTIFACTS)))
        	delete(toolkitRoot.toPath());
        return new CompletedFuture<File>(zipOutPath.toFile());
	}
	
	@Override
	public Future<File> submit(JSONObject submission) throws Exception {
        File toolkitRoot = super.submit(submission).get();
        Path zipOutPath = pack(toolkitRoot.toPath());
        
        JSONObject deployInfo = (JSONObject)  submission.get(SUBMISSION_DEPLOY);
        if (!(deployInfo.containsKey(KEEP_ARTIFACTS) && (boolean)deployInfo.get(KEEP_ARTIFACTS)))
        	delete(toolkitRoot.toPath());
        return new CompletedFuture<File>(zipOutPath.toFile());
	}
	
	// Deletes a file or directory, even if not empty. Returns false if
	// it could not be deleted.
	public static boolean delete(final Path folder) {
		try {
			Files.walkFileTree(folder, new SimpleFileVisitor<Path>(){
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
			        return FileVisitResult.CONTINUE;
			    }

				@Override
			    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			    	Files.delete(dir);
			        return FileVisitResult.CONTINUE;
			    }
			});
		} catch (IOException e) {
			return false;
		}
		return true;
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

}
