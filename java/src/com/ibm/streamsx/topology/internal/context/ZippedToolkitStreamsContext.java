package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
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
        
        String namespace = (String) app.builder().json().get("namespace");
        String name = (String) app.builder().json().get("name");
        String tkName = toolkitRoot.toPath().toAbsolutePath().getFileName().toString();
        
        Path zipOutPath = pack(toolkitRoot.toPath(), namespace, name, tkName);
        
        JSONObject jso = new JSONObject();
        addConfigToJSON(jso, config);  
        
        return deleteToolkitAndProduceCompletedFuture(toolkitRoot, jso, zipOutPath);
	}
	
	@Override
	public Future<File> submit(JSONObject submission) throws Exception {
        File toolkitRoot = super.submit(submission).get();
        
        JSONObject jsonGraph = (JSONObject) submission.get(SUBMISSION_GRAPH);
        String namespace = jsonGraph.get("namespace").toString();
        String name = jsonGraph.get("name").toString();
        String tkName = toolkitRoot.toPath().toAbsolutePath().getFileName().toString();
        
        Path zipOutPath = pack(toolkitRoot.toPath(), namespace, name, tkName);
        
        return deleteToolkitAndProduceCompletedFuture(toolkitRoot, (JSONObject)submission.get(SUBMISSION_DEPLOY), zipOutPath);
	}
	
	// Addresses 
	private CompletedFuture<File> deleteToolkitAndProduceCompletedFuture(File toolkitRoot, JSONObject deploy, Path zipOutPath) throws IOException{
		deleteToolkit(toolkitRoot, deploy);
        return new CompletedFuture<File>(zipOutPath.toFile());
	}
	
	public Path pack(final Path folder, String namespace, String name, String tkName) throws IOException, URISyntaxException {
		Path zipFilePath = Paths.get(folder.toAbsolutePath().toString() + ".zip");
		String workingDir = zipFilePath.getParent().toString();
		// com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar
		File jarLocation = new File(ZippedToolkitStreamsContext.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
		// com.ibm.streamsx.topology/lib/
		File libDir = new File(jarLocation.getParent());
		// com.ibm.streamsx.topology
		File topologyToolkit = new File(libDir.getParent());	
		
		// tkManifest is the list of toolkits contained in the archive
		PrintWriter tkManifest = new PrintWriter("manifest_tk.txt", "UTF-8");
		
		// mainComposite is a string of the namespace and the main composite.
		// This is used by the Makefile
		PrintWriter mainComposite = new PrintWriter("main_composite.txt", "UTF-8");
		
		tkManifest.println(tkName);
		tkManifest.println("com.ibm.streamsx.topology");
		
		mainComposite.print(namespace + "::" + name);
		
		tkManifest.close();
		mainComposite.close();
		
		List<Path> paths = new ArrayList<Path>();
		paths.add(Paths.get(topologyToolkit.getAbsolutePath()));
		paths.add(Paths.get(workingDir + "/manifest_tk.txt"));
		paths.add(Paths.get(workingDir + "/main_composite.txt"));
		paths.add(Paths.get(topologyToolkit.getAbsolutePath() + "/opt/python/templates/common/Makefile"));
		paths.add(folder);
		
		addAllToZippedArchive(paths, zipFilePath);	
		JSONObject jso = new OrderedJSONObject();
		super.deleteToolkit(new File(paths.get(1).toString()), jso);
		super.deleteToolkit(new File(paths.get(2).toString()), jso);
		
	    return zipFilePath;
	}
	
	protected static void addAllToZippedArchive(List<Path> starts, Path zipFilePath) throws IOException {
		try (FileOutputStream fos = new FileOutputStream(zipFilePath.toFile());
				ZipOutputStream zos = new ZipOutputStream(fos)) {
			for (Path start : starts) {
				String startName = start.getFileName().toString();
				Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						String entryName = startName;
						String relativePath = start.relativize(file).toString();
						if(relativePath.length() != 0){
							entryName = entryName + "/" + relativePath;
						}
						zos.putNextEntry(new ZipEntry(entryName));
						Files.copy(file, zos);
						zos.closeEntry();
						return FileVisitResult.CONTINUE;
					}

					public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
						zos.putNextEntry(new ZipEntry(startName + "/" + start.relativize(dir).toString() + "/"));
						zos.closeEntry();
						return FileVisitResult.CONTINUE;
					}
				});
			}
		}
	}
	
	
	@Override
    protected void makeToolkit(JSONObject deployInfo, File toolkitRoot) throws InterruptedException, Exception{
        // Do nothing
    }

}
