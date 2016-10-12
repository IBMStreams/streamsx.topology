package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.Future;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.toolkit.info.DependenciesType;
import com.ibm.streamsx.topology.internal.toolkit.info.DescriptionType;
import com.ibm.streamsx.topology.internal.toolkit.info.IdentityType;
import com.ibm.streamsx.topology.internal.toolkit.info.ObjectFactory;
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
        
        System.err.println("DEPLOY_ENTRY: " + deployInfo);
        
        if (!deployInfo.has(ContextProperties.TOOLKIT_DIR)) {
            deployInfo.addProperty(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }

        final File toolkitRoot = new File(jstring(deployInfo, ContextProperties.TOOLKIT_DIR));

        System.err.println("ROOT: " + toolkitRoot);
        System.err.println("DEPLOY: " + deployInfo);
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
        String name = jstring(json, "name");;

        File f = new File(toolkitRoot,
                namespace + "/" + name + "." + suffix);
        PrintWriter splFile = new PrintWriter(f, "UTF-8");
        splFile.print(content);
        splFile.flush();
        splFile.close();
    }

    public static void makeDirectoryStructure(File toolkitRoot, String namespace)
            throws Exception {

        File tkNamespace = new File(toolkitRoot, namespace);
        File tkImplLib = new File(toolkitRoot, "impl/lib");
        File tkEtc = new File(toolkitRoot, "etc");
        File tkOpt = new File(toolkitRoot, "opt");

        tkImplLib.mkdirs();
        tkNamespace.mkdirs();
        tkEtc.mkdir();
        tkOpt.mkdir();
        
        System.err.println("TOOLKIT_IMPL" + tkImplLib);
        System.err.println("TOOLKIT_IMPL" + tkImplLib.exists());
    }
    
    /**
     * Create an info.xml file for the toolkit.
     */
    private void addToolkitInfo(File toolkitRoot, JsonObject jsonGraph) throws JAXBException, FileNotFoundException, IOException  {
        File infoFile = new File(toolkitRoot, "info.xml");
        
        ToolkitInfoModelType info = new ToolkitInfoModelType();
        
        info.setIdentity(new IdentityType());
        info.getIdentity().setName(toolkitRoot.getName());
        info.getIdentity().setDescription(new DescriptionType());
        info.getIdentity().setVersion("1.0.0." + System.currentTimeMillis());
        info.getIdentity().setRequiredProductVersion("4.0.1.0");
        
        info.setDependencies(new DependenciesType());
        
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
        
        System.out.println("INCLUDES:" + config.get("includes"));
        
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
}
