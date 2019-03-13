/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015, 2016 
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.context.ContextProperties.VMARGS;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.DEPLOYMENT_CONFIG;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.JOB_CONFIG_OVERLAYS;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.core.InternalProperties.TOOLKITS_JSON;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_STREAMS_VERSION;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppName;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppNamespace;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.addAll;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.internal.file.FileUtilities;
import com.ibm.streamsx.topology.internal.graph.GraphKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.toolkit.info.DependenciesType;
import com.ibm.streamsx.topology.internal.toolkit.info.DescriptionType;
import com.ibm.streamsx.topology.internal.toolkit.info.IdentityType;
import com.ibm.streamsx.topology.internal.toolkit.info.ObjectFactory;
import com.ibm.streamsx.topology.internal.toolkit.info.ToolkitDependencyType;
import com.ibm.streamsx.topology.internal.toolkit.info.ToolkitInfoModelType;

public class ToolkitRemoteContext extends RemoteContextImpl<File> {
    
    protected static final Logger TRACE = Logger.getLogger("com.ibm.streamsx.topology");
    
    /**
     * Location where dependent jars are placed
     * that do not include primitive operators.
     */
    public static final String DEP_JAR_LOC = "opt" + File.separator + "streamsx.topology.depends";
    
    /**
     * Location where dependent jars are placed
     * that do not include primitive operators.
     */
    public static final String DEP_OP_JAR_LOC = "impl" + File.separator + "lib";


	private final boolean keepToolkit;

	public ToolkitRemoteContext() {
        this.keepToolkit = false;
    }
	
    public ToolkitRemoteContext(boolean keepToolkit) {
        this.keepToolkit = keepToolkit;
    }
	
    @Override
    public Type getType() {
        return Type.TOOLKIT;
    }

    @Override
    public Future<File> _submit(JsonObject submission) throws Exception {        
        report("Generating code");
        JsonObject deploy = deploy(submission);
        
        addSelectDeployToGraphConfig(submission);
        
        // If no version has been supplied use 4.2 as the
        // Streaming Analytics service is at a minimum 4.2 
        JsonObject graphConfig = GraphKeys.graphConfig(submission);
        if (!graphConfig.has(CFG_STREAMS_VERSION)) {
            graphConfig.addProperty(CFG_STREAMS_VERSION, "4.2");
        }
                      
        if (!deploy.has(ContextProperties.TOOLKIT_DIR)) {
            Path tkDir = keepToolkit ?
                Files.createTempDirectory(Paths.get(""), "tk")
                :
                Files.createTempDirectory("tk");
            
            deploy.addProperty(ContextProperties.TOOLKIT_DIR,
                tkDir.toAbsolutePath().toString());
        }

        final File toolkitRoot = new File(jstring(deploy, ContextProperties.TOOLKIT_DIR));

        JsonObject jsonGraph = object(submission, SUBMISSION_GRAPH);

        makeDirectoryStructure(toolkitRoot, splAppNamespace(jsonGraph));
        
        addToolkitInfo(toolkitRoot, jsonGraph);
        
        copyIncludes(toolkitRoot, jsonGraph);
        
        generateSPL(toolkitRoot, jsonGraph);
        
        if (keepToolkit || keepArtifacts(submission)) {
        	final JsonObject submissionResult = GsonUtilities.objectCreate(submission, RemoteContext.SUBMISSION_RESULTS);
        	submissionResult.addProperty(SubmissionResultsKeys.TOOLKIT_ROOT, toolkitRoot.getAbsolutePath());
        }
        
        setupJobConfigOverlays(deploy, jsonGraph);

        return new CompletedFuture<File>(toolkitRoot);
    } 

    /**
     * Create a Job Config Overlays structure if it does not exist.
     * Set the deployment from the graph config.
     */
    private void setupJobConfigOverlays(JsonObject deploy, JsonObject graph) {
        JsonArray jcos = array(deploy, JOB_CONFIG_OVERLAYS);
        if (jcos == null) {
            deploy.add(JOB_CONFIG_OVERLAYS, jcos = new JsonArray());
            jcos.add(new JsonObject());
        }
        JsonObject jco = jcos.get(0).getAsJsonObject();
        
        JsonObject graphDeployment = GsonUtilities.object(graph, "config", DEPLOYMENT_CONFIG);
        
        if (!jco.has(DEPLOYMENT_CONFIG)) {
             jco.add(DEPLOYMENT_CONFIG, graphDeployment);
             return;
        }
        
        JsonObject deployment = object(jco, DEPLOYMENT_CONFIG);
        
        // Need to merge with the graph taking precedence.
        addAll(deployment, graphDeployment);
        
        if ("legacy".equals(GsonUtilities.jstring(deployment, "fusionScheme "))) {
            if (deployment.has("fusionTargetPeCount"))
                deployment.remove("fusionTargetPeCount");
        }
    }

    private void generateSPL(File toolkitRoot, JsonObject jsonGraph)
            throws IOException {

        // Save a copy of the input JSON file.
        createNamespaceFile(toolkitRoot, jsonGraph, "json", jsonGraph.toString());
        
        // Create the SPL file
        SPLGenerator generator = new SPLGenerator();        
        createNamespaceFile(toolkitRoot, jsonGraph, "spl", generator.generateSPL(jsonGraph));      
    }
    
    private void createNamespaceFile(File toolkitRoot, JsonObject json, String suffix, String content)
            throws IOException {

        String namespace = splAppNamespace(json);
        String name = splAppName(json);
        
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
        File tkOptDepends = new File(toolkitRoot, DEP_JAR_LOC);

        tkImplLib.mkdirs();
        tkNamespace.mkdirs();
        tkEtc.mkdir();
        tkOptDepends.mkdirs();
    }
    
    /**
     * Create an info.xml file for the toolkit.
     * @throws URISyntaxException 
     */
    private void addToolkitInfo(File toolkitRoot, JsonObject jsonGraph) throws JAXBException, FileNotFoundException, IOException, URISyntaxException  {
        File infoFile = new File(toolkitRoot, "info.xml");
        
        ToolkitInfoModelType info = new ToolkitInfoModelType();
        
        info.setIdentity(new IdentityType());
        info.getIdentity().setName(GraphKeys.splAppNamespace(jsonGraph) + "." + GraphKeys.splAppName(jsonGraph));
        info.getIdentity().setDescription(new DescriptionType());
        info.getIdentity().setVersion("1.0.0." + System.currentTimeMillis());
        info.getIdentity().setRequiredProductVersion(jstring(object(jsonGraph, "config"), CFG_STREAMS_VERSION));
              
        DependenciesType dependencies = new DependenciesType();
        
        List<ToolkitDependencyType> toolkits = dependencies.getToolkit();
        
        GsonUtilities.objectArray(object(jsonGraph, "config", "spl"), TOOLKITS_JSON, tk -> {
            ToolkitDependencyType depTkInfo;
            String root = jstring(tk, "root");
            if (root != null) {
                try {
                    depTkInfo = TkInfo.getTookitDependency(root, false);
                    // Handle missing info.xml
                    if (depTkInfo == null)
                        return;
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
        toolkits.add(TkInfo.getTookitDependency(topologyToolkitRoot.getAbsolutePath(), true));
        
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
    private void copyIncludes(File toolkitRoot, JsonObject graph) throws IOException {
        
        JsonObject config = object(graph, "config");
                
        if (!config.has("includes"))
            return;
        
        JsonArray includes = array(config, "includes");
        
        for (int i = 0; i < includes.size(); i++) {
            JsonObject inc = includes.get(i).getAsJsonObject();
            
            String target = jstring(inc, "target");
            File targetDir = new File(toolkitRoot, target);
            if (!targetDir.exists())
                targetDir.mkdirs();
            
            // Simple copy of a file or directory
            if (inc.has("source")) {
                String source = jstring(inc, "source");
                File srcFile = new File(source);
                if (srcFile.isFile())
                    copyFile(srcFile, targetDir);
                else if (srcFile.isDirectory())
                    copyDirectoryToDirectory(toolkitRoot, srcFile, targetDir);
            }
            // Create a jar from a classes directory.
            else if (inc.has("classes")) {
                String classes = jstring(inc, "classes");
                String name = jstring(inc, "name");
                createJarFile(classes, name, targetDir);
            }
            // Create a file from the contents in the file.
            else if (inc.has("contents")) {
                byte[] contents = jstring(inc, "contents").getBytes(StandardCharsets.UTF_8);
                Path targetFile = new File(targetDir, jstring(inc, "name")).toPath();
                Files.write(targetFile, contents);
            }
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
    private static void copyDirectoryToDirectory(File toolkitRoot, File srcDir, File dstDir)
            throws IOException {
        String dirname = srcDir.getName();
        dstDir = new File(dstDir, dirname);
        copyDirectory(toolkitRoot, srcDir, dstDir);
    }

    /**
     * Copy srcDir's children, recursively, to dstDir.  dstDir is created
     * if necessary.  Any existing children in dstDir are overwritten.
     * @param srcDir
     * @param dstDir
     */
    private static void copyDirectory(final File toolkitRoot, File srcDir, final File dstDir) throws IOException {
        final Path targetPath = dstDir.toPath();
        final Path sourcePath = srcDir.toPath();
        final File canonicalTkRoot = toolkitRoot.getCanonicalFile();
        Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir,
                    final BasicFileAttributes attrs) throws IOException {
                
                // Avoid recursion.
                if (canonicalTkRoot.equals(dir.toFile().getCanonicalFile()))
                    return FileVisitResult.SKIP_SUBTREE;
                    
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
    
    /**
     * Create a jar file from a classes directory,
     * creating it directly in the toolkit.
     */
    private static String createJarFile(String classes, String name, File toolkitLib) throws IOException {
        assert name.endsWith(".jar");
        
        final Path classesPath = Paths.get(classes);
        final Path jarPath = new File(toolkitLib, name).toPath();
        try (final JarOutputStream jarOut =
                new JarOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(jarPath.toFile()), 128*1024))) {
        
        Files.walkFileTree(classesPath, new FileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir,
                    BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file,
                    BasicFileAttributes attrs) throws IOException {
                File classFile = file.toFile();
                if (classFile.isFile()) {
                    //  Write the entry followed by the data.
                    Path relativePath = classesPath.relativize(file);
                    JarEntry je = new JarEntry(relativePath.toString());
                    je.setTime(classFile.lastModified());
                    jarOut.putNextEntry(je);
                    
                    final byte[] data = new byte[32*1024];
                    try (final BufferedInputStream classIn =
                            new BufferedInputStream(
                                    new FileInputStream(classFile), data.length)) {
                        
                        for (;;) {
                            int count = classIn.read(data);
                            if (count == -1)
                                break;
                            jarOut.write(data, 0, count);
                        }
                    }
                    jarOut.closeEntry();
                }
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
        
        return jarPath.getFileName().toString();
    }
    
    public static boolean deleteToolkit(File appDir, JsonObject deployConfig) throws IOException {       
        if (jboolean(deployConfig, KEEP_ARTIFACTS)) {
            return false;
        }
        
        FileUtilities.deleteDirectory(appDir);
        return true;
    }
    
    /**
     * Deploy keys that also needed in the graph configuration
     * for code generation.
     */
    private static final Set<String> GRAPH_CONFIG_KEYS = new HashSet<>();
    static {
        
        // ContextProperties
        Collections.addAll(GRAPH_CONFIG_KEYS, VMARGS);
    }
    
    private void addSelectDeployToGraphConfig(JsonObject submission) {
        
        JsonObject deploy = DeployKeys.deploy(submission);
        JsonObject graph = object(submission, SUBMISSION_GRAPH);
        JsonObject graphConfig = object(graph, "config");
        if (graphConfig == null)
            graph.add("config", graphConfig = new JsonObject());
        
        for (String key : GRAPH_CONFIG_KEYS) {
            if (deploy.has(key))
                graphConfig.add(key, deploy.get(key));
        }
    }


}
