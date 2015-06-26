/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.json.java.OrderedJSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperator;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.functional.ops.Functional;
import com.ibm.streamsx.topology.internal.logic.WrapperFunction;

/**
 * The DependencyResolver class exists to separate the logic of jar
 * resolution/copying from the topology.
 * 
 */
public class DependencyResolver {
    private final Map<BOperatorInvocation, Set<Path>> operatorToJarDependencies = new HashMap<>();
    private final Set<Path> globalDependencies = new HashSet<>();

    /**
     * Ensure we don't copy files multiple times and keep
     * a map between a code source and the jar we generate.
     */
    private final Map<Path,String> previouslyCopiedDependencies = new HashMap<>();

    private Topology parentTopology;

    public DependencyResolver(Topology parentTopology) {
        this.parentTopology = parentTopology;
    }
    
    @SuppressWarnings("unused")
    public void addJarDependency(String location) throws IllegalArgumentException{
        File f = new File(location);
        if(null == f){
            throw new IllegalArgumentException("File not found. Invalid "
                    + "third party dependency location.");
        }
        globalDependencies.add(f.toPath().toAbsolutePath());    
    }
    
    public void addClassDependency(Class<?> clazz){
        CodeSource source = clazz.getProtectionDomain().getCodeSource();
        Path absolutePath=null;
        try {
            absolutePath = Paths.get(source.getLocation().toURI()).toAbsolutePath();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        globalDependencies.add(absolutePath);
    }

    public void addJarDependency(BOperatorInvocation op, Object logic) {

        while (logic instanceof WrapperFunction) {
            addJarDependency(op, logic.getClass());
            logic = ((WrapperFunction) logic).getWrappedFunction();

        }
        addJarDependency(op, logic.getClass());
    }

    public void addJarDependency(BOperatorInvocation op, Class<?> clazz) {

        CodeSource thisCodeSource = this.getClass().getProtectionDomain()
                .getCodeSource();

        CodeSource source = clazz.getProtectionDomain().getCodeSource();
        if (null == source || thisCodeSource.equals(source)) {
            return;
        }
        
        Path absolutePath = null;
        try {
            absolutePath = Paths.get(source.getLocation().toURI()).toAbsolutePath();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        
        if (operatorToJarDependencies.containsKey(op)) {
            operatorToJarDependencies.get(op).add(absolutePath);
        } else {
            operatorToJarDependencies.put(op, new HashSet<Path>());
            operatorToJarDependencies.get(op).add(absolutePath);
        }
    }

    // Resolve the dependencies. Copies jars to the impl/lib part of the bundle
    public void resolveDependencies(Map<String, Object> config)
            throws IOException, URISyntaxException {
        for (BOperatorInvocation op : operatorToJarDependencies.keySet()) {    
            ArrayList<String> jars = new ArrayList<String>();
            
            for (Path pa : operatorToJarDependencies.get(op)) {
                String jarName = resolveDependency(pa, config);
                jars.add("impl/lib/" + jarName);
            }

            String[] jarPaths = jars.toArray(new String[jars.size()]);
            op.setParameter("jar", jarPaths);
        }
        
        ArrayList<String> jars = new ArrayList<String>();
        for(Path dep : globalDependencies){
            if(previouslyCopiedDependencies.containsKey(dep)){
                continue;
            }
            String jarName = resolveDependency(dep, config);
            jars.add("impl/lib/" + jarName);
        }
        
        List<BOperator> ops = parentTopology.builder().getOps();
        if(jars.size() != 0){
            for (BOperator op : ops) {
                if (op instanceof BOperatorInvocation) {
                    BOperatorInvocation bop = (BOperatorInvocation) op;
                    if (Functional.class.isAssignableFrom(bop.op()
                            .getOperatorClass())) {
                        JSONObject params = (JSONObject) bop.json().get(
                                "parameters");
                        JSONObject op_jars = (JSONObject) params.get("jar");
                        if (null == op_jars) {
                            JSONObject val = new OrderedJSONObject();
                            val.put("value", new JSONArray());
                            params.put("jar", val);
                            op_jars = val;
                        }
                        JSONArray value = (JSONArray) op_jars.get("value");
                        for (String jar : jars) {
                            value.add(jar);
                        }
                    }
                }
            }
        }
    }
    
    private String resolveDependency(Path pa, Map<String, Object> config){ 
        final File toolkitRoot = new File((String) (config
                .get(ContextProperties.TOOLKIT_DIR)));
        final File toolkitLib = new File(toolkitRoot, "impl/lib/");
        
        String jarName=null;
        
        if (!previouslyCopiedDependencies.containsKey(pa)) {
            Path absolutePath = pa;
            File absoluteFile = absolutePath.toFile();
                    
            // If it's a file, we assume its a jar file.
            if (absoluteFile.isFile()) {
                jarName = absolutePath.getFileName().toString();
                try {
                    Files.copy(absolutePath, new File(toolkitLib, jarName).toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } 
            
            else if (absoluteFile.isDirectory()) {
                try {
                    jarName = createJarFile(toolkitLib, absoluteFile);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            
            else {
                throw new IllegalArgumentException("Path not a file or directory:" + pa);
            }
            previouslyCopiedDependencies.put(pa, jarName);
        } 
        
        else {
            jarName = previouslyCopiedDependencies.get(pa);
        }
        
        // Sanity check
        if(null == jarName){
            throw new IllegalStateException("Error resolving dependency "+ pa);
        }
        return jarName;
    }
    
    /**
     * Create a jar file from the classes directory, creating it directly in
     * the toolkit.
     */
    private static String createJarFile(File toolkitLib, final File classesDir) throws IOException {
        
        final Path classesPath = classesDir.toPath();
        Path jarPath = Files.createTempFile(toolkitLib.toPath(), "classes", ".jar");
        try (final JarOutputStream jarOut =
                new JarOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(jarPath.toFile()), 128*1024))) {
        
        Files.walkFileTree(classesDir.toPath(), new FileVisitor<Path>() {

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

}
