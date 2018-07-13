/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext.DEP_JAR_LOC;
import static com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext.DEP_OP_JAR_LOC;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.arrayCreate;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperator;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.internal.logic.WrapperFunction;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * The DependencyResolver class exists to separate the logic of jar
 * resolution/copying from the topology.
 * 
 */
public class DependencyResolver {    
    
    private final Map<BOperatorInvocation, Set<Path>> operatorToJarDependencies = new HashMap<>();
    /**
     * Map of jar paths that are dependencies.
     * Value is a boolean indicating if it includes a primitive
     * operator that needs to be part of the generated toolkit.
     */
    private final Map<Path, Boolean> globalDependencies = new HashMap<>();
    private final Set<Artifact> globalFileDependencies = new HashSet<>();

    private static class Artifact {
        final String dstDirName;
        final Path absPath;
        Artifact(String dirName, Path absPath) {
            if (dirName==null || absPath==null)
                throw new IllegalArgumentException(Messages.getString("CORE_DIR_NAME_OR_ABS_PATH_NULL"));
            this.dstDirName = dirName;
            this.absPath = absPath;
        }
        @Override
        public boolean equals(Object o) {
            if (o==this)
                return true;
            if (!(o instanceof Artifact))
                return false;
            Artifact o2 = (Artifact)o;
            return dstDirName.equals(o2.dstDirName) && absPath.equals(o2.absPath); 
        }
        @Override
        public int hashCode() {
            // no need to get fancy here
            return absPath.hashCode();
        }
    }
    
    /**
     * Ensure we don't copy files multiple times and keep
     * a map between a code source and the jar we generate.
     */
    private final Map<Path,String> previouslyCopiedDependencies = new HashMap<>();

    private Topology topology;

    public DependencyResolver(Topology parentTopology) {
        this.topology = parentTopology;
    }
    
    public void addJarDependency(String location) throws IllegalArgumentException{
        File f = new File(location);
        if(!f.exists()){
            throw new IllegalArgumentException(Messages.getString("CORE_THIRD_PARTY_DEP", f.toPath().toAbsolutePath().toString()));
        }
        
        addGlobalDependency(f.toPath().toAbsolutePath(), false); 
    }
    
    private void addGlobalDependency(Path path, boolean containsOps) {
        
        if (globalDependencies.containsKey(path)) {
            boolean jarContainsOps = globalDependencies.get(path);
            if ((jarContainsOps == containsOps) && !containsOps)
                return;
        }
        globalDependencies.put(path, containsOps);
    }
    
    public void addClassDependency(Class<?> clazz){
        
        CodeSource source = clazz.getProtectionDomain().getCodeSource();
        if (source == null)
            return;
        Path absolutePath=null;
        try {
            absolutePath = Paths.get(source.getLocation().toURI()).toAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        
        boolean containsOperator = false;
        try {
            @SuppressWarnings("unchecked")
            Class<? extends Annotation> primOpClass =
                    (Class<? extends Annotation>) Class.forName("com.ibm.streams.operator.model.PrimitiveOperator");
            containsOperator  = clazz.isAnnotationPresent(primOpClass);
        } catch (ClassNotFoundException e) {
            containsOperator = false;
        }
            
        addGlobalDependency(absolutePath, containsOperator);
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
            throw new RuntimeException(e);
        }
        
        if (operatorToJarDependencies.containsKey(op)) {
            operatorToJarDependencies.get(op).add(absolutePath);
        } else {
            operatorToJarDependencies.put(op, new HashSet<Path>());
            operatorToJarDependencies.get(op).add(absolutePath);
        }
    }

    /**
     * Copy dependencies from one operator to another.
     */
    public void copyDependencies(BOperatorInvocation source, BOperatorInvocation op) {
        if (operatorToJarDependencies.containsKey(source)) {
            Set<Path> sourceDeps = operatorToJarDependencies.get(source);
            if (!operatorToJarDependencies.containsKey(op))
               operatorToJarDependencies.put(op, new HashSet<>());
                            
            operatorToJarDependencies.get(op).addAll(sourceDeps);
        }
    }
    
    /**
     * Add a file dependency {@code location} to be 
     * added to directory {@code dstDirName} in the bundle.
     * @param location path to a file or directory
     * @param dstDirName name of directory in the bundle
     * @throws IllegalArgumentException if {@code dstDirName} is not {@code etc}
     *     or {@code opt}, or {@code location} is not a file or directory.
     */
    public void addFileDependency(String location, String dstDirName)
            throws IllegalArgumentException {
        
        if (dstDirName==null || !(dstDirName.equals("etc") || dstDirName.equals("opt")))
            throw new IllegalArgumentException(Messages.getString("CORE_DEP_DIR_INVALID"));
        
        File f = new File(location);
        if (!f.exists() || (!f.isFile() && !f.isDirectory()))
            throw new IllegalArgumentException(Messages.getString("CORE_DEP_FILE_INVALID", f.toPath().toAbsolutePath().toString()));
        
        globalFileDependencies.add(new Artifact(dstDirName,
                f.toPath().toAbsolutePath()));    
    }

    /**
     * Resolve the dependencies.
     * Creates entries in the graph config that will
     * result in files being copied into the toolkit.
     */
    public void resolveDependencies()
            throws IOException, URISyntaxException {
        
        JsonObject graphConfig = topology.builder().getConfig();
        JsonArray includes = array(graphConfig, "includes");
        if (includes == null)
            graphConfig.add("includes", includes = new JsonArray());
              
        for (BOperatorInvocation op : operatorToJarDependencies.keySet()) {    
            ArrayList<String> jars = new ArrayList<String>();
            
            for (Path source : operatorToJarDependencies.get(op)) {
                String jarName = resolveDependency(source, false, includes);
                jars.add(DEP_JAR_LOC + File.separator + jarName);
            }

            String[] jarPaths = jars.toArray(new String[jars.size()]);
            op.setParameter("jar", jarPaths);
        }
        
        ArrayList<String> jars = new ArrayList<String>();
        for(Path source : globalDependencies.keySet()){
            boolean containsOperator = globalDependencies.get(source);
            String jarName = resolveDependency(source, containsOperator, includes);
            String location = depJarRoot(containsOperator);
            jars.add(location + File.separator  + jarName);	    
        }	
        
        List<BOperator> ops = topology.builder().getOps();
        if(jars.size() != 0){
            for (BOperator op : ops) {
                if (JavaFunctionalOps.isFunctional(op)) {
                    JsonArray value = arrayCreate(op._json(), "parameters", "jar", "value");
                    for (String jar : jars) {
                        value.add(new JsonPrimitive(jar));
                    }
                }
            }
        }

        for(Artifact dep : globalFileDependencies)
            resolveFileDependency(dep, includes);
    }
    
    private static String depJarRoot(boolean containsOperator) {
        return containsOperator ? DEP_OP_JAR_LOC : DEP_JAR_LOC;
    }
    
    private String resolveDependency(Path source, boolean containsOperator, JsonArray includes){ 
        
        String jarName;
        
        if (!previouslyCopiedDependencies.containsKey(source)) {
            
            File sourceFile = source.toFile();
            
            JsonObject include = new JsonObject();
                 
            // If it's a file, we assume its a jar file.
            if (sourceFile.isFile()) {
                jarName = source.getFileName().toString();
                
                include.addProperty("source", source.toAbsolutePath().toString());
                include.addProperty("target", depJarRoot(containsOperator));
            } 
            
            else if (sourceFile.isDirectory()) {
                // Create an entry that will convert the classes dir into a jar file
                jarName = "classes" + previouslyCopiedDependencies.size() + "_" + sourceFile.getName() + ".jar";
                include.addProperty("classes", source.toAbsolutePath().toString());
                include.addProperty("name", jarName);
                include.addProperty("target", DEP_JAR_LOC);
            }
            
            else {
                throw new IllegalArgumentException(Messages.getString("CORE_PATH_INVALID", source));
            }
            includes.add(include);
            previouslyCopiedDependencies.put(source, jarName);
        } 
        
        else {
            jarName = previouslyCopiedDependencies.get(source);
        }
        
        // Sanity check
        if(null == jarName){
            throw new IllegalStateException(Messages.getString("CORE_ERROR_RESOLVING_DEP", source));
        }
        return jarName;
    }
    
    /**
     * Copy the Artifact to the toolkit
     */
    private void resolveFileDependency(Artifact a, JsonArray includes)  {
        JsonObject include = new JsonObject();
        include.addProperty("source", a.absPath.toString());
        include.addProperty("target", a.dstDirName);
        includes.add(include);
   }
}
