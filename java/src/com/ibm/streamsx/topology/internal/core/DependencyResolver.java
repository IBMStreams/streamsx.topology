/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext.DEP_JAR_LOC;

import java.io.File;
import java.io.IOException;
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

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperator;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext;
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
    private final Set<Artifact> globalFileDependencies = new HashSet<>();

    private static class Artifact {
        final String dstDirName;
        final Path absPath;
        Artifact(String dirName, Path absPath) {
            if (dirName==null || absPath==null)
                throw new IllegalArgumentException("dstDirName="+dirName+" absPath="+absPath);
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
            throw new IllegalArgumentException("File not found. Invalid "
      	       + "third party dependency location:"+ f.toPath().toAbsolutePath().toString());
        }
        globalDependencies.add(f.toPath().toAbsolutePath());    
    }
    
    public void addClassDependency(Class<?> clazz){
        CodeSource source = clazz.getProtectionDomain().getCodeSource();
        if (source == null)
            return;
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
            throw new IllegalArgumentException("dstDirName="+dstDirName);
        
        File f = new File(location);
        if (!f.exists() || (!f.isFile() && !f.isDirectory()))
            throw new IllegalArgumentException("Not a file or directory. Invalid "
                    + "file dependency location:"+ f.toPath().toAbsolutePath().toString());
        
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
        
        JSONObject graphConfig = topology.builder().getConfig();
        JSONArray includes = (JSONArray) graphConfig.get("includes");
        if (includes == null)
            graphConfig.put("includes", includes = new JSONArray());
              
        for (BOperatorInvocation op : operatorToJarDependencies.keySet()) {    
            ArrayList<String> jars = new ArrayList<String>();
            
            for (Path source : operatorToJarDependencies.get(op)) {
                String jarName = resolveDependency(source, includes);
                jars.add(DEP_JAR_LOC + File.separator + jarName);
            }

            String[] jarPaths = jars.toArray(new String[jars.size()]);
            op.setParameter("jar", jarPaths);
        }
        
        ArrayList<String> jars = new ArrayList<String>();
        for(Path source : globalDependencies){
            String jarName = resolveDependency(source, includes);
            jars.add(DEP_JAR_LOC + File.separator  + jarName);	    
        }	
        
        List<BOperator> ops = topology.builder().getOps();
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
                            JSONObject val = new JSONObject();
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

        for(Artifact dep : globalFileDependencies)
            resolveFileDependency(dep, includes);
    }
    
    private String resolveDependency(Path source, JSONArray includes){ 
        
        String jarName;
        
        if (!previouslyCopiedDependencies.containsKey(source)) {
            
            File sourceFile = source.toFile();
            
            JSONObject include = new JSONObject();
                 
            // If it's a file, we assume its a jar file.
            if (sourceFile.isFile()) {
                jarName = source.getFileName().toString();
                
                include.put("source", source.toAbsolutePath().toString());
                include.put("target", DEP_JAR_LOC);
            } 
            
            else if (sourceFile.isDirectory()) {
                // Create an entry that will convert the classes dir into a jar file
                jarName = "classes" + previouslyCopiedDependencies.size() + "_" + sourceFile.getName() + ".jar";
                include.put("classes", source.toAbsolutePath().toString());
                include.put("name", jarName);
                include.put("target", DEP_JAR_LOC);
            }
            
            else {
                throw new IllegalArgumentException("Path not a file or directory:" + source);
            }
            includes.add(include);
            previouslyCopiedDependencies.put(source, jarName);
        } 
        
        else {
            jarName = previouslyCopiedDependencies.get(source);
        }
        
        // Sanity check
        if(null == jarName){
            throw new IllegalStateException("Error resolving dependency "+ source);
        }
        return jarName;
    }
    
    /**
     * Copy the Artifact to the toolkit
     */
    private void resolveFileDependency(Artifact a, JSONArray includes)  {    	
        JSONObject include = new JSONObject();
        include.put("source", a.absPath.toString());
        include.put("target", a.dstDirName);
        includes.add(include);
   }
}
