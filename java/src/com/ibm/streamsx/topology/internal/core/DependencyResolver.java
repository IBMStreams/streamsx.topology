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
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.logic.WrapperFunction;

/**
 * The DependencyResolver class exists to separate the logic of jar
 * resolution/copying from the topology.
 * 
 */
public class DependencyResolver {
    private final Map<BOperatorInvocation, Set<CodeSource>> operatorToJarDependencies = new HashMap<>();

    /**
     * Ensure we don't copy files multiple times and keep
     * a map between a code source and the jar we generate.
     */
    private final Map<CodeSource,String> previouslyCopiedDependencies = new HashMap<>();

    private Topology parentTopology;

    public DependencyResolver(Topology parentTopology) {
        this.parentTopology = parentTopology;
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

        if (operatorToJarDependencies.containsKey(op)) {
            operatorToJarDependencies.get(op).add(source);
        } else {
            operatorToJarDependencies.put(op, new HashSet<CodeSource>());
            operatorToJarDependencies.get(op).add(source);
        }
    }

    // Resolve the dependencies. Copies jars to the impl/lib part of the bundle
    public void resolveDependencies(Map<String, Object> config)
            throws IOException, URISyntaxException {
        for (BOperatorInvocation op : operatorToJarDependencies.keySet()) {
            /*
             * SPLOperator splOp; splOp =
             * parentTopology.splgraph().getOperator(op.op()); // If the splOp
             * is null, it means that the operator for which // jars are being
             * resolved is not in the graph of // parentTopology. In other
             * words, the operator may be part of // a parallel channel, or
             * other composite. if (splOp == null) { continue; }
             */
            final File toolkitRoot = new File((String) (config
                    .get(ContextProperties.TOOLKIT_DIR)));
            ArrayList<String> jars = new ArrayList<String>();

            final File toolkitLib = new File(toolkitRoot, "impl/lib/");
            for (CodeSource cs : operatorToJarDependencies.get(op)) {
                String jarName;
                if (!previouslyCopiedDependencies.containsKey(cs)) {
                    Path absolutePath = Paths.get(cs.getLocation().toURI()).toAbsolutePath();
                    File absoluteFile = absolutePath.toFile();
                            
                    // If it's a file, we assume its a jar file.
                    if (absoluteFile.isFile()) {
                        jarName = absolutePath.getFileName().toString();
                        Files.copy(absolutePath, new File(toolkitLib, jarName).toPath(),
                            StandardCopyOption.REPLACE_EXISTING);
                    } else if (absoluteFile.isDirectory()) {
                        jarName = createJarFile(toolkitLib, absoluteFile);
                    }
                    else {
                        throw new IllegalArgumentException("CodeSource not a file or directory:" + cs);
                    }
                    previouslyCopiedDependencies.put(cs, jarName);
                } else {
                    jarName = previouslyCopiedDependencies.get(cs);
                }

                jars.add("impl/lib/" + jarName);
            }

            String[] jarPaths = jars.toArray(new String[jars.size()]);
            op.setParameter("jar", jarPaths);
        }
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
