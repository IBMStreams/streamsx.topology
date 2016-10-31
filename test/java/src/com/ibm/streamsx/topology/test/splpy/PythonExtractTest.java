/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.splpy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.junit.Test;

import com.ibm.streamsx.topology.internal.process.ProcessOutputToLogger;
import com.ibm.streamsx.topology.test.TestTopology;

public class PythonExtractTest extends TestTopology {
    
    static Logger trace = Logger.getLogger(PythonExtractTest.class.getName());
    
    public static int extract(File tkDir, boolean tkMake) throws Exception {
        
        String topoTk = System.getProperty("topology.toolkit.release");
        String streams = System.getProperty("topology.install.compile");
        
        File expy = new File(topoTk, "bin/spl-python-extract.py");

        List<String> commands = new ArrayList<String>();

        commands.add(System.getProperty("topology.test.python", "python3"));
        commands.add(expy.getAbsolutePath());
        commands.add("--directory");
        commands.add(tkDir.getAbsolutePath());
        if (tkMake)
            commands.add("--make-toolkit");

        ProcessBuilder pb = new ProcessBuilder(commands);
               
        // Set STREAMS_INSTALL in case it was overriden for compilation
        pb.environment().put("STREAMS_INSTALL", streams);

        Process exProcess = pb.start();
        ProcessOutputToLogger.log(trace, exProcess);
        exProcess.getOutputStream().close();
        int rc = exProcess.waitFor();
        trace.info("spl-python-extract.py complete: return code=" + rc);
               
        return rc;
    }
    
    
    public static File createPyTk(String namespace, List<String> pythonCode) throws Exception {
        File tkdir = Files.createTempDirectory("testpyex").toFile();

        File pystreams = new File(tkdir, "opt/python/streams");
        pystreams.mkdirs();
        if (namespace != null || pythonCode != null) {
            Path python = Files.createTempFile(pystreams.toPath(), "tpx", ".py");
            Files.write(python, Collections.singletonList("from streamsx.spl import spl\n"), StandardCharsets.UTF_8);
            if (namespace != null) {
                List<String> nsfn = new ArrayList<>();
                nsfn.add("def splNamespace():");
                nsfn.add("  return '" + namespace + "'\n");
                Files.write(python, nsfn, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            }
            
            if (pythonCode != null)
                Files.write(python, pythonCode, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        }

        return tkdir;
    }
    
    @Test
    public void testEmptyToolkit() throws Exception {
        _testEmptyToolkit(false);
    }
    @Test
    public void testEmptyToolkitMake() throws Exception {
        _testEmptyToolkit(true);
    }
    
    private void _testEmptyToolkit(boolean make) throws Exception {
        
        File tkdir = Files.createTempDirectory("testpyex").toFile();
        try {
             assertEquals(0, extract(tkdir, make)); 
        } finally {
            tkdir.delete();
        }
    }

    @Test
    public void testEmptyToolkitDir() throws Exception {
        _testEmptyToolkitWithDir(false);
    }
    @Test
    public void testEmptyToolkitDirMake() throws Exception {
        _testEmptyToolkitWithDir(true);
    }
    
    static final String NS = "testpy.extract";
    
    private void _testEmptyToolkitWithDir(boolean make) throws Exception {        
        File tkdir = createPyTk(null, null);
        try {
             assertEquals(0, extract(tkdir, make)); 
        } finally {
            tkdir.delete();
        }
    }
    
    @Test
    public void testEmptyPython() throws Exception {
        _testToolkit(Collections.emptyList(), false, tkdir->{});
    }
    @Test
    public void testEmptyPythonMake() throws Exception {
        _testToolkit(Collections.emptyList(), true, tkdir->{});
    }
    
    public static class TestGeneratedOpArtifacts implements Consumer<File> {

        private final String op;
        private final boolean pm;

        public TestGeneratedOpArtifacts(String op) {
            this.op = op;
            this.pm = false;
        }
        public TestGeneratedOpArtifacts(String op, boolean pm) {
            this.op = op;
            this.pm = pm;
        }

        @Override
        public void accept(File tkdir) {
            assertTrue(tkdir.isDirectory());
            assertTrue(tkdir.exists());
            
            File nsdir = new File(tkdir, NS);
            assertTrue(nsdir.isDirectory());
            assertTrue(nsdir.exists());
            
            File opdir = new File(nsdir, op);
            assertTrue(opdir.isDirectory());
            assertTrue(opdir.exists());
            
            File opFile = new File(opdir, op + ".xml");
            assertTrue(opFile.toString(), opFile.isFile());
            assertTrue(opFile.toString(), opFile.exists());
            
            opFile = new File(opdir, op + "_cpp.cgt");
            assertTrue(opFile.toString(), opFile.isFile());
            assertTrue(opFile.toString(), opFile.exists());

            opFile = new File(opdir, op + "_h.cgt");
            assertTrue(opFile.toString(), opFile.isFile());
            assertTrue(opFile.toString(), opFile.exists());

            if (pm) {
                opFile = new File(opdir, op + "_cpp.pm");
                assertTrue(opFile.toString(), opFile.isFile());
                assertTrue(opFile.toString(), opFile.exists());

                opFile = new File(opdir, op + "_h.pm");
                assertTrue(opFile.toString(), opFile.isFile());
                assertTrue(opFile.toString(), opFile.exists());
            }
            
        }

    }
    
    @Test
    public void testPythonMap() throws Exception {
        String[] code = {
                "@spl.map()\n",
                "def f1(*tuple):\n",
                "  pass\n"
        };
        _testToolkit(Arrays.asList(code), false, new TestGeneratedOpArtifacts("f1"));
    }
    @Test
    public void testPythonMapMake() throws Exception {
        String[] code = {
                "@spl.map()\n",
                "def f2(*tuple):\n",
                "  pass\n"
        };
        _testToolkit(Arrays.asList(code), true, new TestGeneratedOpArtifacts("f2", true));
    }
    
    
    public static void _testToolkit(List<String> lines, boolean make, Consumer<File> tester) throws Exception {        
        File tkdir = createPyTk(NS, lines);
        try {
             assertEquals(0, extract(tkdir, make)); 
             tester.accept(tkdir);
        } finally {
            tkdir.delete();
        }
    }
}
