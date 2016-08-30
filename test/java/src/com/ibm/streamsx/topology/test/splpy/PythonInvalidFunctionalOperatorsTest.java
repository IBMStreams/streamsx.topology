/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.splpy;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ibm.streamsx.topology.test.TestTopology;

public class PythonInvalidFunctionalOperatorsTest extends TestTopology {
    
    @Test
    public void testInvalidStyle() throws Exception {
        String[] code = {
                "@spl.map(style='fred')\n",
                "def inv1(*tuple):\n",
                "  pass\n"
        };
        _testInvalidToolkit(Arrays.asList(code));
    }
    @Test
    public void testMismatchedStyle1() throws Exception {
        String[] code = {
                "@spl.map(style='name')\n",
                "def inv2(*tuple):\n",
                "  pass\n"
        };
        _testInvalidToolkit(Arrays.asList(code));
    }
    @Test
    public void testKwargsAndPositional() throws Exception {
        String[] code = {
                "@spl.map(style='positional')\n",
                "def inv3(a,b,c,**tuple):\n",
                "  pass\n"
        };
        _testInvalidToolkit(Arrays.asList(code));
    }
    @Test
    public void testPositionalWithKeywordOnly() throws Exception {
        String[] code = {
                "@spl.map(style='positional')\n",
                "def inv4(a,b,c,**tuple,d):\n",
                "  pass\n"
        };
        _testInvalidToolkit(Arrays.asList(code));
    }
    
    public static void _testInvalidToolkit(String[] code) throws Exception {        
        _testInvalidToolkit(Arrays.asList(code));
    }    
    
    public static void _testInvalidToolkit(List<String> code) throws Exception {        
        File tkdir = PythonExtractTest.createPyTk(PythonExtractTest.NS, code);
        try {
             assertFalse(PythonExtractTest.extract(tkdir, false) == 0); 
        } finally {
            tkdir.delete();
        }
    }
}
