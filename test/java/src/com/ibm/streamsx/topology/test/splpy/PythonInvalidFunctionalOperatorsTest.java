/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.splpy;

import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.streamsx.topology.test.TestTopology;

public class PythonInvalidFunctionalOperatorsTest extends TestTopology {
	
    @BeforeClass
    public static void checkPython() {
    	String pythonversion = System.getProperty("topology.test.python");
    	assumeTrue(pythonversion == null || !pythonversion.isEmpty());
    }
    
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
    
    @Test
    public void testOpParamNonAsciiIdentifier() throws Exception {
        String[] code = {
                "@spl.map()\n",
                "class op1(object):\n",
                "  def __init__(self, naïve):\n",
                "     pass\n",
                "  def __call__(self, *tuple):\n",
                "     pass\n"
        };
        _testInvalidToolkit(Arrays.asList(code));
    }    
    
    @Test
    public void testOpParamKeyword() throws Exception {
        String[] code = {
                "@spl.map()\n",
                "class op2(object):\n",
                "  def __init__(self, stream):\n",
                "     pass\n",
                "  def __call__(self, *tuple):\n",
                "     pass\n"
        };
        _testInvalidToolkit(Arrays.asList(code));
    }
    
    @Test
    public void testOpParamSuppress() throws Exception {
        String[] code = {
                "@spl.map()\n",
                "class op3(object):\n",
                "  def __init__(self, suppress):\n",
                "     pass\n",
                "  def __call__(self, *tuple):\n",
                "     pass\n"
        };
        _testInvalidToolkit(Arrays.asList(code));
    }
    
    @Test
    public void testOpParamInclude() throws Exception {
        String[] code = {
                "@spl.map()\n",
                "class op4(object):\n",
                "  def __init__(self, include):\n",
                "     pass\n",
                "  def __call__(self, *tuple):\n",
                "     pass\n"
        };
        _testInvalidToolkit(Arrays.asList(code));
    }
    
    @Test
    public void testInvalidClassName() throws Exception {
        String[] code = {
                "@spl.map()\n",
                "class NaïveC(object):\n",
                "  def __init__(self):\n",
                "     pass\n",
                "  def __call__(self, *tuple):\n",
                "     pass\n"     
        };
        _testInvalidToolkit(Arrays.asList(code));
    }
    @Test
    public void testInvalidFunctionName() throws Exception {
        String[] code = {
                "@spl.map()\n",
                "def NaïveF(*tuple):\n",
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
