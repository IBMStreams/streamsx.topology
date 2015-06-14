/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.samples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.junit.Test;

import simple.Echo;
import simple.FilterEcho;
import simple.HelloWorld;

public class SimpleSamplesTest {

    @Test
    public void testHelloWorld() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream newOut = new PrintStream(baos);
        System.setOut(newOut);

        try {
            HelloWorld.main(new String[0]);
        } finally {
            System.setOut(originalOut);
        }

        checkPrintedLines(baos, "Hello", "World!");
    }

    @Test
    public void testEcho() throws Exception {
        String[] input = new String[] { "one", "two", "thr@@", "vier" };
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream newOut = new PrintStream(baos);
        System.setOut(newOut);

        try {
            Echo.main(input);
        } finally {
            System.setOut(originalOut);
        }

        checkPrintedLines(baos, input);
    }

    @Test
    public void testFilterEcho() throws Exception {
        String quote = "It is not in the stars to hold our destiny but in ourselves";
        String[] input = quote.split(" ");
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream newOut = new PrintStream(baos);
        System.setOut(newOut);

        try {
            FilterEcho.main(input);
        } finally {
            System.setOut(originalOut);
        }

        checkPrintedLines(baos, "destiny");
    }

    public static void checkPrintedLines(ByteArrayOutputStream baos,
            String... strings) throws Exception {

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        BufferedReader result = new BufferedReader(new InputStreamReader(bais));

        for (String s : strings)
            assertEquals(s, result.readLine());
        assertNull(result.readLine());

    }

}
