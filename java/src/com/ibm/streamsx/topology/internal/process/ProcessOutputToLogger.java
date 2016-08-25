/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProcessOutputToLogger implements Runnable {
    private final Logger logger;
    private final Level level;
    private final BufferedReader reader;

    public static void log(Logger logger, Process process) {
        // standard error
        new ProcessOutputToLogger(logger, Level.SEVERE,
                process.getErrorStream());

        // standard out
        new ProcessOutputToLogger(logger, Level.INFO, process.getInputStream());
    }

    ProcessOutputToLogger(Logger logger, Level level, InputStream processOutput) {
        super();
        this.logger = logger;
        this.level = level;
        this.reader = new BufferedReader(new InputStreamReader(processOutput));

        Thread t = new Thread(this);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void run() {

        try (BufferedReader r = reader) {
            while (!Thread.currentThread().isInterrupted()) {
                String line = reader.readLine();
                if (line == null)
                    break;
                logger.log(level, line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
