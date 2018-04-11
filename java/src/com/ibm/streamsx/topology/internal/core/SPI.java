/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
 */
package com.ibm.streamsx.topology.internal.core;

import java.io.IOException;
import java.util.Properties;

/**
 * Functions/information for the SPI.
 *
 */
public class SPI {
    
    private static final Properties SPI_PROPERTIES = new Properties();
    
    static {
        try {
            SPI_PROPERTIES.load(SPI.class.getResourceAsStream("/com/ibm/streamsx/topology/spi/spi.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Namespace for Java functional topology operators used by the SPI.
     * @return
     */
    public static String namespaceJava() {
        return SPI_PROPERTIES.getProperty("namespace.topology.java");
    }
}
