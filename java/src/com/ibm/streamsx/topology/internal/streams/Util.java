/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import java.io.File;

public class Util {
    public static final String STREAMS_DOMAIN_ID = "STREAMS_DOMAIN_ID";
    public static final String STREAMS_INSTANCE_ID = "STREAMS_INSTANCE_ID";
    private static final String STREAMS_INSTALL = "STREAMS_INSTALL";
    private static String streamsInstall;

    /**
     * Get a valid STREAMS_INSTALL value.
     * @return STREAMS_INSTALL value
     * @throws IllegalStateException if STREAMS_INSTALL not set
     *          or not set to a Streams install directory
     */
    public static String getStreamsInstall() throws IllegalStateException {
        if (streamsInstall==null) {
            String si = System.getenv(STREAMS_INSTALL);
            if (si==null)
                throw new IllegalStateException(STREAMS_INSTALL+" environment variable is not set.");
            File f = new File(si, ".product");
            if (!f.exists())
                throw new IllegalStateException(STREAMS_INSTALL+" "+si+" is not a Streams install directory.");
            streamsInstall = si;
        }
        return streamsInstall;
    }
    
    /**
     * Get the environment variable value
     * @param name environment variable name
     * @return env value
     * @throws IllegalStateException if name not set
     */
    public static String getenv(String name) throws IllegalStateException {
        String s = System.getenv(name);
        if (s==null)
            throw new IllegalStateException(name+" environment variable is not set.");
        return s;
    }
}
