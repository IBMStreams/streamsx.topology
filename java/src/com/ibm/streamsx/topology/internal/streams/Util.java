/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static java.lang.Math.min;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.messages.Messages;

public class Util {
    public static final String STREAMS_DOMAIN_ID = "STREAMS_DOMAIN_ID";
    public static final String STREAMS_INSTANCE_ID = "STREAMS_INSTANCE_ID";
    private static final String STREAMS_INSTALL = "STREAMS_INSTALL";
    public static final String STREAMS_USERNAME = "STREAMS_USERNAME";
    public static final String STREAMS_PASSWORD = "STREAMS_PASSWORD";
    private static String streamsInstall;
    
    /**
     * Logger used for the interactions with IBM Streams functionality, name {@code com.ibm.streamsx.topology.streams}.
     */
    public static Logger STREAMS_LOGGER = Logger.getLogger("com.ibm.streamsx.topology.streams"); 

    /**
     * Get a valid STREAMS_INSTALL value.
     * @return STREAMS_INSTALL value
     * @throws IllegalStateException if STREAMS_INSTALL not set
     *          or not set to a Streams install directory
     */
    public static String getStreamsInstall() {
        if (streamsInstall==null) {
            streamsInstall = verifyStreamsInstall(getenv(STREAMS_INSTALL));
        }
        return streamsInstall;
    }

    private static String verifyStreamsInstall(String installDir) {
        File si = new File(installDir);
        String sicp;
        try {
            sicp = si.getCanonicalPath();
        } catch (IOException e) {
            sicp = null;
        }
        File f = new File(si, ".product");
        if (sicp == null || !f.exists())
            throw new IllegalStateException(Messages.getString("STREAMS_IS_NOT_A_STREAMS_INSTALL_DIRECTORY", STREAMS_INSTALL, installDir));
        return sicp;
    }
    
    /**
     * Get a value for Streams install directory, using the value from
     * the config, defaulting to $STREAMS_INSTALL.
     */
    public static String getStreamsInstall(JsonObject deploy, String installKey) {
        if (!deploy.has(installKey))
            return getStreamsInstall();
        
        return verifyStreamsInstall(jstring(deploy, installKey));
    }
    
    /**
     * Get the environment variable value
     * @param name environment variable name
     * @return env value
     * @throws IllegalStateException if name not set
     */
    public static String getenv(String name) {
        String s = System.getenv(name);
        if (s==null)
            throw new IllegalStateException(Messages.getString("STREAMS_ENVIRONMENT_VARIABLE_IS_NOT_SET", name));
        return s;
    }
    
    /**
     * Check if things are in place to successfully invoke streamtool
     * @throws IllegalStateException if preconditions not met
     */
    public static void checkInvokeStreamtoolPreconditions() 
            throws IllegalStateException {
        getDefaultDomainId();
        getDefaultInstanceId();
    }
    
    public static String getDefaultDomainId() {
        return Util.getenv(Util.STREAMS_DOMAIN_ID);
    }
    public static String getDefaultInstanceId() {
        return Util.getenv(Util.STREAMS_INSTANCE_ID);
    }
 
    /**
     * Extract the map item <code>key</code> with constraint checking.
     * @param map map of key/value
     * @param key item of interest
     * @param requiredClass
     * @return the value associated with the key
     * @throws IllegalArgumentException if value is null or isn't instanceof requiredClass
     */
    public static <T> T getConfigEntry(Map<String,? extends Object> map, String key, 
            Class<T> requiredClass) {
        Object val = map.get(key);
        if (val==null)
            throw new IllegalArgumentException(Messages.getString("STREAMS_CONFIG_ITEM_NULL", key));
        if (!requiredClass.isAssignableFrom(val.getClass())) {
            throw new IllegalArgumentException(Messages.getString("STREAMS_CONFIG_ITEM_REQUIRE", key, val.getClass(), requiredClass));
        }
        return requiredClass.cast(val);
    }

    /**
     * Concatenate a list with items separated with a space.
     * 
     * @param strings list to concatenate
     * @return concatenated list
     */
    public static String concatenate(List<String> strings) {
        // wish for java8 String.join()
        StringBuilder cmdsb = new StringBuilder();
        for(String s : strings) {
            cmdsb.append(s); cmdsb.append(" ");
        }
        return cmdsb.toString();
    }
    
    // The version of IBM Streams being used to build
    // the topology. When Streams install is not
    // set we assume we are building against the
    // Streaming Analytics service.
    private final static String SERVICE_VERSION = "4.2.1";
    public static String productVersion() {
        String si = System.getenv(STREAMS_INSTALL);
        if (si == null || si.isEmpty()) {
            // assume Streaming Analytics, version may be newer than
            // this but this sets the min level.
            return SERVICE_VERSION;
        }
        // This verifies the install.
        getStreamsInstall();
        
        try {
            Class<?> product = Class.forName("com.ibm.streams.operator.version.Product");
            
            return product.getMethod("getVersion").invoke(null).toString();

        } catch (ClassNotFoundException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static boolean versionAtLeast(int v, int r, int m) {
        int[] vers = productVersions();
        
        int pv = vers[0];
        if (pv > v)
            return true;
        if (pv < v)
            return false;
        
        // same v
        int pr = vers[1];
        if (pr > r)
            return true;
        if (pr < r)
            return false;
        
        // same v.r       
        int pm = vers[2];
        return pm >= m;
    }
    
    private static int[] productVersions() {
        String vers = productVersion();

        final String[] vs = vers.toString().split("\\.");
        final int[] iv = new int[4];
        for (int i = 0; i < min(iv.length, vs.length); i++) {
            iv[i] = Integer.valueOf(vs[i]);
        }
        return iv;
    }

}
