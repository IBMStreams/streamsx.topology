/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.streams;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.internal.toolkit.info.ObjectFactory;
import com.ibm.streamsx.topology.internal.toolkit.info.ToolkitInfoModelType;

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
            throw new IllegalStateException(STREAMS_INSTALL+" "+installDir+" is not a Streams install directory.");
        return sicp;
    }
    
    /**
     * Get a value for Streams install directory, using the value from
     * the config, defaulting to $STREAMS_INSTALL.
     */
    public static String getStreamsInstall(JSONObject deployConfig, String installKey) {
        if (deployConfig == null || !deployConfig.containsKey(installKey))
            return getStreamsInstall();
        
        return verifyStreamsInstall(deployConfig.get(installKey).toString());
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
            throw new IllegalStateException(name+" environment variable is not set.");
        return s;
    }
    
    /**
     * Check if things are in place to successfully invoke streamtool
     * @throws IllegalStateException if preconditions not met
     */
    public static void checkInvokeStreamtoolPreconditions() 
            throws IllegalStateException {
        Util.getenv(Util.STREAMS_DOMAIN_ID);
        Util.getenv(Util.STREAMS_INSTANCE_ID);
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
            throw new IllegalArgumentException("config item "+key+" value is null");
        if (!requiredClass.isAssignableFrom(val.getClass())) {
            throw new IllegalArgumentException("config item "+key
                                        + " value is "+val.getClass()
                                        + " but require instanceof "+requiredClass);
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
    
    /**
     * Get the full toolkit information.
     * Returns null if there is no info.xml
     */
    public static ToolkitInfoModelType getToolkitInfo(File toolkitRoot) throws JAXBException {

        File infoFile = new File(toolkitRoot, "info.xml");
        if (!infoFile.exists())
            return null;
        
        StreamSource infoSource = new StreamSource(infoFile);

        JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        ToolkitInfoModelType tkinfo = jaxbUnmarshaller.unmarshal(infoSource, ToolkitInfoModelType.class).getValue();
        return tkinfo;
    }
}
