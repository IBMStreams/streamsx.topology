/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016 
 */
package com.ibm.streamsx.topology.internal.context.remote;

import java.io.File;
import java.net.URISyntaxException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import com.ibm.streamsx.topology.internal.file.FileUtilities;
import com.ibm.streamsx.topology.internal.toolkit.info.ObjectFactory;
import com.ibm.streamsx.topology.internal.toolkit.info.ToolkitDependencyType;
import com.ibm.streamsx.topology.internal.toolkit.info.ToolkitInfoModelType;

public class TkInfo {

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

    public static ToolkitDependencyType getTookitDependency(String toolkitRoot, boolean exact) throws JAXBException {
                
        ToolkitInfoModelType depTkInfo = getToolkitInfo(new File(toolkitRoot));
        if (depTkInfo == null)
            return null;
        
        String depTkVersion = depTkInfo.getIdentity().getVersion();     
        
        String versionRange;
        if (exact) {
        	versionRange = depTkVersion;
        } else {
            String[] elements = depTkVersion.split("\\.");
            int next = Integer.valueOf(elements[0]) + 1;       
            versionRange = "[" + elements[0] + "." + elements[1] + "," + next + ".0)";
        }
        
        ToolkitDependencyType depTk = new ToolkitDependencyType();      
        depTk.setName(depTkInfo.getIdentity().getName());
        depTk.setVersion(versionRange);
    
        return depTk;
    }

    public static File getTopologyToolkitRoot() throws URISyntaxException {
        // com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar
        File jarLocation = new File(FileUtilities.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
        // com.ibm.streamsx.topology
        File topologyToolkitRoot = jarLocation.getParentFile().getParentFile();    
        
        return topologyToolkitRoot;
    }

}
