/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.messaging.kafka;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.ibm.streamsx.topology.internal.messages.Messages;

import com.ibm.streamsx.topology.TopologyElement;

public class Util {
    @SuppressWarnings("unused")
    private static final Util forCoverage = new Util();
    
    private Util() { }
    
    static String[] toKafkaProperty(Map<String,Object> props) {
        List<String> list = new ArrayList<>();
        for (Entry<String,Object> e : props.entrySet()) {
            list.add(e.getKey()+"="+e.getValue());
        }
        return list.toArray(new String[list.size()]);
    }
    
    static void addPropertiesFile(TopologyElement te, String splParameter) {
        try {
            File tmpDir = Files.createTempDirectory("kafkaStreams").toFile();
            
            Path p = new File(splParameter).toPath();
            String dstDirName = p.getName(0).toString();
            Path pathInDst = p.subpath(1, p.getNameCount());
            if (pathInDst.getNameCount() > 1) {
                File dir = new File(tmpDir, pathInDst.getParent().toString());
                dir.mkdirs();
            }
            new File(tmpDir, pathInDst.toString()).createNewFile();
            File location = new File(tmpDir, pathInDst.getName(0).toString());
            
            te.topology().addFileDependency(location.getAbsolutePath(), 
                    dstDirName);
        }
        catch (IOException e) {
            throw new IllegalStateException(Messages.getString("MESSAGING_KAFKA_UNABLE_TO_CREATE_FILE", e), e);
        }
    }
 
    public static String identifyStreamsxMessagingVer() throws Exception {
        String tkloc = System.getenv("STREAMS_INSTALL")
                        + "/toolkits/com.ibm.streamsx.messaging";
        File tkRoot = new File(tkloc);
        
        return com.ibm.streamsx.topology.internal.context.remote.TkInfo.getToolkitInfo(tkRoot).getIdentity().getVersion();     
    }
}
