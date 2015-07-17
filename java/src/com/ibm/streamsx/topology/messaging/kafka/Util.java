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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.ibm.streamsx.topology.TopologyElement;

class Util {
    
    static String[] toKafkaProperty(Map<String,String> props) {
        List<String> list = new ArrayList<>();
        for (Entry<String,String> e : props.entrySet()) {
            list.add(e.getKey()+"="+e.getValue());
        }
        return list.toArray(new String[list.size()]);
    }
    
    static Map<String,String> toMap(Properties props) {
        Map<String,String> map = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            map.put(key,props.getProperty(key));
        }
        return map;
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
            throw new IllegalStateException("Unable to create a properties file: " + e, e);
        }
    }

}
