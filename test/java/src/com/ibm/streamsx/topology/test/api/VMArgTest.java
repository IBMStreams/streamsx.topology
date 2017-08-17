/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.test.TestTopology;

public class VMArgTest extends TestTopology {

    @Test
    public void testSettingSystemProperty() throws Exception {
        
        assumeTrue(!isEmbedded());
        
        final Topology topology = newTopology("testSettingSystemProperty");
        
        final String propertyName = "tester.property.921421";
        final String propertyValue = "abcdef832124";
        final String vmArg = "-D" + propertyName + "=" + propertyValue;
        
        TStream<String> source = topology.limitedSource(new ReadProperty(propertyName), 1);

        final Map<String,Object> config = getConfig();
        @SuppressWarnings("unchecked")
        List<String> vmArgs = (List<String>) config.get(ContextProperties.VMARGS);
        if (vmArgs == null)
            config.put(ContextProperties.VMARGS, vmArgs = new ArrayList<>());
        vmArgs.add(vmArg);
        
        // config.put(ContextProperties.KEEP_ARTIFACTS, Boolean.TRUE);
        completeAndValidate(config, source, 10, propertyValue);
    }
    
    @SuppressWarnings("serial")
    public static class ReadProperty implements Supplier<String> {

        private final String property;
        ReadProperty(String property) {
           this.property = property;
        }
        @Override
        public String get() {
            return System.getProperty(property);
        }       
    }
}
