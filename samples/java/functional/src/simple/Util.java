/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package simple;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.context.StreamsContext;

public class Util {
    private static Logger LOGGER = Logger.getLogger("simple.util");
    
    private static Map<String,Level> levelMap = new HashMap<String,Level>();
    static {
        levelMap.put("off", TraceLevel.OFF);
        levelMap.put("error", TraceLevel.ERROR);
        levelMap.put("warn", TraceLevel.WARN);
        levelMap.put("info", TraceLevel.INFO);
        levelMap.put("trace", TraceLevel.TRACE);
        levelMap.put("debug", TraceLevel.DEBUG);
    }
    
    /**
     * Look for configuration properties present in <code>args</code> 
     * and sets the property in the <code>config</code> map.
     * <p>
     * An property item in <code>args</code> is required to be in the format:
     * <pre>
     * &lt;property-name>=&lt;value>
     * </pre>
     * @param args list to scan.  Unrecognized items are silently ignored.
     * @return configuration map
     * @see StreamsContext#submit(com.ibm.streamsx.topology.Topology, Map)
     */
    public static Map<String,Object> createConfig(String[] args) {
        String[] cfgItems = {
                ContextProperties.APP_DIR,  // String
                ContextProperties.BUNDLE,   // File
                ContextProperties.KEEP_ARTIFACTS, // Boolean
                ContextProperties.TOOLKIT_DIR,  // String
                ContextProperties.TRACING_LEVEL,  // java.util.logging.Level
                // TODO ContextProperties.VMARGS,   // List<String>
                JobProperties.GROUP,  // String
                JobProperties.NAME,   // String
                JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION, // Boolean
                JobProperties.DATA_DIRECTORY, // String
                JobProperties.PRELOAD_APPLICATION_BUNDLES,  // Boolean
        };

        Map<String,Object> config = new HashMap<String,Object>();

        // handle param=<value>
        for (String arg : args) {
            String[] toks = arg.split("=", 2);
            if (toks.length > 1) {
                for (String item : cfgItems) {
                    if (toks[0].equals(item)) {
                        String valStr = toks[1];
                        Object value = valStr;
                        if (item.equals(ContextProperties.BUNDLE))
                            value = new File(valStr);
                        else if (item.equals(ContextProperties.TRACING_LEVEL))
                            value = toTracingLevel(valStr);
                        else if (item.equals(ContextProperties.KEEP_ARTIFACTS))
                            value = Boolean.valueOf(valStr);
                        else if (item.equals(JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION))
                            value = Boolean.valueOf(valStr);
                        else if (item.equals(JobProperties.PRELOAD_APPLICATION_BUNDLES))
                            value = Boolean.valueOf(valStr);
                        
                        config.put(item, value);
                        LOGGER.info("Setting config: item="+item+" value="+value+"  [arg="+arg+"]");
                    }
                }
            }
        }
        
        return config;
    }
    
    private static Level toTracingLevel(String str) {
        Level l = levelMap.get(str.toLowerCase());
        if (l==null)
            throw new IllegalArgumentException("Unrecognized tracing level '"+str+"'."
                    + " Must be one of: "+levelMap.keySet());
        return l;
    }

}
