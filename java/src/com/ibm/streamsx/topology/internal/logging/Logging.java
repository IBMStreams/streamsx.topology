/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.logging;

import java.util.logging.Level;
import java.util.logging.Logger;

public interface Logging {
    
    /**
     * Set the root logging levels from Python logging integer level.
     * @param levelS
     */
    public static void setRootLevels(String levelS) {
        int loggingLevel = Integer.valueOf(levelS);
        Level level;
        if (loggingLevel >= 40) {
            level = Level.SEVERE;
        } else if (loggingLevel >= 30) {
            level = Level.WARNING;
        } else if (loggingLevel >= 20) {
            level = Level.CONFIG;
        } else {
            level = Level.FINE;
        }
        
        Logger.getLogger("").setLevel(level);
    }

}
