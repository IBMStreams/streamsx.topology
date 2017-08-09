package com.ibm.streamsx.topology.spi.builder;

public interface Properties {

    public interface Graph {
        
        /**
         * Graph wide configuration.
         */
        String CONFIG = "config";
        
        public interface Config {
            /**
             * JsonObject mapping operator kind to Java class name.
             * Only used when a graph is executed in a embedded context.
             * When executed in a context that uses SPL then
             * operators much be available through an SPL toolkit.
             */
            String JAVA_OPS = "javaops";
        }

    }

}
