/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streamsx.topology.internal.context.ToolkitStreamsContext;
import com.ibm.streamsx.topology.internal.context.ZippedToolkitStreamsContext;
import com.ibm.streamsx.topology.internal.context.service.RemoteStreamingAnalyticsServiceStreamsContext;
import com.ibm.streamsx.topology.internal.context.service.RemoteStreamingAnalyticsTester;
import com.ibm.streamsx.topology.internal.context.streams.AnalyticsServiceStreamsContext;
import com.ibm.streamsx.topology.internal.context.streams.BundleStreamsContext;
import com.ibm.streamsx.topology.internal.streams.Util;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * Factory for creating {@code StreamsContext} instances.
 *
 */
public class StreamsContextFactory {

    /**
     * Get an {@code EMBEDDED context}.
     * <BR>
     * Topology is executed within the Java virtual machine that declared it.
     * This requires that the topology only contains Java functions or primitive operators.
     * 
     * @return An {@code EMBEDDED context}.
     * 
     * @see StreamsContext.Type#EMBEDDED
     */
    @SuppressWarnings("unchecked")
    public static StreamsContext<JavaTestableGraph> getEmbedded() {
        return (StreamsContext<JavaTestableGraph>) newInstance("com.ibm.streamsx.topology.internal.embedded.EmbeddedStreamsContext");
    }

    /**
     * Get a {@code StreamsContext} from its type name.
     * <BR>
     * 
     * @param type Name of the {@link StreamsContext.Type type} from its enumeration name.
     * 
     * @return An {@link StreamsContext} instance.
     * 
     * @see StreamsContext.Type
     */
    public static StreamsContext<?> getStreamsContext(String type) {
        return getStreamsContext(StreamsContext.Type.valueOf(type));
    }

    /**
     * Get a {@code StreamsContext} from its type.
     * <BR>
     * 
     * @param type {@link StreamsContext.Type Type} of the context.
     * 
     * @return An {@link StreamsContext} instance.
     * 
     * @see StreamsContext.Type
     */
    public static StreamsContext<?> getStreamsContext(StreamsContext.Type type) {
        switch (type) {
        case EMBEDDED:
            return getEmbedded();
        case TOOLKIT:
            return new ToolkitStreamsContext(true);
        case BUILD_ARCHIVE:
            return new ZippedToolkitStreamsContext(true);
        case STANDALONE_BUNDLE:
            return new BundleStreamsContext(true, true);
        case BUNDLE:
            return new BundleStreamsContext(false, true);
        case STANDALONE:
            return newInstance("com.ibm.streamsx.topology.internal.context.streams.StandaloneStreamsContext");
        case DISTRIBUTED:
            return newInstance("com.ibm.streamsx.topology.internal.context.streams.DistributedStreamsContext");
        case STANDALONE_TESTER:
            return newInstance("com.ibm.streamsx.topology.internal.context.streams.StandaloneTester");
        case EMBEDDED_TESTER:
            return newInstance("com.ibm.streamsx.topology.internal.embedded.EmbeddedTester");
        case DISTRIBUTED_TESTER:
            return newInstance("com.ibm.streamsx.topology.internal.context.streams.DistributedTester");
        
        case ANALYTICS_SERVICE:
        case STREAMING_ANALYTICS_SERVICE:
            String si = System.getenv("STREAMS_INSTALL");
            if (si == null || si.isEmpty())
                return new RemoteStreamingAnalyticsServiceStreamsContext();
            return new AnalyticsServiceStreamsContext(type);
            
        case STREAMING_ANALYTICS_SERVICE_TESTER:
            return new RemoteStreamingAnalyticsTester();
        default:
            throw new IllegalArgumentException(Messages.getString("CONTEXT_UNKNOWN_TYPE", type));
        }
    }
    
    @SuppressWarnings("unchecked")
    private static StreamsContext<?> newInstance(String contextClassName) {
        try {
            Class<StreamsContext<?>> contextClass;
            contextClass = (Class<StreamsContext<?>>) Class.forName(contextClassName);
                    
            return contextClass.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new IllegalStateException(e);
        }

    }
    
    /**
     * Get the default Streams domain identifier.
     * <BR>
     * Returns the default Streams domain identifier used by
     * contexts
     * {@link StreamsContext.Type#DISTRIBUTED DISTRIBUTED}
     * and {@link StreamsContext.Type#DISTRIBUTED_TESTER DISTRIBUTED_TESTER}.
     * 
     * <P>
     * This is obtained from the environment variable {@code STREAMS_DOMAIN_ID}.
     * </P>
     * 
     * @return Default Streams domain identifier.
     * 
     * @since 1.7
     */
    public static String getDefaultDomainId() {
        return Util.getDefaultDomainId();
    }
    
    /**
     * Get the default Streams instance identifier.
     * <BR>
     * Returns the default Streams instance identifier used by
     * contexts
     * {@link StreamsContext.Type#DISTRIBUTED DISTRIBUTED}
     * and {@link StreamsContext.Type#DISTRIBUTED_TESTER DISTRIBUTED_TESTER}.
     * 
     * <P>
     * This is obtained from the environment variable {@code STREAMS_INSTANCE_ID}.
     * </P>
     * 
     * @return Default Streams instance identifier.
     * 
     * @since 1.7
     */
    public static String getDefaultInstanceId() {
        return Util.getDefaultInstanceId();
    }
    
    /**
     * Get the default Streams install location.
     * <BR>
     * Returns the default Streams install location used by
     * contexts
     * {@link StreamsContext.Type#BUNDLE BUNDLE},
     * {@link StreamsContext.Type#STANDALONE_BUNDLE STANDALONE_BUNDLE},
     * {@link StreamsContext.Type#STANDALONE STANDALONE},
     * {@link StreamsContext.Type#STANDALONE_TESTER STANDALONE_TESTER},
     * {@link StreamsContext.Type#DISTRIBUTED DISTRIBUTED}
     * and {@link StreamsContext.Type#DISTRIBUTED_TESTER DISTRIBUTED_TESTER}.
     * 
     * <P>
     * This is obtained from the environment variable {@code STREAMS_INSTALL}.
     * </P>
     * 
     * @return Default Streams install.
     * 
     * @since 1.7
     */
    public static String getDefaultStreamsInstall() {
        return Util.getStreamsInstall();
    }
}
