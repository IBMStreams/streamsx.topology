/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streamsx.topology.internal.context.AnalyticsServiceStreamsContext;
import com.ibm.streamsx.topology.internal.context.BundleStreamsContext;
import com.ibm.streamsx.topology.internal.context.DistributedStreamsContext;
import com.ibm.streamsx.topology.internal.context.DistributedTester;
import com.ibm.streamsx.topology.internal.context.EmbeddedStreamsContext;
import com.ibm.streamsx.topology.internal.context.EmbeddedTester;
import com.ibm.streamsx.topology.internal.context.StandaloneStreamsContext;
import com.ibm.streamsx.topology.internal.context.StandaloneTester;
import com.ibm.streamsx.topology.internal.context.ToolkitStreamsContext;
import com.ibm.streamsx.topology.internal.context.ZippedToolkitStreamsContext;
import com.ibm.streamsx.topology.internal.streams.Util;

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
    public static StreamsContext<JavaTestableGraph> getEmbedded() {
        return new EmbeddedStreamsContext();
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
            return new StandaloneStreamsContext();
        case DISTRIBUTED:
            return new DistributedStreamsContext();
        case STANDALONE_TESTER:
            return new StandaloneTester();
        case EMBEDDED_TESTER:
            return new EmbeddedTester();
        case DISTRIBUTED_TESTER:
            return new DistributedTester();
        case ANALYTICS_SERVICE:
        case STREAMING_ANALYTICS_SERVICE:
            return new AnalyticsServiceStreamsContext(type);
        default:
            throw new IllegalArgumentException("Unknown type:" + type);
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
        return Util.getenv(Util.STREAMS_DOMAIN_ID);
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
        return Util.getenv(Util.STREAMS_INSTANCE_ID);
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
