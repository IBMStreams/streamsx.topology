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

public class StreamsContextFactory {

    public static StreamsContext<JavaTestableGraph> getEmbedded() {
        return new EmbeddedStreamsContext();
    }

    public static StreamsContext<?> getStreamsContext(String type) {
        return getStreamsContext(StreamsContext.Type.valueOf(type));
    }

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
}
