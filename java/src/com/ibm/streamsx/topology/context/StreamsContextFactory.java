/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

import static com.ibm.streamsx.topology.context.StreamsContextWrapper.wrap;

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
        return wrap(new EmbeddedStreamsContext());
    }

    public static StreamsContext<?> getStreamsContext(String type) {
        return getStreamsContext(StreamsContext.Type.valueOf(type));
    }

    public static StreamsContext<?> getStreamsContext(StreamsContext.Type type) {
        switch (type) {
        case EMBEDDED:
            return getEmbedded();
        case TOOLKIT:
            return wrap(new ToolkitStreamsContext());
        case BUILD_ARCHIVE:
            return wrap(new ZippedToolkitStreamsContext());
        case STANDALONE_BUNDLE:
            return wrap(new BundleStreamsContext(true));
        case BUNDLE:
            return wrap(new BundleStreamsContext(false));
        case STANDALONE:
            return wrap(new StandaloneStreamsContext());
        case DISTRIBUTED:
            return wrap(new DistributedStreamsContext());
        case STANDALONE_TESTER:
            return wrap(new StandaloneTester());
        case EMBEDDED_TESTER:
            return wrap(new EmbeddedTester());
        case DISTRIBUTED_TESTER:
            return wrap(new DistributedTester());
        case ANALYTICS_SERVICE:
            return wrap(new AnalyticsServiceStreamsContext());
        default:
            throw new IllegalArgumentException("Unknown type:" + type);
        }
    }
}
