/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.context;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeTrue;

import org.junit.Test;

import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.test.TestTopology;

public class ContextTypeTest extends TestTopology {

    /**
     * Test that all types return a valid StreamsContext
     */
    @Test
    public void testFactoryByType() {
        assumeTrue(isMainRun());
        for (StreamsContext.Type type : StreamsContext.Type.values()) {
            StreamsContext<?> context = StreamsContextFactory
                    .getStreamsContext(type);
            assertNotNull(context);
            assertSame(type, context.getType());

            context = StreamsContextFactory.getStreamsContext(type.name());
            assertNotNull(context);
            assertSame(type, context.getType());
        }
    }
}
