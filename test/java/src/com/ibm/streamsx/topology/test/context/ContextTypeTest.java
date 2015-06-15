/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.context;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;

public class ContextTypeTest {

    /**
     * Test that all types return a valid StreamsContext
     */
    @Test
    public void testFactoryByType() {
        for (StreamsContext.Type type : StreamsContext.Type.values()) {
            StreamsContext context = StreamsContextFactory
                    .getStreamsContext(type);
            assertNotNull(context);
            assertSame(type, context.getType());

            context = StreamsContextFactory.getStreamsContext(type.name());
            assertNotNull(context);
            assertSame(type, context.getType());
        }
    }
}
