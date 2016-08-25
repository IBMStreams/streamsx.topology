/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
package com.ibm.streamsx.topology.test.api;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.test.TestTopology;

public class AutoClosableTest extends TestTopology {
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */
    
    @Before
    public void onlyInEmbedded() {
        assumeTrue(isMainRun());
    }
    
    /**
     * Test close is called for various transformations.
     * @throws Exception
     */
    @Test
    public void testCloseIsCalled() throws Exception {
        Topology topology = newTopology();
        
        TStream<String> stream = topology.source(new CloseSupplier());
        stream = stream.filter(new ClosePredicate()) ;
        stream = stream.modify(new CloseUnary()) ;
        stream = stream.multiTransform(new CloseMultiTransform()) ;
        stream = stream.last().aggregate(new CloseAggregate());
        stream.sink(new CloseConsumer());
        
        this.getTesterContext().submit(topology).get();
        
        assertTrue(CloseSupplier.seenClose.get());
        assertTrue(ClosePredicate.seenClose.get());
        assertTrue(CloseUnary.seenClose.get());
        assertTrue(CloseMultiTransform.seenClose.get());
        assertTrue(CloseAggregate.seenClose.get());
        assertTrue(CloseConsumer.seenClose.get());    
    }
    
    public static class CloseSupplier implements Supplier<Iterable<String>>, AutoCloseable {
        private static final long serialVersionUID = 1L;
        public final static AtomicBoolean seenClose = new AtomicBoolean();

        @Override
        public void close() {
            seenClose.set(true);          
        }

        @Override
        public Iterable<String> get() {
            return Collections.singleton("42!");
        }
    }
    public static class CloseConsumer implements Consumer<String>, AutoCloseable {
        private static final long serialVersionUID = 1L;
        public final static AtomicBoolean seenClose = new AtomicBoolean();

        @Override
        public void close() {
            seenClose.set(true);          
        }

        @Override
        public void accept(String v) {
        }      
    }
    public static class ClosePredicate implements Predicate<String>, AutoCloseable {
        private static final long serialVersionUID = 1L;
        public final static AtomicBoolean seenClose = new AtomicBoolean();

        @Override
        public void close() {
            seenClose.set(true);          
        }

        @Override
        public boolean test(String v) {
            return true;
        }      
    }
    public static class CloseUnary implements UnaryOperator<String>, AutoCloseable {
        private static final long serialVersionUID = 1L;
        public final static AtomicBoolean seenClose = new AtomicBoolean();

        @Override
        public void close() {
            seenClose.set(true);          
        }

        @Override
        public String apply(String v) {
            return v;
        }      
    }
    public static class CloseMultiTransform implements Function<String,Iterable<String>>, AutoCloseable {
        private static final long serialVersionUID = 1L;
        public final static AtomicBoolean seenClose = new AtomicBoolean();

        @Override
        public void close() {
            seenClose.set(true);          
        }

        @Override
        public Iterable<String> apply(String v) {
            return Collections.singleton(v);
        }      
    }
    public static class CloseAggregate implements Function<List<String>,String>, AutoCloseable {
        private static final long serialVersionUID = 1L;
        public final static AtomicBoolean seenClose = new AtomicBoolean();

        @Override
        public void close() {
            seenClose.set(true);          
        }

        @Override
        public String apply(List<String> v) {
            return v.get(0);
        }      
    }
}
