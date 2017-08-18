package com.ibm.streamsx.topology.internal.test.handlers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.internal.tester.conditions.handlers.StringTupleTester;
import com.ibm.streamsx.topology.spl.SPLStream;

public class HandlerTesterSupport {
    private final Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers = new HashMap<>();
    
    /*
     * Graph declaration time.
     */

    /**
     * Just collect the stream handlers that may be used for testing of this
     * topoogy. What gets added to the graph depends on the context it is run
     * in.
     */
    private void addHandler(TStream<?> stream, StreamHandler<Tuple> handler) {
        Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);
        if (streamHandlers == null) {
            streamHandlers = new HashSet<StreamHandler<Tuple>>();
            handlers.put(stream, streamHandlers);
        }

        streamHandlers.add(handler);
    }
    
    @SuppressWarnings("unchecked")
    public void addSplHandler(Object stream, Object handler) {
        addHandler((SPLStream) stream, (StreamHandler<Tuple>) handler);
    }
    public void addStringTupleTester(TStream<String> stream, Predicate<String> tester) {
        addHandler(stream, new StringTupleTester(tester));
    }
}
