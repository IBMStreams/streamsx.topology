package com.ibm.streamsx.topology.internal.tester.embedded;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.handlers.HandlerCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.handlers.HandlerTesterRuntime;

/**
 * Embedded tester that takes the conditions and
 * adds corresponding handlers directly into the
 * application graph.
 *
 */
public class EmbeddedTesterRuntime extends HandlerTesterRuntime {
    
    public EmbeddedTesterRuntime(TupleCollection tester) {
        super(tester);
    }

    @Override
    public void start(Object info) throws Exception {
        JavaTestableGraph tg = (JavaTestableGraph) info;
        setupEmbeddedTestHandlers(tg);
    }

    @Override
    public void shutdown() throws Exception {
    }
    
    private void setupEmbeddedTestHandlers(JavaTestableGraph tg) throws Exception {

        for (TStream<?> stream : handlers.keySet()) {
            Set<StreamHandler<Tuple>> streamHandlers = handlers.get(stream);

            final BOutput output = stream.output();
            if (output instanceof BOutputPort) {
                final BOutputPort outputPort = (BOutputPort) output;
                final OutputPortDeclaration portDecl = outputPort.port();
                for (StreamHandler<Tuple> streamHandler : streamHandlers) {
                    tg.registerStreamHandler(portDecl, streamHandler);
                }
            }
        }
    }

}
