/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.internal.functional.ops.FunctionFunctor.FUNCTIONAL_LOGIC_PARAM;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.streams.operator.Operator;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.internal.functional.ObjectUtils;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.internal.spljava.Schemas;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;

/**
 * Maintains the core core for building a topology of Java streams.
 * 
 */
public class JavaFunctional {

    /**
     * Add an operator that executes a Java function (as an instance of a class)
     * for its logic.
     */
    public static BOperatorInvocation addFunctionalOperator(TopologyElement te,
            Class<? extends Operator> opClass, Serializable logic) {

        verifySerializable(logic);
        String logicString = ObjectUtils.serializeLogic(logic);
        BOperatorInvocation bop = te.builder().addOperator(opClass,
                Collections.singletonMap(FUNCTIONAL_LOGIC_PARAM, logicString));

        addDependency(te, bop, logic);

        return bop;
    }
    public static BOperatorInvocation addFunctionalOperator(TopologyElement te,
            String name, Class<? extends Operator> opClass, Serializable logic) {

        return addFunctionalOperator(te, name, opClass, logic, null);
    }
    public static BOperatorInvocation addFunctionalOperator(TopologyElement te,
            String name, Class<? extends Operator> opClass, Serializable logic,
            Map<String,Object> params) {
        if (params == null)
            params = new HashMap<>();

        verifySerializable(logic);
        String logicString = ObjectUtils.serializeLogic(logic);        
        params.put(FUNCTIONAL_LOGIC_PARAM, logicString);
        BOperatorInvocation bop = te.builder().addOperator(name, opClass,
                params);

        addDependency(te, bop, logic);

        return bop;
    }
    
    private static final Set<Class<?>> VIEWABLE_TYPES = new HashSet<>();
    static {
        VIEWABLE_TYPES.add(Tuple.class);
        VIEWABLE_TYPES.add(String.class);
    }
    

    /**
     * Add an output port to an operator that uses a Java class as its object
     * type.
     */
    public static <T> TStream<T> addJavaOutput(TopologyElement te,
            BOperatorInvocation bop, Class<T> tupleTypeClass) {
        SPLMapping<T> mapping = Schemas.getSPLMapping(tupleTypeClass);
        BOutputPort bstream = bop.addOutput(mapping.getSchema());
        addDependency(te, bop, tupleTypeClass);
        
        // If the stream is just a Java object as a blob
        // then don't allow them to be viewed.
        if (!VIEWABLE_TYPES.contains(tupleTypeClass)) {
            bop.addConfig("streamViewability", false);
        }

        return new StreamImpl<T>(te, bstream, tupleTypeClass);
    }

    public static <T> TStream<T> addSubscribeOperator(TopologyElement te,
            String topic, Class<T> tupleTypeClass) {
        SPLMapping<T> mapping = Schemas.getSPLMapping(tupleTypeClass);
        @SuppressWarnings("unused")
        SPLStream splImport = SPLStreams.subscribe(te, topic,
                mapping.getSchema());
        return null; // TODO
        /*
         * return new OutputPortStream<T>(bop, te, splImport.getPort(),
         * tupleTypeClass); return GraphUtils.portToStream(this,
         * splImport.getPort(), tupleTypeClass);
         */

    }

    /**
     * Connect a Java functional operator to output from an output creating an
     * input port if required. Ensures the operator has its dependencies on the
     * tuple type added.
     */
    public static BInputPort connectTo(TopologyElement te, BOutput output,
            Class<?> tupleTypeClass, BOperatorInvocation receivingBop,
            BInputPort input) {

        addDependency(te, receivingBop, tupleTypeClass);
        return receivingBop.inputFrom(output, input);
    }
    
    /**
     * Add a third-party dependency to all eligible operators
     */
    public static void addJarDependency(TopologyElement te,
            String location) {
        te.topology().getDependencyResolver().addJarDependency(location);
    }
    
    /**
     * Add a third-party dependency to all eligible operators.
     */
    public static void addClassDependency(TopologyElement te,
            Class<?> clazz) {
        te.topology().getDependencyResolver().addClassDependency(clazz);
    }

    /**
     * Add a dependency for the operator to a Java tuple type.
     */
    private static void addDependency(TopologyElement te,
            BOperatorInvocation bop, Class<?> clazz) {
        if (Tuple.class.equals(clazz))
            return;
        te.topology().getDependencyResolver().addJarDependency(bop, clazz);
    }

    /**
     * Add a dependency for the operator to its functional logic.
     */
    private static void addDependency(TopologyElement te,
            BOperatorInvocation bop, Object function) {
        te.topology().getDependencyResolver().addJarDependency(bop, function);
    }
    
    /**
     * Simple check of the fields in the serializable logic
     * to ensure that all non-transient field are serializable. 
     * @param logic
     */
    private static void verifySerializable(Serializable logic) {
        
        final Class<?> logicClass = logic.getClass();
        
        for (Field f : logicClass.getDeclaredFields()) {
            final int modifiers = f.getModifiers();
            if (Modifier.isStatic(modifiers))
                continue;
            if (Modifier.isTransient(modifiers))
                continue;
            
            // We can't check for regular fields as the
            // declaration might be valid as Object, but only
            // contain serializable objects at runtime.
            if (!f.isSynthetic())
                continue;
            
            Class<?> fieldClass = f.getType();
            if (fieldClass.isPrimitive())
                continue;
                        
            if (!Serializable.class.isAssignableFrom(fieldClass)) {
                throw new IllegalArgumentException(
                        "Functional logic argument " + logic + " contains a non-serializable field:"
                                + f.getName() + " ,"
                                + "ensure anonymous classes are declared in a static context.");
            }           
        }
    }
}
