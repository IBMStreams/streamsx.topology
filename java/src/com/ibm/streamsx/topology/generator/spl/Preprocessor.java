/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.END_PARALLEL;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.isHashAdder;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.kind;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.operators;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

/**
 * Preprocessor modifies the passed in JSON to perform
 * logical graph transformations.
 */
class Preprocessor {
    
    private final SPLGenerator generator;
    private final JsonObject graph;
    private final PEPlacement pePlacementPreprocess;
    
    Preprocessor(SPLGenerator generator, JsonObject graph) {
        this.generator = generator;
        this.graph = graph;
        pePlacementPreprocess = new PEPlacement(this.generator, graph);
    }
    
    Preprocessor preprocess() {
        
        GraphValidation graphValidationProcess = new GraphValidation();
        graphValidationProcess.validateGraph(graph);

        // The hash adder operators need to be relocated to enable directly
        // adjacent parallel regions
        relocateHashAdders();
        
        pePlacementPreprocess.tagIsolationRegions();
        pePlacementPreprocess.tagLowLatencyRegions();

        
        ThreadingModel.preProcessThreadedPorts(graph);
        
        removeRemainingVirtualMarkers();
        
        AutonomousRegions.preprocessAutonomousRegions(graph);
        
        pePlacementPreprocess.resolveColocationTags();

        // Optimize phase.
        new Optimizer(graph).optimize();
        
        return this;
    }
    
    private void removeRemainingVirtualMarkers(){
        for (BVirtualMarker marker : Arrays.asList(BVirtualMarker.UNION, BVirtualMarker.PENDING)) {
            List<JsonObject> unionOps = GraphUtilities.findOperatorByKind(marker, graph);
            GraphUtilities.removeOperators(unionOps, graph);
        }
    }

    public void compositeColocateIdUsage(List<JsonObject> composites) {
        if (composites.size() < 2)
            return;
        for (JsonObject composite : composites)
            pePlacementPreprocess.compositeColocateIdUse(composite);
    }

    private void relocateHashAdders(){
        final Set<JsonObject> hashAdders = new HashSet<>();
        // First, find all HashAdders in the graph. The reason for not
        // moving HashAdders in this loop is to avoid modifying the graph
        // structure while traversing the graph.
        operators(graph, op -> {
            if (isHashAdder(op))
                hashAdders.add(op);
        });

        // Second, relocate HashAdders one by one.
        for(JsonObject hashAdder : hashAdders){
            relocateHashAdder(hashAdder);
        }
    }

    /**
     * This method relocates {@code HashAdder} to directly connect $Unparallel$
     * to $Parallel$ operators which enables cat's-cradle shuffle. There could
     * be four scenarios
     * <p>
     * Scenario 1 (supported):
     * <pre><code>
     *     TStream<T> u = someStream.endParallel();
     *     TStream<T> p = u.parallel(()->3, Routing.HASH_PARTITIONED);
     * </code></pre>
     * <BR>
     * The code above demonstrates the simplest case where HashAdder is
     * $Unparellel$'s only child, and $Unparallel$ is HashAdder's only
     * parent:
     * <pre><code>
     *  $Unparallel$ -> HashAdder -> $Parallel$ -> HashRemover
     * </code></pre>
     * <BR>
     * To enable cat's-cradle shuffle, this method moves {@code HashAdder}
     * to the front of $Unparellel$.
     * </p>
     * <p>
     * Scenario 2 (not yet supported):
     * <pre><code>
     *     TStream<T> u1 = stream1.endParallel();
     *     TStream<T> u2 = stream2.endParallel();
     *     TStream<T> d = stream3.sample(0.5);
     *     TStream<T> u = u1.union(u2);
     *     TStream<T> p = u.parallel(()->3, Routing.HASH_PARTITIONED);
     * </code></pre>
     * <BR>
     * This is slightly more complicated case where the {@code HashAdder}
     * has multiple parents and each parent has only one child.
     * <pre><code>
     *           $Unparellel1$
     *                |
     *                V
     *  sample -> HashAdder -> $Parallel$ -> HashRemover
     *                ^
     *                |
     *           $Unparellel2$
     *
     * </code></pre>
     * <BR>
     * To enable cat's-cradle shuffle, this method inserts one copy of
     * {@code HashAdder} to the front of every $Unparellel$, inserts one copy
     * of {@code HashAdder} after every non-parallel parent, and removes the
     * original {@code HashAdder}. The original graph will be modified to the
     * following structure.
     * <pre><code>
     *           HashAdder -> $Unparellel1$
     *                             |
     *                             V
     *  sample -> HashAdder -> $Parallel$ -> HashRemover
     *                             ^
     *                             |
     *           HashAdder -> $Unparellel2$
     * </code></pre>
     * </p>
     * <p>
     * Scenario 3 (supported):
     * <pre><code>
     *     TStream<T> u = input.endParallel();
     *     TStream<T> p1 = u.parallel(()->3, keyer1);
     *     TStream<T> p2 = u.parallel(()->4, keyer2);
     *     TStream<T> f1 = u.filter((T x) -> true);
     *     TStream<T> f2 = u.filter((T x) -> false);
     * </code></pre>
     * In this scenario, one $Unparallel can have multiple children, but each
     * {@code HashAdder} has only one parent. The example code above results
     * in the following graph structure.
     * <pre><code>
     *           HashAdder1 -> $Parellel1$ -> HashRemover1
     *               ^
     *               |         ----> filter1
     *               |        /
     * input -> $Unparallel$
     *               |       \
     *               |        ----> filter 2
     *               V
     *           HashAdder2 -> $Parellel2$ -> HashRemover2
     * </code></pre>
     * <BR>
     * To enable cat's-cradle shuffle, this method needs to move all
     * HashAdders to the front of $Unparallel$. Besides, as the two
     * non-parallel streams should not consume data from HashAdder, this
     * method needs to insert a PassThrough operator before $Unparallel$
     * as well, resulting in the following structure.
     * <pre><code>
     * HashAdder1 -> $Unparallel$ -> $Parellel1$ -> HashRemover1
     *     ^
     *     |                            ----> filter1
     *     |                           /
     *   input -> PassThrough -> $Unparallel$
     *     |                          \
     *     |                           ----> filter 2
     *     V
     * HashAdder2 -> $Unparallel$ -> $Parellel2$ -> HashRemover2
     * </code></pre>
     * <BR>
     * The {@code PassThrough} operator is only necessary when there are more
     * than one downstream non-parallel children. If there is only one
     * non-parallel children, the $Unparallel$ operator can be directly linked
     * with {@code input}.
     * </p>
     * <p>
     * Scenario 4 (not yet supported):
     * This scenario is a mix of Scenario 2 and 3, where $Unparallel$ can have
     * multiple children and {@code HashAdder} can have multiple parents.
     * </p>
     *
     * @param hashAdder the target HashAdder operator to be relocated
     */
    private void relocateHashAdder(JsonObject hashAdder){
        // Only optimize the case where $Unparallel$ is the HashAdder's only
        // parent and the HashAdder is $Unparallel$'s only child.
        // $Unparallel$ -> hashAdder -> $Parallel$ -> hashRemover
        Set<JsonObject> parents = GraphUtilities.getUpstream(hashAdder, graph);
        
        // check if hashAdder has only one parent
        if (parents.size() != 1) return;

        JsonObject parent = parents.iterator().next();
        // check if HashAdder's parent is $Unparallel$, and $Unparallel$
        // has only one child
        if (END_PARALLEL.isThis(kind(parent)) &&
                GraphUtilities.getDownstream(parent, graph).size() == 1) {
            // retrieve HashAdder's output port schema
            String schema = GraphUtilities.getOutputPortType(hashAdder, 0);
            // insert a copy of HashAdder to the front of Unparallel
            JsonObject hashAdderCopy = GraphUtilities.copyOperatorNewName(
                    hashAdder, jstring(hashAdder, "name"));
            GraphUtilities.removeOperator(hashAdder, graph);
            GraphUtilities.addBefore(parent, hashAdderCopy, graph);
            // set Unparallel's output port schema using HashAdder's schema
            GraphUtilities.setOutputPortType(parent, 0, schema);
        }
    }
}
