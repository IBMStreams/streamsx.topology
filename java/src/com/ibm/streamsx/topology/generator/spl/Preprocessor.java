/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.ArrayList;
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
        for (JsonObject composite : composites)
            pePlacementPreprocess.compositeColocateIdUse(composite);
    }

    /**
     * This method relocates {@code HashAdder} to the front of its parent
     * $Unparallel$ which enables cat's-cradle shuffle. There are four
     * scenarios.
     * <p>
     * Scenario 1 (optimized):
     * <pre><code>
     *     TStream<T> u = someStream.endParallel();
     *     TStream<T> p = u.parallel(()->3, Routing.HASH_PARTITIONED);
     * </code></pre>
     * <BR>
     * The code above demonstrates the simplest case where HashAdder is
     * $Unparallel$'s only child, and $Unparallel$ is HashAdder's only
     * parent:
     * <pre><code>
     *  $Unparallel$ -> HashAdder -> $Parallel$ -> HashRemover
     * </code></pre>
     * <BR>
     * To enable cat's-cradle shuffle, this method moves {@code HashAdder}
     * to the front of $Unparallel$.
     * </p>
     * <p>
     * Scenario 2 (not yet optimized):
     * <pre><code>
     *     TStream<T> u1 = stream1.endParallel();
     *     TStream<T> u2 = stream2.endParallel();
     *     TStream<T> d = stream3.sample(0.5);
     *     TStream<T> u = d.union(u1).union(u2);
     *     TStream<T> p = u.parallel(()->3, Routing.HASH_PARTITIONED);
     * </code></pre>
     * <BR>
     * This is slightly more complicated case where the {@code HashAdder}
     * has multiple parents and each parent has only one child.
     * <pre><code>
     *           $Unparallel1$
     *                |
     *                V
     *  sample -> HashAdder -> $Parallel$ -> HashRemover
     *                ^
     *                |
     *           $Unparallel2$
     *
     * </code></pre>
     * <BR>
     * To enable cat's-cradle shuffle, this method inserts one copy of
     * {@code HashAdder} to the front of every $Unparallel$, inserts one copy
     * of {@code HashAdder} after every non-parallel parent, and removes the
     * original {@code HashAdder}. The original graph will be modified to the
     * following structure.
     * <pre><code>
     *           HashAdder -> $Unparallel1$
     *                             |
     *                             V
     *  sample -> HashAdder -> $Parallel$ -> HashRemover
     *                             ^
     *                             |
     *           HashAdder -> $Unparallel2$
     * </code></pre>
     * </p>
     * <p>
     * Scenario 3 (optimized):
     * <pre><code>
     *     TStream<T> u = input.endParallel();
     *     TStream<T> p1 = u.parallel(()->3, keyer1);
     *     TStream<T> p2 = u.parallel(()->4, keyer2);
     *     TStream<T> f1 = u.filter((T x) -> true);
     *     TStream<T> f2 = u.filter((T x) -> false);
     * </code></pre>
     * In this scenario, one $Unparallel$ can have multiple children, but each
     * {@code HashAdder} has only one parent. The example code above results
     * in the following graph structure.
     * <pre><code>
     *           HashAdder1(p1) -> $Parallel1$ -> HashRemover1
     *               ^
     *               |         ----> filter1(f1)
     *               |        /
     * input -> $Unparallel$
     *               |       \
     *               |        ----> filter2(f2)
     *               V
     *           HashAdder2(p2) -> $Parallel2$ -> HashRemover2
     * </code></pre>
     * <BR>
     * To enable cat's-cradle shuffle, this method needs to move every
     * {@code HashAdder} upstream, and insert a copy of the original
     * $Unparallel$ after the {@code HashAdder}, resulting in the following
     * structure.
     * <pre><code>
     * HashAdder1(p1) -> $UnparallelCopy$ -> $Parallel1$ -> HashRemover1
     *     ^
     *     |               ----> filter1(f1)
     *     |              /
     *   input -> $Unparallel$
     *     |              \
     *     |               ----> filter2(f2)
     *     V
     * HashAdder2(p2) -> $UnparallelCopy$ -> $Parallel2$ -> HashRemover2
     * </code></pre>
     * <BR>
     * If there is no non-parallel children, the original $Unparallel$ should
     * be removed.
     * </p>
     * <p>
     * Scenario 4 (not yet optimized):
     * This scenario is a mix of Scenario 2 and 3, where $Unparallel$ can have
     * multiple children and {@code HashAdder} can have multiple parents.
     * </p>
     */
    private void relocateHashAdders(){
        final Set<JsonObject> hashAdderParents = new HashSet<>();
        // 1. find all HashAdders in the graph. The reason for not
        // moving HashAdders in this loop is to avoid modifying the graph
        // structure while traversing the graph.
        operators(graph, op -> {
            if (isHashAdder(op)) {
                Set<JsonObject> parents = GraphUtilities.getUpstream(op, graph);
                // Only consider HashAdders with exactly one parent and that
                // parent is an $Unparallel$, i.e., ignore scenarios #2 and #4.
                JsonObject parent = parents.iterator().next();
                if (parents.size() == 1 && END_PARALLEL.isThis(kind(parent))) {
                    hashAdderParents.add(parent);
                }
            }
        });

        // 2. process HashAdder's parents one by one
        for(JsonObject parent : hashAdderParents){
            relocateChildrenHashAdders(parent);
        }
    }

    /**
     * @param parent parent $Unparallel$ operator of {@code HashAdder}s, where
     *               the $Unparallel$ operator is the only parent of all its
     *               children {@code HashAdders}. If the $Unparallel$ operator
     *               has non-parallel children operators, those non-parallel
     *               children may have multiple parents.
     */
    private void relocateChildrenHashAdders(JsonObject parent){
        Set<JsonObject> children = GraphUtilities.getDownstream(parent, graph);
        if (children.size() == 1) {
            // Scenario #1: HashAdder is its parent $Unparallel$'s only child
            JsonObject hashAdder = children.iterator().next();
            // retrieve HashAdder's output port schema
            String schema = GraphUtilities.getOutputPortType(hashAdder, 0);
            // insert a copy of HashAdder to the front of Unparallel
            JsonObject hashAdderCopy = GraphUtilities.copyOperatorNewName(
                    hashAdder, jstring(hashAdder, "name"));
            GraphUtilities.removeOperator(hashAdder, graph);
            GraphUtilities.addBefore(parent, hashAdderCopy, graph);
            // set Unparallel's output port schema using HashAdder's schema
            GraphUtilities.setOutputPortType(parent, 0, schema);
            GraphUtilities.setInputPortType(parent, 0, schema);
        } else {
            // Scenario #3: the parent $Unparallel$ has multiple children

            // distinguish HashAdders from other children
            ArrayList<JsonObject> hashAdders = new ArrayList<>();
            ArrayList<JsonObject> others = new ArrayList<>();
            children.forEach(
                    (JsonObject parentChild) -> {
                        if (GraphUtilities.isHashAdder(parentChild)) {
                            hashAdders.add(parentChild);
                        } else {
                            others.add(parentChild);
                        }
                    }
            );

            // Please note that $Unparallel$ has at least one HashAdder child,
            // because it is discovered through a HashAdder.
            int nUnparallelCopy = 0;
            for (JsonObject hashAdder: hashAdders) {
                // The full transformation will first move the HashAdder upstream.
                // Then, a copy of the $Unparallel$ will be inserted after the
                // HashAdder.

                // Step 1. move the HashAdder upstream i.e.
                // input -> $Unparallel$ -> HashAdder -> $Parallel$
                // becomes
                // input -> HashAdder -> $Parallel$
                //      \-> $Unparallel$
                GraphUtilities.moveOperatorUpstream(hashAdder, graph);

                // Step 2. insert an $Unparallel$ after the HashAdder, i.e.,
                // input -> HashAdder -> $Parallel$
                //      \-> $Unparallel$
                // becomes
                // input -> HashAdder -> $UnparallelCopy$ -> $Parallel$
                //      \-> $Unparallel$

                // make a copy of original $Unparallel$
                JsonObject unparallelCopy = GraphUtilities.copyOperatorNewName(
                        parent,
                        jstring(parent, "name") + "_" + Integer.toString(nUnparallelCopy++));

                // retrieve $Parallel$
                Set<JsonObject> hashAdderChildren = GraphUtilities.getDownstream(hashAdder, graph);
                assert hashAdderChildren.size() == 1;
                JsonObject hashAdderChild = hashAdderChildren.iterator().next();
                // make sure $Unparallel$ output schema matches downstream
                // $Parallel$ input schema
                String schema = GraphUtilities.getOutputPortType(hashAdder, 0);
                GraphUtilities.setOutputPortType(unparallelCopy, 0, schema);
                GraphUtilities.setInputPortType(unparallelCopy, 0, schema);
                // insert the copy of $Unparallel$ after HashAdder
                GraphUtilities.addBetween(hashAdder, hashAdderChild, unparallelCopy);
                graph.get("operators").getAsJsonArray().add(unparallelCopy);
            }

            // The $Unparallel$ operator can be removed if it has zero
            // non-parallel child
            if (others.isEmpty()) {
                GraphUtilities.removeOperator(parent, graph);
            }
        }
    }
}
