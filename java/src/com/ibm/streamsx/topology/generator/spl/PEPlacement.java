/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.generator.spl.GsonUtilities.nestedObjectCreate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.builder.JOperator;
import com.ibm.streamsx.topology.builder.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.function.Consumer;

class PEPlacement {
    
    private int isolateRegionCount;
    private int lowLatencyRegionCount;
    
    private void setIsolateRegionId(JsonObject op, String isolationRegionId) {
       
        JsonObject placement = nestedObjectCreate(op, JOperator.CONFIG, JOperatorConfig.PLACEMENT);

        // If the region has already been assigned a PLACEMENT_ISOLATE_REGION_ID
        // tag, simply return.
        String id = jstring(placement, JOperator.PLACEMENT_ISOLATE_REGION_ID);
        if (id != null && !id.isEmpty()) {
            return;
        }
        
        placement.addProperty(JOperator.PLACEMENT_ISOLATE_REGION_ID, isolationRegionId);
    }
    
    @SuppressWarnings("serial")
    private void assignIsolateRegionIds(JsonObject isolate, Set<JsonObject> starts,
            JsonObject graph) {

        final String isolationRegionId = newIsolateRegionId();

        Set<BVirtualMarker> boundaries = EnumSet.of(BVirtualMarker.ISOLATE);

        GraphUtilities.visitOnce(starts, boundaries, graph,
                new Consumer<JsonObject>() {

                    @Override
                    public void accept(JsonObject op) {
                        setIsolateRegionId(op, isolationRegionId);
                    }

                });
    }

    /**
     * Determine whether any isolated region is ever joined with its parent.
     * I.E:
     * 
     * <pre>
     * <code>
     *       |---$Isolate---|
     *   ----|              |----
     *       |--------------|
     * </code>
     * </pre>
     * 
     * @param isolate
     *            An $Isolate$ operator in the graph
     * @return a boolean which is false if the the Isolated region is later
     *         merged with its parent.
     */
    @SuppressWarnings("serial")
    private void checkValidColocationRegion(JsonObject isolate, JsonObject graph) {
        final Set<JsonObject> isolateChildren = GraphUtilities.getDownstream(
                isolate, graph);
        Set<JsonObject> isoParents = GraphUtilities.getUpstream(isolate, graph);

        assertNotIsolated(isoParents);

        Set<BVirtualMarker> boundaries = EnumSet.of(BVirtualMarker.ISOLATE);

        GraphUtilities.visitOnce(isoParents, boundaries, graph,
                new Consumer<JsonObject>() {
                    @Override
                    public void accept(JsonObject op) {
                        if (isolateChildren.contains(op)) {
                            throw new IllegalStateException(
                                    "Invalid isolation "
                                            + "configuration. An isolated region is joined with a non-"
                                            + "isolated region.");
                        }
                    }
                });
    }

    void tagIsolationRegions(JsonObject graph) {
        // Check whether graph is valid for colocations
        Set<JsonObject> isolateOperators = GraphUtilities.findOperatorByKind(
                BVirtualMarker.ISOLATE, graph);
        
        for (JsonObject jso : isolateOperators) {
            checkValidColocationRegion(jso, graph);
        }

        // Assign isolation regions their partition colocations
        for (JsonObject isolate : isolateOperators) {
            assignIsolateRegionIds(isolate,
                    GraphUtilities.getUpstream(isolate, graph), graph);
            assignIsolateRegionIds(isolate,
                    GraphUtilities.getDownstream(isolate, graph), graph);
        }
 
        tagIslandIsolatedRegions(graph);
        GraphUtilities.removeOperators(isolateOperators, graph);
    }
    
    @SuppressWarnings("serial")
    private void tagIslandIsolatedRegions(JsonObject graph){
        Set<JsonObject> starts = GraphUtilities.findStarts(graph);   
        
        for(JsonObject start : starts){
            final String colocationTag = newIsolateRegionId();
            
            JsonObject placement = nestedObjectCreate(start, JOperator.CONFIG, JOperatorConfig.PLACEMENT);
                     
            String regionTag = jstring(placement, JOperator.PLACEMENT_ISOLATE_REGION_ID);         
            if (regionTag != null && !regionTag.isEmpty()) {
                continue;
            }
            
            Set<JsonObject> startList = Collections.singleton(start);
            
            Set<BVirtualMarker> boundaries = EnumSet.of(BVirtualMarker.ISOLATE);
            
            GraphUtilities.visitOnce(startList, boundaries, graph,
                    new Consumer<JsonObject>() {
                        @Override
                        public void accept(JsonObject op) {
                            setIsolateRegionId(op, colocationTag);
                        }
                    });           
        }
    }
    
    private String newIsolateRegionId() {
        return "__jaa_isolateId" + isolateRegionCount++;
    }

    private static void assertNotIsolated(Collection<JsonObject> jsos) {
        for (JsonObject jso : jsos) {
            if (BVirtualMarker.ISOLATE.isThis(jstring(jso, "kind"))) {
                throw new IllegalStateException(
                        "Cannot put \"isolate\" regions immediately"
                                + " adjacent to each other. E.g -- .isolate().isolate()");
            }
        }
    }
    
    void tagLowLatencyRegions(JsonObject graph) {
        Set<JsonObject> lowLatencyStartOperators = GraphUtilities
                .findOperatorByKind(BVirtualMarker.LOW_LATENCY, graph);
        Set<JsonObject> lowLatencyEndOperators = GraphUtilities
                .findOperatorByKind(BVirtualMarker.END_LOW_LATENCY, graph);

        // Assign isolation regions their lowLatency tag
        for (JsonObject llStart : lowLatencyStartOperators) {
            assignLowLatency(llStart,
                    GraphUtilities.getDownstream(llStart, graph), graph);
        }

        List<JsonObject> allLowLatencyOps = new ArrayList<>();
        allLowLatencyOps.addAll(lowLatencyEndOperators);
        allLowLatencyOps.addAll(lowLatencyStartOperators);

        GraphUtilities.removeOperators(allLowLatencyOps, graph);
    }

    @SuppressWarnings("serial")
    private void assignLowLatency(JsonObject llStart,
            Set<JsonObject> llStartChildren, JsonObject graph) {

        final String lowLatencyTag = "LowLatencyRegion"
                + Integer.toString(lowLatencyRegionCount++);

        Set<BVirtualMarker> boundaries =
                EnumSet.of(BVirtualMarker.LOW_LATENCY, BVirtualMarker.END_LOW_LATENCY);

        GraphUtilities.visitOnce(llStartChildren, boundaries, graph,
                new Consumer<JsonObject>() {
                    @Override
                    public void accept(JsonObject op) {
                        // If the region has already been assigned a
                        // lowLatency tag, simply return.
                        JsonObject placement = nestedObjectCreate(op, JOperator.CONFIG, JOperatorConfig.PLACEMENT);
                        String regionTag = jstring(placement, JOperator.PLACEMENT_LOW_LATENCY_REGION_ID);
                        if (regionTag != null && !regionTag.isEmpty()) {
                            return;
                        }
                        placement.addProperty(JOperator.PLACEMENT_LOW_LATENCY_REGION_ID, lowLatencyTag);
                    }
                });

    }


}
