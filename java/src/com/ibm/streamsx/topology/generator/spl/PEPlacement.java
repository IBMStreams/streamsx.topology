/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.builder.JOperator;
import com.ibm.streamsx.topology.builder.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.function.Consumer;

class PEPlacement {
    
    private int isolateRegionCount;
    private int lowLatencyRegionCount;
    
    private void setIsolateRegionId(JSONObject op, String isolationRegionId) {
       
        JSONObject placement = JOperatorConfig.createJSONItem(op, JOperatorConfig.PLACEMENT);

        // If the region has already been assigned a PLACEMENT_ISOLATE_REGION_ID
        // tag, simply return.
        String id = (String) placement.get(JOperator.PLACEMENT_ISOLATE_REGION_ID);
        if (id != null && !id.isEmpty()) {
            return;
        }
        
        placement.put(JOperator.PLACEMENT_ISOLATE_REGION_ID, isolationRegionId);
    }
    
    @SuppressWarnings("serial")
    private void assignIsolateRegionIds(JSONObject isolate, List<JSONObject> starts,
            JSONObject graph) {

        final String isolationRegionId = newIsolateRegionId();

        Set<BVirtualMarker> boundaries = EnumSet.of(BVirtualMarker.ISOLATE);

        GraphUtilities.visitOnce(starts, boundaries, graph,
                new Consumer<JSONObject>() {

                    @Override
                    public void accept(JSONObject op) {
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
    private void checkValidColocationRegion(JSONObject isolate, JSONObject graph) {
        final List<JSONObject> isolateChildren = GraphUtilities.getDownstream(
                isolate, graph);
        List<JSONObject> isoParents = GraphUtilities.getUpstream(isolate, graph);

        assertNotIsolated(isoParents);

        Set<BVirtualMarker> boundaries = EnumSet.of(BVirtualMarker.ISOLATE);

        GraphUtilities.visitOnce(isoParents, boundaries, graph,
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject op) {
                        if (isolateChildren.contains(op)) {
                            throw new IllegalStateException(
                                    "Invalid isolation "
                                            + "configuration. An isolated region is joined with a non-"
                                            + "isolated region.");
                        }
                    }
                });
    }

    void tagIsolationRegions(JSONObject graph) {
        // Check whether graph is valid for colocations
        List<JSONObject> isolateOperators = GraphUtilities.findOperatorByKind(
                BVirtualMarker.ISOLATE, graph);
        
        for (JSONObject jso : isolateOperators) {
            checkValidColocationRegion(jso, graph);
        }

        // Assign isolation regions their partition colocations
        for (JSONObject isolate : isolateOperators) {
            assignIsolateRegionIds(isolate,
                    GraphUtilities.getUpstream(isolate, graph), graph);
            assignIsolateRegionIds(isolate,
                    GraphUtilities.getDownstream(isolate, graph), graph);
        }
 
        tagIslandIsolatedRegions(graph);
        GraphUtilities.removeOperators(isolateOperators, graph);
    }
    
    @SuppressWarnings("serial")
    private void tagIslandIsolatedRegions(JSONObject graph){
        List<JSONObject> starts = GraphUtilities.findStarts(graph);   
        
        for(JSONObject start : starts){
            final String colocationTag = newIsolateRegionId();
            
            JSONObject placement = JOperatorConfig.createJSONItem(start, JOperatorConfig.PLACEMENT);
                     
            String regionTag = (String) placement.get(JOperator.PLACEMENT_ISOLATE_REGION_ID);         
            if (regionTag != null && !regionTag.isEmpty()) {
                continue;
            }
            
            List<JSONObject> startList = new ArrayList<JSONObject>();
            startList.add(start);
            
            Set<BVirtualMarker> boundaries = EnumSet.of(BVirtualMarker.ISOLATE);
            
            GraphUtilities.visitOnce(startList, boundaries, graph,
                    new Consumer<JSONObject>() {
                        @Override
                        public void accept(JSONObject op) {
                            setIsolateRegionId(op, colocationTag);
                        }
                    });           
        }
    }
    
    private String newIsolateRegionId() {
        return "__jaa_isolateId" + isolateRegionCount++;
    }

    private static void assertNotIsolated(Collection<JSONObject> jsos) {
        for (JSONObject jso : jsos) {
            if (BVirtualMarker.ISOLATE.isThis((String) jso.get("kind"))) {
                throw new IllegalStateException(
                        "Cannot put \"isolate\" regions immediately"
                                + " adjacent to each other. E.g -- .isolate().isolate()");
            }
        }
    }
    
    void tagLowLatencyRegions(JSONObject graph) {
        List<JSONObject> lowLatencyStartOperators = GraphUtilities
                .findOperatorByKind(BVirtualMarker.LOW_LATENCY, graph);
        List<JSONObject> lowLatencyEndOperators = GraphUtilities
                .findOperatorByKind(BVirtualMarker.END_LOW_LATENCY, graph);

        // Assign isolation regions their lowLatency tag
        for (JSONObject llStart : lowLatencyStartOperators) {
            assignLowLatency(llStart,
                    GraphUtilities.getDownstream(llStart, graph), graph);
        }

        List<JSONObject> allLowLatencyOps = new ArrayList<>();
        allLowLatencyOps.addAll(lowLatencyEndOperators);
        allLowLatencyOps.addAll(lowLatencyStartOperators);

        GraphUtilities.removeOperators(allLowLatencyOps, graph);
    }

    @SuppressWarnings("serial")
    private void assignLowLatency(JSONObject llStart,
            List<JSONObject> llStartChildren, JSONObject graph) {

        final String lowLatencyTag = "LowLatencyRegion"
                + Integer.toString(lowLatencyRegionCount++);

        Set<BVirtualMarker> boundaries =
                EnumSet.of(BVirtualMarker.LOW_LATENCY, BVirtualMarker.END_LOW_LATENCY);

        GraphUtilities.visitOnce(llStartChildren, boundaries, graph,
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject op) {
                        // If the region has already been assigned a
                        // lowLatency tag, simply return.
                        JSONObject placement = JOperatorConfig.createJSONItem(op, JOperatorConfig.PLACEMENT);
                        String regionTag = (String) placement.get(JOperator.PLACEMENT_LOW_LATENCY_REGION_ID);
                        if (regionTag != null && !regionTag.isEmpty()) {
                            return;
                        }
                        placement.put(JOperator.PLACEMENT_LOW_LATENCY_REGION_ID, lowLatencyTag);
                    }
                });

    }


}
