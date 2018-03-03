/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.END_LOW_LATENCY;
import static com.ibm.streamsx.topology.builder.BVirtualMarker.ISOLATE;
import static com.ibm.streamsx.topology.builder.BVirtualMarker.LOW_LATENCY;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.CONFIG;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_COLOCATE_KEY;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_EXPLICIT_COLOCATE_ID;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_ISOLATE_REGION_ID;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_LOW_LATENCY_REGION_ID;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.findOperatorByKind;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getUpstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.operators;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_COLOCATE_TAG_MAPPING;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_HAS_ISOLATE;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_HAS_LOW_LATENCY;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.function.Consumer;

class PEPlacement {
    
    private final SPLGenerator generator;
    private final JsonObject graph;

    
    private int isolateRegionCount;
    private int lowLatencyRegionCount;
    
    PEPlacement(SPLGenerator generator, JsonObject graph) {
        this.generator = generator;
        this.graph = graph;
    }
    
    private void setIsolateRegionId(JsonObject op, String isolationRegionId) {
       
        JsonObject placement = objectCreate(op, CONFIG, PLACEMENT);
        
        // As we create regions up and downstream of isolate
        // operators the same region can be seen twice,
        // e.g x -> isolate1 -> y -> isolate2 -> z
        // y is seen twice, once from isolate1 and once from isolate2
        if (jstring(placement, PLACEMENT_ISOLATE_REGION_ID) != null)
                return;
        
        placement.addProperty(PLACEMENT_ISOLATE_REGION_ID, isolationRegionId);
    }
    
    /**
     * Assign a region isolate identifier to all operators
     * in an isolate region. From the starts (which are immediately
     * up/downstream of an isolate set the isolate region id for
     * all reachable operators until another isolate or the edge is hit.
     * 
     * @param starts Set of operators upstream or downstream of an isolate marker.
     */
    private void assignIsolateRegionIds(Set<JsonObject> starts) {

        final String isolationRegionId = newIsolateRegionId();

        Set<BVirtualMarker> boundaries = EnumSet.of(BVirtualMarker.ISOLATE);

        GraphUtilities.visitOnce(starts, boundaries, graph,
                op -> setIsolateRegionId(op, isolationRegionId));
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
    private void checkValidColocationRegion(JsonObject isolate) {
        final Set<JsonObject> isolateChildren = getDownstream(isolate, graph);
        Set<JsonObject> isoParents = getUpstream(isolate, graph);

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

    void tagIsolationRegions() {
        // Check whether graph is valid for colocations
        Set<JsonObject> isolateOperators = findOperatorByKind(ISOLATE, graph);
        
        if (!isolateOperators.isEmpty())
            graph.getAsJsonObject("config").addProperty(CFG_HAS_ISOLATE, true);
        
        for (JsonObject jso : isolateOperators) {
            checkValidColocationRegion(jso);
        }

        // Assign isolation regions their partition colocations
        // by working upstream from the the isolate marker
        // and then downstream to separate the regions with
        // different isolate region identifiers.
        for (JsonObject isolate : isolateOperators) {
            assignIsolateRegionIds(getUpstream(isolate, graph));
            assignIsolateRegionIds(getDownstream(isolate, graph));
        }
 
        // For 4.2 and later we do not force colocation
        // on every operator, instead we allow submission
        // time fusion to figure out the best plan.
        if (!generator.versionAtLeast(4, 2))
            tagIslandIsolatedRegions();
        GraphUtilities.removeOperators(isolateOperators, graph);
    }
    
    /**
     * Tag any "island" regions with their own isolated region id.
     * This can occur when there are there sub-graphs that are
     * not connected to a region already processed with an isolate.
     * So two cases:
     *   a) No isolates exist at all in the graph
     *   b) Isolates exist in the whole graph but a disconnected
     *   sub-graph has no isolates. 
     */
    private void tagIslandIsolatedRegions(){
        Set<JsonObject> starts = GraphUtilities.findStarts(graph);   
        
        for(JsonObject start : starts){
            final String colocationTag = newIsolateRegionId();
            
            JsonObject placement = objectCreate(start, CONFIG, PLACEMENT);
                     
            String regionTag = jstring(placement, PLACEMENT_ISOLATE_REGION_ID);         
            if (regionTag != null && !regionTag.isEmpty()) {
                continue;
            }
            
            Set<JsonObject> startList = Collections.singleton(start);
            
            Set<BVirtualMarker> boundaries = EnumSet.of(BVirtualMarker.ISOLATE);
            
            GraphUtilities.visitOnce(startList, boundaries, graph,
                    op -> setIsolateRegionId(op, colocationTag));          
        }
    }
    
    private String newIsolateRegionId() {
        return "__spl_isolateRegionId" + isolateRegionCount++;
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
    
    void tagLowLatencyRegions() {
        Set<JsonObject> lowLatencyStartOperators = GraphUtilities
                .findOperatorByKind(LOW_LATENCY, graph);
        
        if (lowLatencyStartOperators.isEmpty())
            return;
        
        graph.getAsJsonObject("config").addProperty(CFG_HAS_LOW_LATENCY, true);
        
        // Assign isolation regions their lowLatency tag
        for (JsonObject llStart : lowLatencyStartOperators) {
            assignLowLatency(llStart);
        }

        // Remove all the markers
        lowLatencyStartOperators.addAll(findOperatorByKind(END_LOW_LATENCY, graph));
        GraphUtilities.removeOperators(lowLatencyStartOperators, graph);
    }

    @SuppressWarnings("serial")
    private void assignLowLatency(JsonObject llStart) {
        
        final String lowLatencyTag = "__spl_lowLatencyRegionId" + lowLatencyRegionCount++;

        Set<JsonObject> llStartChildren = getDownstream(llStart, graph);
        
        // Determine if the region has already been tagged, which would happen if there are
        // multiple starts to the low latency region.
        Boolean[] isAlreadyTagged = {false};
        llStartChildren.forEach(oper -> {
            JsonObject placement = object(oper, CONFIG, PLACEMENT);
            if(placement!=null && jstring(placement, PLACEMENT_LOW_LATENCY_REGION_ID) != null)
                isAlreadyTagged[0]=true;
                
        });
        
        if(isAlreadyTagged[0])
            return;
        
        Set<BVirtualMarker> boundaries = EnumSet.of(LOW_LATENCY, END_LOW_LATENCY);

        GraphUtilities.visitOnce(llStartChildren, boundaries, graph,
                new Consumer<JsonObject>() {
                    @Override
                    public void accept(JsonObject op) {
                        // Add a manual threading annotation
                        // to ensure low latency by not allowing
                        // any scheduled ports to be added
                        //JsonObject threading = new JsonObject();
                        //threading.addProperty("model", "manual");
                        // op.add("threading", threading);
                                            
                        // If the region has already been assigned a
                        // lowLatency tag, simply return.
                        JsonObject placement = objectCreate(op, CONFIG, PLACEMENT);
                        
                        assert jstring(placement, PLACEMENT_LOW_LATENCY_REGION_ID) == null
                                || jstring(placement, PLACEMENT_LOW_LATENCY_REGION_ID).equals(lowLatencyTag);
                        placement.addProperty(PLACEMENT_LOW_LATENCY_REGION_ID, lowLatencyTag);
                    }
                });

    }
    
    /**
     * Goes through the graph and looks to merge all colocation tags to a
     * single value for the set of colocated operators.
     * This resolves the mutliple potential colocate tags
     * through explicit colocation, isolation and low-latency.
     * Each operator with a colocate directive is left with
     * a single key to a map of tags in the graph config.
     * The value in the map is the actual tag used in SPL
     * placement config for the operator.
     */
    void resolveColocationTags() {
        
        JsonObject tagMaps = objectCreate(graph, CONFIG, CFG_COLOCATE_TAG_MAPPING);
        
        operators(graph, op -> {
            JsonObject placement = object(op, CONFIG, PLACEMENT);
            if (placement == null)
                return;
            
            // Three types of co-locate.
            String explicit = jstring(placement, PLACEMENT_EXPLICIT_COLOCATE_ID);
            String lowLatency = jstring(placement, PLACEMENT_LOW_LATENCY_REGION_ID);
            String isolate = jstring(placement, PLACEMENT_ISOLATE_REGION_ID);
            
            Set<String> sameTags = new HashSet<>();
            if (explicit != null)
                sameTags.add(explicit);
            if (lowLatency != null)
                sameTags.add(lowLatency);
            if (isolate != null)
                sameTags.add(isolate);
            if (sameTags.isEmpty())
                return;
            
            // Find if any of these tags are already mapped.
            Set<String> existingTags = new HashSet<>();
            for (String tag : sameTags) {
                if (tagMaps.has(tag)) {
                    existingTags.add(jstring(tagMaps, tag));
                }
            }
            final String singleTag;
            if (existingTags.isEmpty()) {
                // pick any one of the existing ones
                // none of which have been seen yet.
                singleTag = sameTags.iterator().next();
            } else if (existingTags.size() == 1) {
                // single existing tag, use it
                singleTag = existingTags.iterator().next();
            } else {
                // Need to merge tags
                // e.g. A->A, B->B were separate but now we need to merge A,B
                // pick one tag as single
                singleTag = existingTags.iterator().next();
                sameTags.addAll(existingTags);
            }           

            for (String tag : sameTags)
                tagMaps.addProperty(tag, singleTag);
            
            // finally add the single tag key to the operator
            placement.addProperty(PLACEMENT_COLOCATE_KEY, singleTag);
        });
        
        object(graph, CONFIG).add(CFG_COLOCATE_TAG_MAPPING, tagMaps);
    }
}
