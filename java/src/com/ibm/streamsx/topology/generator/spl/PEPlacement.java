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
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_COLOCATE_TAGS;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_ISOLATE_REGION_ID;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.addColocationTag;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.findOperatorByKind;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getUpstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.kind;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.operators;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_COLOCATE_IDS;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_COLOCATE_TAG_MAPPING;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_HAS_ISOLATE;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities.Direction;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities.VisitController;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

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
        List<JsonObject> isolateOperators = findOperatorByKind(ISOLATE, graph);
        
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
 
        GraphUtilities.removeOperators(isolateOperators, graph);
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
    
    /**
     * We colocate between upon LOW_LATENCY and END_LOW_LATENCY markers.
     * Given they can be nested we take the simple approch
     * of work down from LOW_LATENCY and then up from LOW_LATENCY.
     * 
     * In addition assignLowLatency colocates operators immediately upstream
     * of LOW_LATENCY as defined by the lowLatency() methods.
     */
    void tagLowLatencyRegions() {
        List<JsonObject> lowLatencyStartOperators = GraphUtilities
                .findOperatorByKind(LOW_LATENCY, graph);
        
        if (lowLatencyStartOperators.isEmpty())
            return;
        
        // Assign isolation regions a colocation tag
        for (JsonObject llStart : lowLatencyStartOperators) {
            assignLowLatency(llStart, Direction.DOWNSTREAM);
        }
        
        List<JsonObject> lowLatencyEndOperators = GraphUtilities
                .findOperatorByKind(END_LOW_LATENCY, graph);
        
        if (lowLatencyEndOperators.isEmpty())
            return;
        
        for (JsonObject llStart : lowLatencyEndOperators)
            assignLowLatency(llStart, Direction.UPSTREAM);
    }

    private void assignLowLatency(JsonObject llStart, Direction direction) {
        
        final JsonPrimitive lowLatencyTag =
            new JsonPrimitive("__spl_lowLatency$" + lowLatencyRegionCount++);

        Set<JsonObject> llStartChildren = getDownstream(llStart, graph);
        
        Set<BVirtualMarker> boundaries = EnumSet.of(LOW_LATENCY, END_LOW_LATENCY);

        GraphUtilities.visitOnce(
                new VisitController(direction, boundaries),
                llStartChildren, graph,
                op -> addColocationTag(op, lowLatencyTag));
        
        if (direction == Direction.DOWNSTREAM) {     
            // Low latency merges with upstream.
            for (JsonObject op : getUpstream(llStart, graph)) {
                String kind = kind(op);
                if (BVirtualMarker.PARALLEL.isThis(kind))
                    continue;
                if (BVirtualMarker.END_PARALLEL.isThis(kind))
                    continue;
                if (BVirtualMarker.UNION.isThis(kind))
                    continue;
                addColocationTag(op, lowLatencyTag);
            }
        }
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
            
            JsonArray tags = GsonUtilities.array(placement, PLACEMENT_COLOCATE_TAGS);
            if (tags == null)
                return;
            
            Set<String> sameTags = new HashSet<>();
            for (JsonElement e : tags)
                sameTags.add(e.getAsString());
            
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
        
        setupForRelativizeColocateTags();
    }
    
    /**
     * Create a Json object containing a count (initially 0)
     * for each colocation tag. The count will be the number
     * of unique composites it is used in.
     */
    void setupForRelativizeColocateTags() {   
         JsonObject tagMaps = object(graph, CONFIG, CFG_COLOCATE_TAG_MAPPING);
         JsonObject colocateIds = objectCreate(graph, CONFIG, CFG_COLOCATE_IDS);
      
         for (Entry<String, JsonElement> entry : tagMaps.entrySet()) {
             JsonObject idInfo = new JsonObject();
             idInfo.addProperty("parallel", 0);
             idInfo.addProperty("lowLatency", 0);
             colocateIds.add(entry.getValue().getAsString(), idInfo);
         }
    }
    
    /**
     * Bump the count for each colocation id used in a composite.
     * If a colocation tag is only used in a single parallel composite
     * then its actually id needs to be channel based and relative to the
     * composite instance name, otherwise it's absolute.
     */
    void compositeColocateIdUse(JsonObject compositeDefinition) {
        
        if (jboolean(compositeDefinition, "lowLatencyComposite"))
            return;
               
        Set<String> usedColocateKeys = new HashSet<>();
        operators(compositeDefinition, op -> {
            JsonObject placement = object(op, CONFIG, PLACEMENT);
            if (placement == null)
                return;
            
            String colocateKey = jstring(placement, PLACEMENT_COLOCATE_KEY);
            if (colocateKey != null)
                usedColocateKeys.add(colocateKey);
        });
        
        if (usedColocateKeys.isEmpty())
            return;
            
        Set<String> usedColocateIds = new HashSet<>();       
        JsonObject tagMaps = object(graph, CONFIG, CFG_COLOCATE_TAG_MAPPING);
        for (String key : usedColocateKeys)
            usedColocateIds.add(jstring(tagMaps, key));
        
        final boolean parallel = jboolean(compositeDefinition, "parallelComposite");
        final boolean main = jboolean(compositeDefinition, "__spl_mainComposite");
        final boolean lowLatency = jboolean(compositeDefinition, "lowLatencyComposite");
        JsonObject colocateIds = object(graph, CONFIG, CFG_COLOCATE_IDS);
        for (String id : usedColocateIds) {
            JsonObject idInfo = colocateIds.getAsJsonObject(id);
            
            if (parallel) 
                idInfo.addProperty("parallel", idInfo.get("parallel").getAsInt()+1);
            if (lowLatency)
                idInfo.addProperty("lowLatency", idInfo.get("lowLatency").getAsInt()+1);
            if (main)
                idInfo.addProperty("main", main);
        }
    }
}
