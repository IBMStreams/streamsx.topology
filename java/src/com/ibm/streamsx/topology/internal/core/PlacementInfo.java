/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
*/
package com.ibm.streamsx.topology.internal.core;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.ISOLATE;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.JOperator;
import com.ibm.streamsx.topology.builder.JOperator.JOperatorConfig;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities;

/**
 * Manages fusing of Placeables. 
 */
class PlacementInfo {

    private static final Map<Topology, WeakReference<PlacementInfo>> placements = new WeakHashMap<>();
    
    private int nextFuseId;
    private final Map<Placeable<?>, String> fusingIds = new HashMap<>();
    private final Map<Placeable<?>, Set<String>> resourceTags = new HashMap<>();
      
    static PlacementInfo getPlacementInfo(TopologyElement te) {
        
        PlacementInfo pi; 
        synchronized(placements) {
            WeakReference<PlacementInfo> wr = placements.get(te.topology());
            if (wr == null) {
                wr = new WeakReference<>(new PlacementInfo());
                placements.put(te.topology(), wr);
            }
            pi = wr.get();
        }
        
        return pi;
    }
    
    /**
     * Fuse a number of placeables.
     * If fusing occurs then the fusing id
     * is set as "explicitColocate" in the "placement" JSON object in
     * the operator's config.
     * @throws IllegalArgumentException if Placeables are from different
     *          topologies or if Placeable.isPlaceable()==false.
     */
    
    synchronized boolean colocate(Placeable<?> first, Placeable<?> ... toFuse) {
        
        Set<Placeable<?>> elements = new HashSet<>();
        elements.add(first);
        elements.addAll(Arrays.asList(toFuse));
      
        // check high level constraints
        for (Placeable<?> element : elements) {
            if (!element.isPlaceable())
                throw new IllegalArgumentException("Placeable.isPlaceable()==false");
            
            if (!first.topology().equals(element.topology()) )
                throw new IllegalArgumentException("Different topologies: "+ first.topology().getName() + " and " + element.topology().getName());
        }
        
        if (elements.size() < 2)
            return false;
        
        disallowColocateInLowLatency(elements);
        disallowColocateIsolatedOpWithParent(first, toFuse);
        
        String fusingId = null;
        for (Placeable<?> element : elements) {
            fusingId = fusingIds.get(element);
            if (fusingId != null) {
                break;
            }
        }
        if (fusingId == null) {
            fusingId = "__jaa_colocate" + nextFuseId++;
        }
        
        Set<String> fusedResourceTags = new HashSet<>();
        
        for (Placeable<?> element : elements) {
            fusingIds.put(element, fusingId);
            
            Set<String> elementResourceTags = resourceTags.get(element);
            if (elementResourceTags != null) {
                fusedResourceTags.addAll(elementResourceTags);
            }            
            resourceTags.put(element, fusedResourceTags);
            
            
        }
        
        // And finally update all the JSON info
        for (Placeable<?> element : elements) {
             updatePlacementJSON(element);
        }
        return true;
    }
    
    /** throw if s1.isolate().filter().colocate(s1) */
    private void disallowColocateIsolatedOpWithParent(Placeable<?> first, Placeable<?> ... toFuse) {
        JSONObject graph = first.builder().complete();
        JSONObject colocateOp = first.operator().complete();
        List<JSONObject> parents = GraphUtilities.getUpstream(colocateOp, graph);
        if (!parents.isEmpty()) {
            JSONObject isolate = parents.get(0);
            String kind = (String) isolate.get("kind");
            if (!ISOLATE.kind().equals(kind))
                return;
            parents = GraphUtilities.getUpstream(isolate, graph);
            if (parents.isEmpty())
                return;
            JSONObject isolateParentOp = parents.get(0);
            for (Placeable<?> placeable : toFuse) {
                JSONObject tgtOp = placeable.operator().complete();
                if (tgtOp == isolateParentOp)
                    throw new IllegalStateException("Illegal to colocate an isolated stream with its parent.");
            }
        }
    }
    
    // A short term concession to the fact that colocate()
    // and low latency regions aren't playing well together
    // i.e., the low latency guarantee is being violated.
    // So disallow that configuration for now.
    private void disallowColocateInLowLatency(Set<Placeable<?>> elements) {
        for (Placeable<?> element : elements) {
            BOperatorInvocation op = element.operator();
            if (element.builder().isInLowLatencyRegion(op))
                throw new IllegalStateException("colocate() is not allowed in a low latency region");
        }
    }
    
    synchronized Set<String> getResourceTags(Placeable<?> element) {
        Set<String> elementResourceTags = resourceTags.get(element);
        if (elementResourceTags == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(elementResourceTags);
    }

    synchronized void addResourceTags(Placeable<?> element, String ... tags) {
        Set<String> elementResourceTags = resourceTags.get(element);
        if (elementResourceTags == null) {
            elementResourceTags = new HashSet<>();
            resourceTags.put(element, elementResourceTags);
        }
        for (String tag : tags) {
            if (!tag.isEmpty())
                elementResourceTags.add(tag);
        }
        
        updatePlacementJSON(element);
    } 
    
    /**
     * Update an element's placement configuration.
     */
    private void updatePlacementJSON(Placeable<?> element) {
        JSONObject placement = JOperatorConfig.createJSONItem(element.operator().json(), JOperatorConfig.PLACEMENT);
        placement.put(JOperator.PLACEMENT_EXPLICIT_COLOCATE_ID, fusingIds.get(element));
        
        Set<String> elementResourceTags = resourceTags.get(element);
        if (elementResourceTags != null && !elementResourceTags.isEmpty()) {
            JSONArray listOfTags = new JSONArray();
            listOfTags.addAll(elementResourceTags);    
            placement.put(JOperator.PLACEMENT_RESOURCE_TAGS, listOfTags);    
        } 
    }
}
