/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
*/
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.ISOLATE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.CONFIG;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_COLOCATE_TAGS;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.PLACEMENT_RESOURCE_TAGS;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.arrayCreate;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.intersect;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.generator.spl.GraphUtilities;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * Manages fusing of Placeables. 
 */
class PlacementInfo {
    
    private static final AtomicLong nextFuseId = new AtomicLong();

    private static boolean hasPlacement(Placeable<?> element) {        
        return object(element.operator()._json(), CONFIG, PLACEMENT) != null;
    }
    private static JsonObject placement(Placeable<?> element) {
        return objectCreate(element.operator()._json(), CONFIG, PLACEMENT);
    }
    
    /**
     * Fuse a number of placeables.
     * If fusing occurs then the fusing id
     * is set as explicitColocate in the placement JSON object in
     * the operator's config.
     * @throws IllegalArgumentException if Placeables are from different
     *          topologies or if Placeable.isPlaceable()==false.
     */
    
    static boolean colocate(Placeable<?> first, Placeable<?> ... toFuse) {
        
        Set<Placeable<?>> elements = new HashSet<>();
        elements.add(first);
        elements.addAll(Arrays.asList(toFuse));
      
        // check high level constraints
        for (Placeable<?> element : elements) {
            if (!element.isPlaceable())
                throw new IllegalArgumentException(Messages.getString("CORE_ILLEGAL_OPERATION_PLACEABLE"));
            
            if (!first.topology().equals(element.topology()) )
                throw new IllegalArgumentException(Messages.getString("CORE_DIFFERENT_TOPOLOGIES", first.topology().getName(), element.topology().getName()));
        }
        
        if (elements.size() < 2)
            return false;
        
        disallowColocateInLowLatency(elements);
        disallowColocateIsolatedOpWithParent(first, toFuse);
     
        final JsonPrimitive colocateTag =
                new JsonPrimitive("__spl_colocate$" +  nextFuseId.getAndIncrement());
        Set<String> fusedResourceTags = new HashSet<>();
        for (Placeable<?> element : elements) {
            JsonObject placement = placement(element);
            JsonArray colocateIds = arrayCreate(placement, PLACEMENT_COLOCATE_TAGS);
            colocateIds.add(colocateTag);
            
            // Determine the union of all resource tags for the colocated operators
            if (placement.has(PLACEMENT_RESOURCE_TAGS)) {
                addToSet(fusedResourceTags, array(placement, PLACEMENT_RESOURCE_TAGS));
            }
        }
        
        JsonArray fusedResourceTagsJson = setToArray(fusedResourceTags);
        
        // And finally update all the JSON info
        for (Placeable<?> element : elements) {
            JsonObject placement = placement(element);
            placement.add(PLACEMENT_RESOURCE_TAGS, fusedResourceTagsJson);
        }
        
        return true;
    }
    
    private static void addToSet(Set<String> set, JsonArray array) {
        for (JsonElement item : array)
            set.add(item.getAsString());
    }
    
    private static JsonArray setToArray(Set<String> set) {
        JsonArray array = new JsonArray();
        for (String item : set)
            if (!item.isEmpty())
                array.add(new JsonPrimitive(item));
        return array;      
    }
    
    /** throw if s1.isolate().filter().colocate(s1) */
    private static void disallowColocateIsolatedOpWithParent(Placeable<?> first, Placeable<?> ... toFuse) {
        JsonObject graph = first.builder()._complete();
        JsonObject colocateOp = first.operator()._complete();
        Set<JsonObject> parents = GraphUtilities.getUpstream(colocateOp, graph);
        if (!parents.isEmpty()) {
            JsonObject isolate = parents.iterator().next();
            String kind = jstring(isolate, "kind");
            if (!ISOLATE.kind().equals(kind))
                return;
            parents = GraphUtilities.getUpstream(isolate, graph);
            if (parents.isEmpty())
                return;
            JsonObject isolateParentOp = parents.iterator().next();
            for (Placeable<?> placeable : toFuse) {
                JsonObject tgtOp = placeable.operator()._complete();
                if (jstring(tgtOp, "name").equals(jstring(isolateParentOp, "name")))
                    throw new IllegalStateException(Messages.getString("CORE_ILLEGAL_TO_COLOCATE"));
            }
        }
    }
    
    // A short term concession to the fact that colocate()
    // and low latency regions aren't playing well together
    // i.e., the low latency guarantee is being violated.
    // So disallow that configuration for now.
    private static void disallowColocateInLowLatency(Set<Placeable<?>> elements) {
        for (Placeable<?> element : elements) {
            BOperatorInvocation op = element.operator();
            if (element.builder().isInLowLatencyRegion(op))
                throw new IllegalStateException(Messages.getString("CORE_COLOCATE_IN_LOW_LATENCY_REGION"));
        }
    }

    static Set<String> getResourceTags(Placeable<?> element) {
        
        if (!hasPlacement(element))
            return Collections.emptySet(); 
              
        JsonObject placement = placement(element);       
        if (!placement.has(PLACEMENT_RESOURCE_TAGS))
            return Collections.emptySet();
        
        Set<String> elementResourceTags = new HashSet<>();
        addToSet(elementResourceTags, array(placement, PLACEMENT_RESOURCE_TAGS));
        return Collections.unmodifiableSet(elementResourceTags);
    }

    static void addResourceTags(Placeable<?> element, String ... tags) {
        if (Objects.requireNonNull(tags).length == 0)
            return;
        
        Set<String> elementResourceTags = new HashSet<>();
        elementResourceTags.addAll(Arrays.asList(tags));
        
        JsonObject placement = placement(element);
                
        if (placement.has(PLACEMENT_RESOURCE_TAGS))            
            addToSet(elementResourceTags, array(placement, PLACEMENT_RESOURCE_TAGS));
               
        if (placement.has(PLACEMENT_COLOCATE_TAGS))
            findAllColocatedResourceTags(element, array(placement, PLACEMENT_COLOCATE_TAGS), elementResourceTags);

        JsonArray resourceTags = setToArray(elementResourceTags);
        placement.add(PLACEMENT_RESOURCE_TAGS, resourceTags);
        
        if (placement.has(PLACEMENT_COLOCATE_TAGS))
            updateAllColocatedResourceTags(element, array(placement, PLACEMENT_COLOCATE_TAGS), resourceTags);      
    }
    
    private static void findAllColocatedResourceTags(Placeable<?> element, JsonArray colocateTags, Set<String> elementResourceTags) {               
        JsonObject graph = element.builder()._complete();
               
        GsonUtilities.objectArray(graph, "operators", op -> {
            JsonObject placement = object(op, CONFIG, PLACEMENT);
            if (placement != null) {
                JsonArray opColocateTags = array(placement, PLACEMENT_COLOCATE_TAGS);
                if (opColocateTags != null && intersect(colocateTags, opColocateTags)) {
                    if (placement.has(PLACEMENT_RESOURCE_TAGS))
                        addToSet(elementResourceTags, array(placement, PLACEMENT_RESOURCE_TAGS));
                }
            }
                
        });
    }
    private static void updateAllColocatedResourceTags(Placeable<?> element, JsonArray colocateTags, JsonArray resourceTags) {               
        JsonObject graph = element.builder()._complete();
               
        GsonUtilities.objectArray(graph, "operators", op -> {
            JsonObject placement = object(op, CONFIG, PLACEMENT);
            if (placement != null) {
                JsonArray opColocateTags = array(placement, PLACEMENT_COLOCATE_TAGS);
                if (opColocateTags != null && intersect(colocateTags, opColocateTags)) {
                    placement.add(PLACEMENT_RESOURCE_TAGS, resourceTags);
                }
            }
                
        });
    }
}
