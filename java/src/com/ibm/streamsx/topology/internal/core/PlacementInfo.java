/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
*/
package com.ibm.streamsx.topology.internal.core;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.context.Placeable;

/**
 * Manages fusing of Placeables. 
 */
class PlacementInfo {
    
    private static final Map<Topology, WeakReference<PlacementInfo>> placements = new WeakHashMap<>();
    
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
    
    void fuse(Placeable<?> first, Placeable<?> ... toFuse) {
        
        Set<Placeable<?>> elements = new HashSet<>();
        elements.add(first);
        for (Placeable<?> element : toFuse) {
            elements.add(element);
            if (!first.topology().equals(element.topology()) )
                throw new IllegalArgumentException();
        }
        
        if (elements.size() < 2)
            return;
        
        String fusingId = null;
        for (Placeable<?> element : elements) {
            fusingId = fusingIds.get(element);
            if (fusingId != null) {
                break;
            }
        }
        if (fusingId == null) {
            fusingId = "__jaa_fuse" + fusingIds.size();
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
        for (String tag : tags)
            elementResourceTags.add(tag);       
    } 
}
