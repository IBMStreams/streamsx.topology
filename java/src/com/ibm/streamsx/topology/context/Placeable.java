/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
*/
 package com.ibm.streamsx.topology.context;

import java.util.Set;

import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;

/**
 * Placement directives for a topology element when executing
 * in a distributed runtime.
 * <BR>
 * Placement directives only apply when the topology
 * is submitted to a {@link StreamsContext.Type#DISTRIBUTED DISTRIBUTED},
 * {@link StreamsContext.Type#STREAMING_ANALYTICS_SERVICE STREAMING_ANALYTICS_SERVICE}
 * or {@link StreamsContext.Type#DISTRIBUTED_TESTER DISTRIBUTED_TESTER} context.
 * <BR>
 * For all other context types directives are ignored.
 */
public interface Placeable<T extends Placeable<T>> extends TopologyElement {
    
    /**
     * Can this element have placement directives applied to it.
     * @return {@code true} if placement directives can be assigned, {@code false} if it can not.
     */
    boolean isPlaceable();
    
    /**
     * Colocate this element with other topology elements so that
     * at runtime they all execute within the same processing element
     * (PE or operating system process).
     * {@code elements} may contain any {@code Placeable} within
     * the same topology, there is no requirement
     * that the element is connected (by a stream) directly or indirectly
     * to this element.
     * 
     * <P>
     * When a set of colocated elements are completely within a single parallel
     * region then the colocation constraint is limited to that channel. So if elements
     * {@code A} and {@code B} are colocated then it
     * is guaranteed that {@code A[0],B[0]} from channel 0 are in a single PE
     * and {@code A[1],B[1]}from channel 1 are in a single PE,
     * but typically a different PE to {@code A[0],B[0]}.
     * <BR>
     * When a a set of colocated elements are not completely within a single parallel
     * region then the colocation constraint applies to the whole topology.
     * So if {@code A,B} are in a parallel region but {@code C} is outside then
     * all the replicas of {@code A,B} are colocated with {@code C} and each other. 
     * </P>
     * 
     * <P>
     * Colocation is also referred to as fusing, when topology elements
     * are connected by streams then the communication between them is in-process
     * using direct method calls (instead of a network transport such as TCP).
     * </P>
     * 
     * @param elements Elements to colocate with this container.
     * 
     * @return this
     * 
     * @throws IllegalStateExeception Any element including {@code this} returns {@code false} for {@link #isPlaceable()}.
     */
    T colocate(Placeable<?> ... elements);
    
    /**
     * Add required resource tags for this topology element for distributed submission.
     * This topology element and any it has been {@link #colocate(Placeable...) colocated}
     * with will execute on a resource (host) that has all the tags returned by
     * {@link #getResourceTags()}.
     * 
     * @param tags Tags to be required at runtime.
     * @return this
     */
    T addResourceTags(String ... tags);
    
    /**
     * Get the set of resource tags this element requires.
     * If this topology element has been {@link #colocate(Placeable...) colocated}
     * with other topology elements then the returned set is the union
     * of all {@link #addResourceTags(String...) resource tags added} to each colocated element.
     * @return Read-only set of host tags this element requires.
     */
    Set<String> getResourceTags();
    
    /**
     * Return the invocation name of this element. 
     * This name is unique within the context of its topology.
     * By default an element has a unique generated invocation name
     * 
     * @return Current invocation name if {@link #isPlaceable()} is {@code true}
     * otherwise {@code null}.
     * 
     * @see #invocationName(String)
     * 
     * @since 1.7
     */
    default String getInvocationName() {
        return isPlaceable() ? operator().name() : null;
    }
    
    /**
     * Set the invocation name of this placeable.
     * 
     * Allows an invocation name to be assigned to this placeable.
     * The name is visible in the Streams console for a running job
     * and is associated with the logic for this placeable.
     * <P>
     * Names must be unique within a topology, if this name
     * is already in use then the '{@code name_N'} will be used
     * where {@code N} is a number that makes the name unique
     * within the topology.
     * </P>
     * <P>
     * For example the invocation for sending alerts as SMS (text)
     * messages could be named as <em>SMSAlerts</em>:
     * <pre>
     * <code>
     * TStream&lt;Alert&gt; alerts = ...
     * 
     * alerts.forEach(Alert::sendAsTextMsg).<B>name("SMSAlerts")</B>; 
     * </code>
     * </pre>
     * </P>
     * <P>
     * Note that {@code name} will eventually resolve into an identifier for
     * an operator name and/or a stream name. Identifiers are limited
     * to ASCII characters, thus {@code name} will be modified at code
     * generation time if it cannot be represented as an identifier.
     * </P>
     *
     * @param name Name to assigned.
     * 
     * @return This.
     * 
     * @throws IllegalStateException {@link #isPlaceable()} returns {@code false}.
     * 
     * @see #getInvocationName()
     * 
     * @since 1.7
     */
    T invocationName(String name);
    
    BOperatorInvocation operator(); 
}
