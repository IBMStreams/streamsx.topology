/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */

/**
 * Topic based Publish-Subscribe sample Java applications.
 * 
 * Publish-Subscribe allows applications to
 * subscribe to published streams from other applications,
 * allowing data exchange between applications.
 * <BR>
 * Streams may be exchanged between applications implemented
 * in different languages.
 * <P>
 * To demo these applications execute the {@link topic.PublishBeacon}
 * application one or more times and then the {@link topic.SubscribeBeacon}
 * one or more times. Then view the Streams console or Streams Studio
 * live instance graph to see that the applications connected.
 * <BR>
 * To see the dynamic nature you can run: publish, subscribe, subscribe,
 * publish,subscribe and then see the first two subscribe applications
 * did connect up with the publish application that was submitted after them.
 * For example, the output in Streams Studio instance graph would look
 * something like this, where you can see five running jobs, two publishers
 * (jobs ids 42, 45) and three subscribers (jobs 43,44, 46):
 * <BR>
 * <img src="doc-files/pubsub.png">
 * </P>
 * 
 * @see <a href="../../../spldoc/html/tk$com.ibm.streamsx.topology/ns$com.ibm.streamsx.topology.topic$1.html">Integration with SPL applications</a>
 */
package topic;

