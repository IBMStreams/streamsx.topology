/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

/**
 * A streams context that uses requires a bundle, so the bundle
 * creation is performed using an instance of BundleStreamsContext.
 *
 * @param <T>
 */
abstract class BundleUserStreamsContext<T> extends StreamsContextImpl<T> {

    protected final BundleStreamsContext bundler;

    BundleUserStreamsContext(boolean standalone) {
        bundler = new BundleStreamsContext(standalone);
    }
}
