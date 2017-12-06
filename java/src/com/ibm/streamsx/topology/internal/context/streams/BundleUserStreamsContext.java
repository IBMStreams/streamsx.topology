/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import java.io.File;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.internal.context.JSONStreamsContext;

/**
 * A streams context that uses requires a bundle, so the bundle
 * creation is performed using an instance of BundleStreamsContext.
 *
 * @param <T>
 */
abstract class BundleUserStreamsContext<T> extends JSONStreamsContext<T> {

    private final BundleStreamsContext bundler;

    BundleUserStreamsContext(boolean standalone) {
        bundler = new BundleStreamsContext(standalone, false);
    }
    
    @Override
    protected Future<T> action(AppEntity entity) throws Exception {
        File bundle = bundler._submit(entity).get();
        preInvoke(entity, bundle);
        Future<T> future = invoke(entity, bundle);       
        return postInvoke(entity, bundle, future);
    }
    void preInvoke(AppEntity entity, File bundle) throws Exception {      
    }
    
    abstract Future<T> invoke(AppEntity entity, File bundle) throws Exception;
    
    Future<T> postInvoke(AppEntity entity, File bundle, Future<T> future) {
        return future;
    }

}
