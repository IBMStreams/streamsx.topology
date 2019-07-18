/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.context.ContextProperties.FORCE_REMOTE_BUILD;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.google.gson.JsonObject;
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
    /**
     * See if the remote build service should be used.
     * If STREAMS_INSTALL was not set is handled elsewhere,
     * so this path assumes that STREAMS_INSTALL is set and not empty.
     * 
     * Remote build if:
     *  - FORCE_REMOTE_BUILD is set to true.
     *  - Architecture cannot be determined as x86_64. (assumption that service is always x86_64)
     *  - OS is not Linux.
     *  - OS is not centos or redhat
     *  - OS is less than version 6.
     *  - OS service base does not match local os version
     */
    protected final boolean useRemoteBuild(AppEntity entity, Function<AppEntity,Integer> serviceBaseGetter) {
        
        assert System.getenv("STREAMS_INSTALL") != null;
        assert !System.getenv("STREAMS_INSTALL").isEmpty();
        
        final JsonObject deploy = deploy(entity.submission);        
        if (jboolean(deploy, FORCE_REMOTE_BUILD))
            return true;
        
        try {
            final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            
            final String arch = os.getArch();
            boolean x86_64 = "x86_64".equalsIgnoreCase(arch) || "amd64".equalsIgnoreCase(arch);
            if (!x86_64)
                return true;
            
            if (!"Linux".equalsIgnoreCase(os.getName()))
                return true; 
                        
            // /etc/system-release-cpe examples
            //
            // cpe:/o:centos:linux:6:GA
            // cpe:/o/redhat:enterprise_linux:7.2:ga:server

            File cpeFile = new File("/etc/system-release-cpe");
            if (!cpeFile.isFile())
                return true;
            
            List<String> lines = Files.readAllLines(cpeFile.toPath());
            
            String osVendor = null;
            String osVersion = null;
            @SuppressWarnings("unused")
            String osUpdate = null;
            for (String cpe : lines) {
                cpe = cpe.trim();
                if (!cpe.startsWith("cpe:"))
                    continue;
                String[] comps = cpe.split(":");
                if (comps.length < 5)
                    continue;
                if (!"cpe".equals(comps[0]))
                    continue;  
                if (!"/o".equals(comps[1]))
                    continue;
                
                if (!comps[2].isEmpty()) {
                    osVendor = comps[2];
                    osVersion = comps[4];
                    if (comps.length >= 6)
                        osUpdate = comps[5];
                }
            }
            
            // If we can' determine the info, force remote
            if (osVendor == null || osVendor.isEmpty())
                return true;

            if (osVersion == null || osVersion.isEmpty())
                return true;
            
            if (!"centos".equals(osVendor) && !"redhat".equals(osVendor))
                return true;

            double version = Double.valueOf(osVersion);
            if (version < 6)
                return true;
            
            if (serviceBaseGetter != null) {
                // Compare base versions, ir it doesn't exactly match the
                // service force remote build.
                int serviceBase = serviceBaseGetter.apply(entity);

                if (((int) version) != serviceBase)
                    return true;
            }
            
        } catch (SecurityException | IOException | NumberFormatException e) {
            // Can't determine information, force remote.
            return true;
        }
        return false;
    }
}
