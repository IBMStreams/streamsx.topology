/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.build;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.Expose;

/**
 * 
 * An object describing an IBM Streams build artifact.
 * 
 */
public class Artifact extends Element {
    
    @Expose
    private String id;
    @Expose
    private long size;
    @Expose
    private String name;
    @Expose
    private String applicationBundle;
    
    static final Artifact create(AbstractConnection connection, String gsonJobString) {
    	Artifact element = gson.fromJson(gsonJobString, Artifact.class);
        element.setConnection(connection);
        return element;
    }

    static final List<Artifact> createArtifactList(Build build, String uri) throws IOException {
        
        List<Artifact> elements = createList(build.connection(), uri, ArtifactArray.class);
        return elements;
    }

    /**
     * Gets the IBM Streams unique identifier for this build artifact.
     * 
     * @return the IBM Streams unique identifier.
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the name for this build artifact.
     * @return name for this build artifact.
     */
    public String getName() {
        return name;
    }
    
    public long getSize() {
    	return size;
    }
    
    public String getURL() {
    	return applicationBundle;
    }
    
    public File download(File directory) throws IOException {
    	File location = directory == null ? new File(getName()) : new File(directory, getName());
    	return StreamsRestUtils.getFile(connection().getExecutor(), connection().getAuthorization(), getURL(), location);
    }
    
    private static class ArtifactArray  extends ElementArray<Artifact> {
        @Expose
        private ArrayList<Artifact> artifacts;
        @Override
        List<Artifact> elements() { return artifacts; }
    }
}
