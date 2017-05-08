/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * Package class to hold information about the Jobs list from GET jobs URL
 */
class JobsArray {
    private JobsArrayGson jobArray;

    public JobsArray(StreamsConnection sc, String gsonJobs) {
        jobArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                                    .fromJson(gsonJobs, JobsArrayGson.class);

        for (Job job : jobArray.jobs) {
            job.setConnection(sc);
        }
    };

    /**
     * @return List of {@Job}
     */
    public List<Job> getJobs() {
        return jobArray.jobs;
    }

    private static class JobsArrayGson {
        @Expose
        public ArrayList<Job> jobs;
        @Expose
        public String resourceType;
        @Expose
        public int total;
    }

    public String getResourceType() {
        return jobArray.resourceType;
    }

    public int getTotal() {
        return jobArray.total;
    }

}
