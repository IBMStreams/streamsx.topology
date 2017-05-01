/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

/**
 * Package class to hold information about the Jobs list from GET jobs URL
 */
class JobsArray {
    private List<Job> jobs;
    private JobsArrayGson jobArray;

    public JobsArray(StreamsConnection sc, String gsonJobs) {
        jobArray = new Gson().fromJson(gsonJobs, JobsArrayGson.class);

        jobs = new ArrayList<Job>(jobArray.jobs.size());
        for (JobGson jg : jobArray.jobs) {
            jobs.add(new Job(sc, jg));
        }
    };

    /**
     * @return List of {@Job}
     */
    public List<Job> getJobs() {
        return jobs;
    }

    private static class JobsArrayGson {
        public ArrayList<JobGson> jobs;
        public String resourceType;
        public int total;
    }

    public String getResourceType() {
        return jobArray.resourceType;
    }

    public int getTotal() {
        return jobArray.total;
    }

}
