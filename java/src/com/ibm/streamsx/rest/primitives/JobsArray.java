/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.primitives;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.JobGson;

public class JobsArray {
    private final StreamsConnection connection;
    private List<Job> jobs;
    private JobsArrayGson jobArray;

    public JobsArray(StreamsConnection sc, String gsonJobs) {
        connection = sc;
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
