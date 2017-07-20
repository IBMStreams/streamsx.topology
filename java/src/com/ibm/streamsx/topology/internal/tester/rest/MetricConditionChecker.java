/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Metric;
import com.ibm.streamsx.rest.Operator;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime.TestState;
import com.ibm.streamsx.topology.internal.tester.fns.ConditionChecker;

public class MetricConditionChecker {

    private Job job;
    private boolean seenHealthy;
    
    private final Map<String,MetricCondition<?>> conditions = new HashMap<>();

    public MetricConditionChecker() {
    } 
    
    void addCondition(String name, MetricCondition<?> condition) {
        if (condition == null)
            throw new IllegalStateException();
        conditions.put(name, condition);
    }
    
    void setup(Job job) throws IOException, TimeoutException, InterruptedException {
        this.job = job;
    }
    
    TesterRuntime.TestState checkTestState() throws IOException, InterruptedException {
        if (!seenHealthy) {
            try {
                job.waitForHealthy(5, TimeUnit.SECONDS);
                seenHealthy = true;
                findConditionMetrics();
            } catch (TimeoutException te) {
                return TesterRuntime.TestState.NOT_READY;
            }
        } else {
            job.refresh();
            if (!"healthy".equals(job.getHealth()))
                return TestState.FAIL;
        }
        
        Thread.sleep(1000);
        TesterRuntime.TestState state = oneCheck();
        if (state == TestState.NOT_READY) {
            Thread.sleep(200);
            findConditionMetrics();
        }
        return state;
    }
    
    private TesterRuntime.TestState oneCheck() throws IOException, InterruptedException {
        
        int validCount = 0;
        int progressCount = 0;
        int noProgressCount = 0;
               
        for (MetricCondition<?> condition : conditions.values()) {
            TestState state = condition.oneCheck();
            switch (state) {
            case NOT_READY:
                return TesterRuntime.TestState.NOT_READY;
            case NO_PROGRESS:
                noProgressCount++;
                break;
            case PROGRESS:
                progressCount++;
                break;
            case VALID:
                validCount++;
                break;
            case FAIL:
                return TesterRuntime.TestState.FAIL;
            }
        }
        
        if (validCount == conditions.size())
            return TesterRuntime.TestState.VALID;
        
        if (noProgressCount == conditions.size())
            return TesterRuntime.TestState.NO_PROGRESS;
        
        return TesterRuntime.TestState.PROGRESS;
    }

    private void findConditionMetrics() throws IOException {
        
        job.refresh();

        for (Operator op : job.getOperators()) {
            for (Metric metric : op.getMetrics()) {

                final String metricName = metric.getName();
                if (!metricName.startsWith(ConditionChecker.METRIC_PREFIX))
                    continue;

                String conditionName = metricName.substring(metricName.lastIndexOf(':') + 1);
                if (conditions.containsKey(conditionName)) {
                    MetricCondition<?> condition = conditions.get(conditionName);
                    if (condition.hasMetrics())
                        continue;

                    final String validName = ConditionChecker.metricName("valid", conditionName);
                    final String seqName = ConditionChecker.metricName("seq", conditionName);
                    final String failName = ConditionChecker.metricName("fail", conditionName);

                    if (metricName.equals(validName))
                        condition.setValidMetric(metric);
                    else if (metricName.equals(seqName))
                        condition.setSeqMetric(metric);
                    else if (metricName.equals(failName))
                        condition.setFailMetric(metric);
                }
            }
        }
    }


    void shutdown() throws IOException, Exception {
        for (MetricCondition<?> condition : this.conditions.values()) {
            condition.freeze();
        }
        try {
            if (job != null)
                job.cancel(); 
        } finally {
            job = null;
        }
    }
}
