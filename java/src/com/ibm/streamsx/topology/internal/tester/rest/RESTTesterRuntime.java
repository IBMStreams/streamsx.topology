/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.rest;

import static com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext.streamingAnalyticServiceFromDeploy;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;
import com.ibm.streamsx.topology.internal.tester.conditions.ContentsUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.CounterUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.NoStreamCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.StringPredicateUserCondition;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.internal.tester.fns.StringPredicateChecker;
import com.ibm.streamsx.topology.internal.tester.fns.TupleContents;
import com.ibm.streamsx.topology.internal.tester.fns.TupleCount;
import com.ibm.streamsx.topology.tester.Condition;

public class RESTTesterRuntime extends TesterRuntime {
    
    
    private int id;
    private final MetricConditionChecker metricsChecker;

    public RESTTesterRuntime(ConditionTesterImpl tester) {
        super(tester);
        metricsChecker = new MetricConditionChecker();
    }

    @Override
    public void start(Object info) throws Exception {
        
        JsonObject deployment = (JsonObject) info;
        
        final StreamingAnalyticsService sas = streamingAnalyticServiceFromDeploy(deployment);  

        JsonObject submission = jobject(deployment, "submissionResults");
        requireNonNull(submission);

        String jobId = jstring(submission, SubmissionResultsKeys.JOB_ID);
        requireNonNull(jobId);

        Job job = sas.getInstance().getJob(jobId);

        metricsChecker.setup(job);
    }

    @Override
    public void shutdown(Future<?> future) throws Exception {
        metricsChecker.shutdown();
    }
    
    @Override
    public void finalizeTester(Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers,
            Map<TStream<?>, Set<UserCondition<?>>> conditions) throws Exception {
        
        // REST api does not support handlers.
        if (!handlers.isEmpty())
            throw new UnsupportedOperationException();
        
        for (TStream<?> stream : conditions.keySet()) {
            for (UserCondition<?> uc : conditions.get(stream)) {
                if (stream != null)
                    addConditionToStream(stream, uc);
                else
                    addNoStreamCondition((NoStreamCondition) uc);
            }
        }
    }

    /**
     * Add the conditions as for each operators that monitor the
     * condition and sets metrics.
     * 
     * Then create a condition implementation that will monitor the
     * metrics using the REST api and link it to the user condition.
     */
    @SuppressWarnings("unchecked")
    private void addConditionToStream(TStream<?> stream, UserCondition<?> userCondition) {
        
        MetricCondition<?> condition = null;
        String name = null;
        Consumer<Object> fn = null;
        
        if (userCondition instanceof CounterUserCondition) {
            
            CounterUserCondition uc = (CounterUserCondition) userCondition;
            
            name = "count_" + id++;
            fn = new TupleCount<Object>(name, uc.getExpected(), uc.isExact());

            
            condition = new CounterMetricCondition(name, uc);
            
        } else if (userCondition instanceof ContentsUserCondition) {
            ContentsUserCondition<Object> uc = (ContentsUserCondition<Object>) userCondition;
            
            name = "contents_" + id++;
            fn = new TupleContents<Object>(name, uc.isOrdered(), uc.getExpected());
            
            condition = new MetricCondition<Object>(name, (UserCondition<Object>) userCondition);
        } else if (userCondition instanceof StringPredicateUserCondition) {
            StringPredicateUserCondition uc = (StringPredicateUserCondition) userCondition;
            name = "stringChecker_" + id++;
            fn = new StringPredicateChecker(name, uc.getPredicate());
            condition = new MetricCondition<Object>(name, (UserCondition<Object>) userCondition);
        }
        
        if (metricsChecker == null)
            throw new UnsupportedOperationException(userCondition.toString());
        
        TStream<Object> os = (TStream<Object>) stream;
        TSink end = os.forEach(fn);
        if (os.isPlaceable())
            end.colocate(os);
        
        metricsChecker.addCondition(name, condition);       
    }
    
    private void addNoStreamCondition(NoStreamCondition userCondition) {

        String name = userCondition.getClass().getSimpleName() + id++;       
        userCondition.addTo(topology(), name);
        
        @SuppressWarnings("unchecked")
        MetricCondition<?> condition =
                new MetricCondition<Object>(name, (UserCondition<Object>) userCondition);
        
        metricsChecker.addCondition(name, condition);       
    }

    @Override
    public TestState checkTestState(StreamsContext<?> context, Map<String, Object> config, Future<?> future,
            Condition<?> endCondition) throws Exception {
        return metricsChecker.checkTestState();
    }

}
