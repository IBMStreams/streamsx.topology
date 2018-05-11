package com.ibm.streamsx.topology.internal.tester.ops;

import static com.ibm.streamsx.topology.internal.tester.fns.ConditionChecker.metricName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ConsistentRegionMXBean;
import com.ibm.streams.operator.control.ConsistentRegionMXBean.State;
import com.ibm.streams.operator.control.ConsistentRegionMXBean.Trigger;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.control.Controllable;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.metrics.OperatorMetrics;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * Initiates consistent region resets through ConsistentRegionMXBean.reset API.
 *
 * Internal operator exposed through the testing apis.
 */
@PrimitiveOperator(namespace="com.ibm.streamsx.topology.testing.consistent")
public class Resetter extends AbstractOperator implements Controllable {
    
    private static final Logger trace = Logger.getLogger("com.ibm.streamsx.topology.testing");
    
    private final Random rand = new Random();
    private List<ScheduledFuture<?>> futures = Collections.synchronizedList(new ArrayList<>());
    
    private Metric valid;
    private Metric seq;
    private Metric fail;
    
    private String conditionName;
    private int minimumResets = 10;
    private final Set<Metric> resetCounts =
            Collections.synchronizedSet(new HashSet<>());
    
    
    public String getConditionName() {
        return conditionName;
    }

    /**
     * Name for condition metrics.
     * @param conditionName
     */
    @Parameter
    public void setConditionName(String conditionName) {
        this.conditionName = conditionName;
    }
    
    public int getMinimumResets() {
        return minimumResets;
    }

    /**
     * Minimum number of resets per channel.
     */
    @Parameter(optional=true)
    public void setMinimumResets(int minimumResets) {
        this.minimumResets = minimumResets;
    }
        
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        
        createConditionMetrics();
        
        Metric mrm = getOperatorContext().getMetrics().createCustomMetric(
                "nMinimumResets", "Minimum number of resets per channel", Metric.Kind.COUNTER);
        mrm.setValue(getMinimumResets());
        
        ControlPlaneContext control = context.getOptionalContext(ControlPlaneContext.class);
        control.connect(this);
    }
    
    private void createConditionMetrics() {
        final OperatorMetrics metrics = getOperatorContext().getMetrics();
        final String name = getConditionName();
        
        valid = metrics.createCustomMetric(metricName("valid", name),
                "Condition: " + name + " is valid", Metric.Kind.GAUGE);
        
        seq = metrics.createCustomMetric(metricName("seq", name),
                "Condition: " + name + " sequence",  Metric.Kind.COUNTER);
        
        fail = metrics.createCustomMetric(metricName("fail", name),
                "Condition: " + name + " failed", Metric.Kind.GAUGE);
    }

    @Override
    public void event(MBeanServerConnection mbs, OperatorContext context, EventType event) {
        switch (event) {
        case ConnectionFailure:
        case ConnectionClosed:
            cancelFutures();
            break;
        default:
            break;
        }
    }

    @Override
    public boolean isApplicable(OperatorContext context) {
        return true;
    }

    @Override
    public void setup(MBeanServerConnection mbs, OperatorContext context) throws InstanceNotFoundException, Exception {
        seq.increment();
        
        ObjectName consistentWildcard = ObjectName.getInstance("com.ibm.streams.control:type=consistent,index=*");
        Set<ObjectName> regions = mbs.queryNames(consistentWildcard, null);
        
        if (regions.isEmpty()) {
            fail.setValue(1);
            trace.severe("No consistent regions!");
            return;
        }
            
        for (ObjectName name : regions) {            
            ConsistentRegionMXBean crbean = JMX.newMXBeanProxy(mbs, name,
                    ConsistentRegionMXBean.class, true);
            
            if (trace.isLoggable(Level.FINE))               
                trace.fine("Discovered consistent region: " + crbean.getName());            
            
            String metricName = "nResets." + crbean.getName();
            Metric resetCount;
            try {
                resetCount = context.getMetrics().createCustomMetric(metricName, "Requested resets for region", Metric.Kind.COUNTER);                
            } catch (IllegalArgumentException e) {
                resetCount = context.getMetrics().getCustomMetric(metricName);
            }
            resetCounts.add(resetCount);
            
            scheduleReset(crbean, resetCount);
            seq.increment();
        }
    }
    
    /**
     * See if the condition has become valid by having
     * executed the minimum number of resets for each region.
     * 
     * Once it becomes valid then stop any resetting.
     */
    private void checkRequiredResets() {
        if (fail.getValue() != 0 || valid.getValue() != 0)
            return;
        
        synchronized (resetCounts) {
            for (Metric resetCount : resetCounts) {
                if (resetCount.getValue() < getMinimumResets())
                    return;
            }
        }
        
        if (fail.getValue() == 0) {
            valid.setValue(1);
            // and stop resetting.
            cancelFutures();
        }
    }
    
    /**
     * Remove any completed items from the list.
     */
    private void completeFutures() {
        synchronized (futures) {
            Iterator<ScheduledFuture<?>> it = futures.iterator();
            while (it.hasNext()) {
                ScheduledFuture<?> future = it.next();
                if (future.isDone())
                    it.remove();
            }
        }
    }
    
    /**
     * Disconnected from JCP so remove all pending items.
     */
    private void cancelFutures() {
        synchronized (futures) {
            for (ScheduledFuture<?> future : futures) {
                if (!future.isDone())
                    future.cancel(false);
            }
            futures.clear();
        }
    }
    
    /**
     * Schedule a reset with a random delay.
     */
    private void scheduleReset(ConsistentRegionMXBean crbean, Metric resetCount) {
        completeFutures();
        
        final int period;
        if (crbean.getTrigger() == Trigger.PERIODIC)
            period = (int) crbean.getPeriod();
        else
            period = 10;
        int delay = rand.nextInt(period * 4);
        
        if (trace.isLoggable(Level.FINE))
            trace.fine("Reset region:" + crbean.getName() + " scheduled in " + delay + " seconds");
        
        futures.add(getOperatorContext().getScheduledExecutorService().schedule(
                () -> performReset(crbean, resetCount), delay, TimeUnit.SECONDS));
    }
    
    /**
     * Perform a reset using the MXBean.
     * 
     * If the maximum number of resets has been reached
     * then a reset is forced.
     * 
     * After issuing the reset another reset is scheduled.
     */
    private void performReset(ConsistentRegionMXBean crbean, Metric resetCount) {
        
        if (trace.isLoggable(Level.FINE))
            trace.fine("Resetting region:" + crbean.getName() + " " + crbean.getState());
               
        crbean.reset(crbean.getState() == State.MAXIMUM_RESET_ATTEMPTS_REACHED);
        resetCount.increment();
        seq.increment();
        
        getOperatorContext().getScheduledExecutorService().schedule(
                () -> checkRequiredResets(), 5, TimeUnit.SECONDS);
        
        scheduleReset(crbean, resetCount);       
    }
}
