/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016 
 */
package com.ibm.streamsx.topology.consistent;

import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.TStream;

/**
 * Consistent region configuration.
 * 
 * @see TStream#setConsistent(ConsistentRegionConfig)
 */
public final class ConsistentRegionConfig {

    /**
     * Defines how the drain-checkpoint cycle of a consistent region is triggered.
     */
    public enum Trigger {
        /**
         * Region is triggered by the start operator.
         */
        OPERATOR_DRIVEN,
        
        /**
         * Region is triggered periodically.
         */
        PERIODIC,
        ;
    }

    private final Trigger trigger;
    private final TimeUnit unit = TimeUnit.SECONDS;
    private final long period ;
    private long drain = 180;
    private long reset = 180;
    private int attempts = 5;

    /**
     * Create a {@link Trigger#OPERATOR_DRIVEN} consistent region configuration.
     * Configuration values are set to the default values.
     */
    public ConsistentRegionConfig() {
        this.trigger = Trigger.OPERATOR_DRIVEN;
        this.period = -1;
    }

    /**
     * Create a {@link Trigger#PERIODIC} consistent region configuration.
     * Configuration values are set to the default values.
     * @param period Trigger period in seconds.
     */
    public ConsistentRegionConfig(long period) {
        super();
        this.trigger = Trigger.PERIODIC;
        this.period = period;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public long getPeriod() {
        return period;
    }

    public TimeUnit getTimeUnit() {
        return unit;
    }
    
    public long getDrainTimeout() {
        return drain;
    }
    ConsistentRegionConfig setDrainTimeout(long drainTimeout) {
        this.drain = drainTimeout;
        return this;
    }

    public long getResetTimeout() {
        return reset;
    }
    ConsistentRegionConfig setResetTimeout(long resetTimeout) {
        this.reset = resetTimeout;
        return this;
    }

    public int getMaxConsecutiveResetAttempts() {
        return attempts;
    }
    ConsistentRegionConfig setMaxConsecutiveResetAttempts(int attempts) {
        this.attempts = attempts;
        return this;
    }
}
