/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016 
 */
package com.ibm.streamsx.topology.consistent;

import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.internal.messages.Messages;

/**
 * Immutable consistent region configuration.
 * 
 * The default values for a {@code ConsistentRegionConfig} are:
 * <UL>
 * <LI><b>{@code drainTimeout}</b> - 180 seconds - Indicates the maximum time in
 * seconds that the drain and checkpoint of the region is allotted to finish
 * processing. If the process takes longer than the specified time, a failure is
 * reported and the region is reset to the point of the previously successfully
 * established consistent state.</LI>
 * <LI><b>{@code resetTimeout}</b> - 180 seconds - Indicates the maximum time in
 * seconds that the reset of the region is allotted to finish processing. If the
 * process takes longer than the specified time, a failure is reported and
 * another reset of the region is attempted</LI>
 * <LI>
 * <b>{@code maxConsecutiveResetAttempts}</b> - 5 - Indicates the maximum
 * number of consecutive attempts to reset a consistent region. After a failure,
 * if the maximum number of attempts is reached, the region stops processing new
 * tuples. After the maximum number of consecutive attempts is reached, a region
 * can be reset only with manual intervention or with a program with a call to a
 * method in the consistent region controller.
 * </LI>
 * </UL>
 * <P>
 * A configuration has {@link #getTimeUnit() time unit} that applies to
 * {@link #getPeriod()}, {@link #getDrainTimeout()} and {@link #getResetTimeout()}.
 * The time unit is hard-coded to {@code TimeUnit.SECONDS}, future versions may
 * allow creating configurations with a different time unit. 
 * </P>
 * <P>
 * <b>Example Use:</b>
 * <pre>
 * <code>
 * // set source to be a the start of an operator driven consistent region
 * // with a drain timeout of five seconds and a reset timeout of twenty seconds.
 * source.setConsistent(operatorDriven().drainTimeout(5).resetTimeout(20));
 * <code>
 * </pre>
 * </P>
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
    private final long drain;
    private final long reset;
    private final int attempts;
    
    private ConsistentRegionConfig(ConsistentRegionConfig old, Long drain, Long reset, Integer attempts) {
        this.trigger = old.trigger;
        this.period = old.period;
        this.drain = drain == null ? old.drain : drain;
        this.reset = reset == null ? old.reset : reset;
        this.attempts = attempts == null ? old.attempts : attempts;       
    }
    
    private ConsistentRegionConfig(Trigger trigger, long period) {
        this.trigger = trigger;
        this.period = period;
        this.drain = 180;
        this.reset = 180;
        this.attempts = 5;       
    }

    /**
     * Create a {@link Trigger#OPERATOR_DRIVEN} consistent region configuration.
     * The source operator drives when a region is drained and checkpointed,
     * for example a messaging source might drain and checkpoint every ten thousand
     * messages.
     * <BR>
     * Configuration values are set to the default values.
     */
    public ConsistentRegionConfig() {
        this(Trigger.OPERATOR_DRIVEN, -1);
    }
    
    /**
     * Create a {@link Trigger#OPERATOR_DRIVEN} consistent region configuration.
     * The source operator triggers drain and checkpoint cycles for the region.
     * <BR>
     * Configuration values are set to the default values.
     * <P>
     * This is equivalent to {@link #ConsistentRegionConfig()}
     * but when used as an imported static method can produce clearer code:
     * <BR>
     * <pre>
     * <code>
     * import static com.ibm.streamsx.topology.consistent.ConsistentRegionConfig.operatorDriven;
     * ...
     *     stream.setConsistent(operatorDriven());
     * </code>
     * </pre>
     * </P>
     */
    public static ConsistentRegionConfig operatorDriven() {
        return new ConsistentRegionConfig();
    }
    
    /**
     * Create a {@link Trigger#PERIODIC} consistent region configuration.
     * The IBM Streams runtime will trigger a drain and checkpoint
     * the region periodically approximately every {@code period} seconds.
     * <BR>
     * Configuration values are set to the default values.
     * <P>
     * This is equivalent to {@link #ConsistentRegionConfig(int)}
     * but when used as an imported static method can produce clearer code:
     * <BR>
     * <pre>
     * <code>
     * import static com.ibm.streamsx.topology.consistent.ConsistentRegionConfig.periodic;
     * ...
     *     stream.setConsistent(periodic(30));
     * </code>
     * </pre>
     * </P>
     * @param period Trigger period in seconds.
     */
    public static ConsistentRegionConfig periodic(int period) {
        return new ConsistentRegionConfig(period);
    }

    /**
     * Create a {@link Trigger#PERIODIC} consistent region configuration.
     * The IBM Streams runtime will trigger a drain and checkpoint
     * the region periodically approximately every {@code period} seconds.
     * <BR>
     * Configuration values are set to the default values.
     * @param period Trigger period in seconds.
     */
    public ConsistentRegionConfig(int period) {
        this(Trigger.PERIODIC, period);
        if (period <= 0)
            throw new IllegalArgumentException(Messages.getString("CONSISTENT_PERIOD", period));
    }

    /**
     * Get the trigger type.
     * @return trigger type.
     */
    public Trigger getTrigger() {
        return trigger;
    }

    /**
     * Get the trigger period.
     * @return period if {@link #getTrigger()} is {@link Trigger#PERIODIC} otherwise -1.
     */
    public long getPeriod() {
        return period;
    }

    /**
     * Get the time unit for {@link #getPeriod()}, {@link #getDrainTimeout()}
     * and {@link #getResetTimeout()()}.
     * @return Time unit for this configuration.
     */
    public TimeUnit getTimeUnit() {
        return unit;
    }
    
    /**
     * Get the drain timeout for this configuration.
     * @return Drain timeout in units of {@link #getTimeUnit()}.
     */
    public long getDrainTimeout() {
        return drain;
    }
    
    /**
     * Return a new configuration changing {@code drainTimeout}.
     * A new configuration instance is returned that is a copy
     * of this configuration with only {@code drainTimeout} changed.
     * <P>
     * {@code stream.setConsistent(periodic(30).drainTimeout(5))}
     * </P>
     * @param drainTimeout Drain timeout to use in seconds, must be greater than 0.
     * @return New configuration with drain timeout set to {@code drainTimeout}
     * and the remaining values copied from this configuration.
     */
    public ConsistentRegionConfig drainTimeout(long drainTimeout) {
        if (drainTimeout <= 0)
            throw new IllegalArgumentException(Messages.getString("CONSISTENT_DRAIN_TIMEOUT", drainTimeout));
            
        return new ConsistentRegionConfig(this, drainTimeout, null, null);
    }
    
    /**
     * Get the reset timeout for this configuration.
     * @return Reset timeout in units of {@link #getTimeUnit()}.
     */
    public long getResetTimeout() {
        return reset;
    }
    
    /**
     * Return a new configuration changing {@code resetTimeout}.
     * A new configuration instance is returned that is a copy
     * of this configuration with only {@code resetTimeout} changed.
     * <P>
     * {@code stream.setConsistent(periodic(30).resetTimeout(15))}
     * </P>
     * @param resetTimeout Reset timeout to use in seconds, must be greater than 0.
     * @return New configuration with reset timeout set to {@code resetTimeout}
     * and the remaining values copied from this configuration.
     */
    public ConsistentRegionConfig resetTimeout(long resetTimeout) {
        if (resetTimeout <= 0)
            throw new IllegalArgumentException(Messages.getString("CONSISTENT_RESET_TIMEOUT", resetTimeout));

        return new ConsistentRegionConfig(this, null, resetTimeout, null);
    }
    

    /**
     * Get the maximum number of consecutive reset attempts.
     * @return Maximum number of consecutive reset attempts.
     */
    public int getMaxConsecutiveResetAttempts() {
        return attempts;
    }
    
    /**
     * Return a new configuration changing {@code maxConsecutiveResetAttempts}.
     * A new configuration instance is returned that is a copy
     * of this configuration with only {@code maxConsecutiveResetAttempts} changed.
     * <P>
     * {@code stream.setConsistent(periodic(30).maxConsecutiveResetAttempts(7))}
     * </P>
     * @param attempts Maximum number of consecutive reset attempts, must be greater than 0.
     * @return New configuration with maxConsecutiveResetAttempts set to {@code attempts}
     * and the remaining values copied from this configuration.
     */
    public ConsistentRegionConfig maxConsecutiveResetAttempts(int attempts) {
        if (attempts <= 0)
            throw new IllegalArgumentException(Messages.getString("CONSISTENT_MAX_CONSECUTIVE_RESET_ATTEMPTS", attempts));

        return new ConsistentRegionConfig(this, null, null, attempts);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(trigger, period, drain, reset, attempts);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConsistentRegionConfig other = (ConsistentRegionConfig) obj;
        if (attempts != other.attempts)
            return false;
        if (drain != other.drain)
            return false;
        if (period != other.period)
            return false;
        if (reset != other.reset)
            return false;
        if (trigger != other.trigger)
            return false;
        if (unit != other.unit)
            return false;
        return true;
    }
}
