package com.ibm.streamsx.topology.spl;

import com.ibm.streamsx.topology.generator.spl.SPLGenerator;

/**
 * Wrapper classes for specifying SPL unsigned integer values.
 * <p>
 * Use:
 * <pre>{@code
 * // to specify an SPL operator parameter value
 * int uint = 0xFFFFFFFE;  // 65534 => -2
 * Map<String,Object> params = new HashMap<>();
 * params.put("aUint32OpParam", new UnsignedInteger(uint));
 * params.put("aUint32OpParam", topology.getSubmissionParameter(..., new UnsignedInteger(uint));
 * params.put("aUint32OpParam", topology.getSubmissionParameter(..., UnsignedInteger.class);
 * ...
 * ... = SPL.invokeOperator(..., params);
 * 
 * // to specify an SPL unsigned int parameter's submission time value
 * Map<String,Object> config = new HashMap<>();
 * Map<String,Object> submitParams = new HashMap<>();
 * submitParams.put("paramName", new UnsignedInteger(...);
 * config.put(ContextProperties.SUBMISSION_PARAMS, submitParams);
 * StreamsContextFactory.getStreamsContext(DISTRIBUTED)
 *     .submit(topology); 
 * }</pre>
 */
public class Unsigned {
    
    // This isn't a parameterized type as that won't suffice for
    // use with Topology.getSubmissionParameter(String,Class).
    
    private Unsigned() {
    }

    /**
     * SPL uint8 wrapper
     */
    public static class UnsignedByte {
        private final Byte value;
        /**
         * Wrap a Byte
         * @param value
         */
        public UnsignedByte(Byte value) {
            this.value = value;
        }
        /**
         * Get the wrapped value;
         * @return the value
         */
        public Byte value() {
            return value;
        }
        /**
         * Returns a string representation of the wrapped value as
         * as an unsigned decimal value.
         * Same as toString().
         * @return the string
         */
        public String toUnsignedString() {
            return SPLGenerator.unsignedString(value);
        }
        @Override
        public String toString() {
            return toUnsignedString();
        }
    }
    
    /**
     * SPL uint16 wrapper
     */
    public static class UnsignedShort {
        private final Short value;
        /**
         * Wrap a Short
         * @param value
         */
        public UnsignedShort(Short value) {
            this.value = value;
        }
        /**
         * Get the wrapped value;
         * @return the value
         */
        public Short value() {
            return (Short) value;
        }
        /**
         * Returns a string representation of the wrapped value as
         * as an unsigned decimal value.
         * Same as toString().
         * @return the string
         */
        public String toUnsignedString() {
            return SPLGenerator.unsignedString(value);
        }
        @Override
        public String toString() {
            return toUnsignedString();
        }
    }
    
    /**
     * SPL uint32 wrapper
     */
    public static class UnsignedInteger {
        private final Integer value;
        /**
         * Wrap a Integer
         * @param value
         */
        public UnsignedInteger(Integer value) {
            this.value = value;
        }
        /**
         * Get the wrapped value;
         * @return the value
         */
        public Integer value() {
            return value;
        }
        /**
         * Returns a string representation of the wrapped value as
         * as an unsigned decimal value.
         * Same as toString().
         * @return the string
         */
        public String toUnsignedString() {
            return SPLGenerator.unsignedString(value);
        }
        @Override
        public String toString() {
            return toUnsignedString();
        }
    }
    
    /**
     * SPL uint64 wrapper
     */
    public static class UnsignedLong {
        private final Long value;
        /**
         * Wrap a Long
         * @param value
         */
        public UnsignedLong(Long value) {
            this.value = value;
        }
        /**
         * Get the wrapped value;
         * @return the value
         */
        public Long value() {
            return value;
        }
        /**
         * Returns a string representation of the wrapped value as
         * as an unsigned decimal value.
         * Same as toString().
         * @return the string
         */
        public String toUnsignedString() {
            return SPLGenerator.unsignedString(value);
        }
        @Override
        public String toString() {
            return toUnsignedString();
        }
    }
}
