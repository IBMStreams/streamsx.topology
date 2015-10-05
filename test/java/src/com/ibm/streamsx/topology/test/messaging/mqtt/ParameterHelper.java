package com.ibm.streamsx.topology.test.messaging.mqtt;

import java.util.HashMap;
import java.util.Map;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Supplier;

/**
 * A utility class to help with global Submission Parameter
 * creation and use.
 * <p>
 * The helper also enables handling of compile time and submission parameters
 * in a common fashion.  i.e., it enables writing configurable code
 * that's insensitive as to whether a parameter is a submission parameter
 * or a configuration driven compile time constant.
 * <p>
 * <pre>{@code
 * Topology t = new Topology("my application");
 * 
 * // define and create the topology's global 
 * // submission time and compile time parameters
 * // Our submission parameters 
 * ParameterHelper params = new ParameterHelper(t);
 * params.definitions().put("mqtt.serverURI", "tcp://localhost:1883");
 * params.definitions().put("mqtt.userID", System.getProperty("user.name"));
 * params.definitions().put("mqtt.password", String.class);
 * params.definitions().put("mqtt.topic", String.class);
 * params.definitions().put("stage1width", 3);
 * params.definitions().put("stage2width", 2);
 * // a compile time configuration parameter
 * params.definitions().put("stage3width", new Value(2));
 * params.createAll();
 * 
 * ...
 * // use the parameters when creating the MQTT connector configuration
 * MqttStreams mqtt = new MqttStreams(t, createConfig(params));
 * 
 * // use the submission parameters when constructing the topology
 * TStream<Message> filteredMsgs =
 *              mqtt.subscribe(params.getString("mqtt.topic"))
 *              .parallel(params.getInt("stage1width"))
 *                .filter(...)
 *              .endParallel();
 *              
 * // pass the configuration parameters to other topology construction code
 * TStream<Foo> fooStream = doStage2Processing(filteredMsgs, params);
 * doStage3Processing(fooStream, params);
 * }</pre>
 */
class ParameterHelper {
    private final Map<String,Supplier<?>> params = new HashMap<>();
    private final Map<String,Object> defaults = new HashMap<>();
    private final Topology top;
    
    /**
     * Create a helper.
     * @param topology the topology
     */
    public ParameterHelper(Topology topology) {
        this.top = topology;
    }
    
    /**
     * Get the underlying modifiable parameter map.
     * @return the map
     */
    public Map<String,Supplier<?>> parameters() {
        return params;
    }
    
    /**
     * Get the underlying modifiable definitions map.
     * <p>
     * A map entry's key is the parameter name.
     * The entry's value is either:
     * <ul>
     * <li>a {@code Supplier<?>} for a compile time parameter</li>
     * <li>a {@code Class<?>} for a submission parameter lacking a default value</li>
     * <li>a default value for a submission parameter.</li>
     * <ul>
     *  
     * @return the map
     */
    public Map<String,Object> definitions() {
        return defaults;
    }
    
    /**
     * Create all of the parameters present in the definitions map.
     */
    public void createAll() {
        for (Map.Entry<String,Object> e : defaults.entrySet()) {
            String name = e.getKey();
            Supplier<?> value;
            Object val = e.getValue(); 
            if (val instanceof Supplier<?>)
                value  = (Supplier<?>) val;
            else if (val instanceof Class<?>)
                value = top.createSubmissionParameter(name, (Class<?>) e.getValue());
            else
                value = top.createSubmissionParameter(name, e.getValue());
            params.put(name, value);
        }
    }
    
    /**
     * Get a String valued parameter's Supplier<String>. 
     * @param name the submission parameter name
     * @return the Supplier<String>
     * @throws IllegalArgumentException if submission parameter {@code name}
     *         has not been defined.
     */
    public Supplier<String> getString(String name) {
        @SuppressWarnings("unchecked")
        Supplier<String> value = (Supplier<String>) params.get(name);
        if (value == null)
            throw new IllegalArgumentException("name " + name);
        return value;
    }
    
    /**
     * Get a Integer valued parameter's Supplier<Integer>. 
     * @param name the submission parameter name
     * @return the Supplier<Integer>
     * @throws IllegalArgumentException if submission parameter {@code name}
     *         has not been defined.
     */
    public Supplier<Integer> getInt(String name) {
        @SuppressWarnings("unchecked")
        Supplier<Integer> value = (Supplier<Integer>) params.get(name);
        if (value == null)
            throw new IllegalArgumentException("name " + name);
        return value;
    }
}