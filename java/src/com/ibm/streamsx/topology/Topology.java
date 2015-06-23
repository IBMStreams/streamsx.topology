/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import static com.ibm.streamsx.topology.internal.core.InternalProperties.SPL_PREFIX;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.GraphBuilder;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.function7.Supplier;
import com.ibm.streamsx.topology.internal.core.DependencyResolver;
import com.ibm.streamsx.topology.internal.core.InternalProperties;
import com.ibm.streamsx.topology.internal.core.JavaFunctional;
import com.ibm.streamsx.topology.internal.core.SourceInfo;
import com.ibm.streamsx.topology.internal.core.StreamImpl;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionPeriodicSource;
import com.ibm.streamsx.topology.internal.functional.ops.FunctionSource;
import com.ibm.streamsx.topology.internal.logic.Constants;
import com.ibm.streamsx.topology.internal.logic.EndlessSupplier;
import com.ibm.streamsx.topology.internal.logic.LimitedSupplier;
import com.ibm.streamsx.topology.internal.logic.LogicUtils;
import com.ibm.streamsx.topology.internal.logic.SingleToIterableSupplier;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.internal.spljava.Schemas;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * A declaration of a topology of streaming data.
 * 
 * This class provides some fundamental generic methods to create source
 * streams, such as {@link #source(Supplier, Class)},
 * {@link #subscribe(String, Class)}, {@link #strings(String...)}. <BR>
 * Utility methods in the {@code com.ibm.streamsx.topology.streams} package
 * provide specific source streams, or transformations on streams with specific
 * types.
 * 
 */
public class Topology implements TopologyElement {
    
    /**
     * Logger used for the Topology API, name {@code com.ibm.streamsx.topology}.
     */
    public static Logger TOPOLOGY_LOGGER = Logger.getLogger("com.ibm.streamsx.topology");
    
    /**
     * Logger used for the interactions with IBM InfoSphere Streams functionality, name {@code com.ibm.streamsx.topology.streams}.
     */
    public static Logger STREAMS_LOGGER = Logger.getLogger("com.ibm.streamsx.topology.streams");

    private final String name;

    private final DependencyResolver dependencyResolver;

    private final GraphBuilder builder;
    
    private final Map<String,Object> config = new HashMap<>();

    /**
     * Optional tester of the topology.
     */
    private TupleCollection tester;

    
    /**
     * Create an new topology with a default name.
     * The name is taken to be the calling method name if it is available.
     * If the calling method name is {@code main} then the the calling
     * class is used if it is available.
     * <BR>
     * If a name cannot be determined then {@code Topology} is used.
     */
    public Topology() {
        String[] defaultNames = defaultNamespaceName(true);
        dependencyResolver = new DependencyResolver(this);
        name = defaultNames[1];
        builder = new GraphBuilder(defaultNames[0], name);
    }
    
    /**
     * Create an new topology with a given name.
     */
    public Topology(String name) {
        this.name = name;
        String[] defaultNames = defaultNamespaceName(false);
        dependencyResolver = new DependencyResolver(this);
        builder = new GraphBuilder(defaultNames[0], name);
    }

    /**
     * Name of this topology.
     * @return Name of this topology.
     */
    public String getName() {
        return name;
    }
    
    public Map<String,Object> getConfig() {
        return config;
    }

    /**
     * Get the underlying {@code OperatorGraph}. Internal use only.
     */
    public OperatorGraph graph() {
        return builder().graph();
    }

    /**
     * Return this topology.
     * Returns {@code this}.
     */
    @Override
    public Topology topology() {
        return this;
    }

    /**
     * Create a stream of {@code String} tuples.
     * 
     * @param tuples
     * @return Stream containing {@code tuples}.
     */
    public TStream<String> strings(String... tuples) {
        return constants(Arrays.asList(tuples), String.class);
    }

    /**
     * Create a stream of {@code Number} tuples.
     * 
     * @param tuples
     * @return Stream containing {@code tuples}.
     */
    public TStream<Number> numbers(Number... tuples) {
        return constants(Arrays.asList(tuples), Number.class);
    }

    /**
     * Create a stream containing all the tuples in {@code data}.
     * 
     * @param data
     *            List of tuples.
     * @param tupleTypeClass
     *            Class type {@code T} of the returned stream.
     * @return Declared stream containing tuples from {@code data}.
     */
    public <T> TStream<T> constants(final List<T> data, Class<T> tupleTypeClass) {
        if (data == null)
            throw new NullPointerException();
        
        return source(new Constants<T>(data), tupleTypeClass);
    }

    /**
     * Declare a new source stream that iterates over the return of
     * {@code Iterable<T> get()} from {@code data}. Once all the tuples from
     * {@code data.get()} have been submitted on the stream, no more tuples are
     * submitted.
     * 
     * @param data
     *            Function that produces that data for the stream.
     * @param tupleTypeClass
     *            Class type {@code T} of the returned stream.
     * @return New stream containing the tuples from the iterator returned by
     *         {@code data.get()}.
     */
    public <T> TStream<T> source(Supplier<Iterable<T>> data,
            Class<T> tupleTypeClass) {
        
        String opName = LogicUtils.functionName(data);
        if (opName.isEmpty()) {
            opName = tupleTypeClass.getSimpleName() + "Source";
        } else if (data instanceof Constants) {
            opName = tupleTypeClass.getSimpleName() + opName;
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FunctionSource.class, data);
        SourceInfo.setSourceInfo(bop, getClass());
        return JavaFunctional.addJavaOutput(this, bop, tupleTypeClass);
    }
    
    /**
     * Declare a new source stream that calls
     * {@code data.get()} periodically. Each non-null value
     * present in from the returned {@code Iterable} will
     * appear on the returned stream. If there is no data to be
     * sent then an empty {@code Iterable} must be returned.
     * Thus each call to {code data.get()} will result in
     * zero, one or N tuples on the stream.
     * 
     * @param data
     *            Function that produces that data for the stream.
     * @param period Approximate period {code data.get()} will be called.
     * @param unit Time unit of {@code period}.
     * @param tupleTypeClass
     *            Class type {@code T} of the returned stream.
     * @return New stream containing the tuples from the iterator returned by
     *         {@code data.get()}.
     */
    public <T> TStream<T> periodicMultiSource(Supplier<Iterable<T>> data,
            long period, TimeUnit unit,
            Class<T> tupleTypeClass) {
        
        String opName = LogicUtils.functionName(data);
        if (opName.isEmpty()) {
            opName = tupleTypeClass.getSimpleName() + "PeriodicMultiSource";
        } else if (data instanceof Constants) {
            opName = tupleTypeClass.getSimpleName() + opName;
        }
        
        double dperiod = ((double) unit.toMillis(period)) / 1000.0;
        Map<String,Object> params = new HashMap<>();
        params.put("period", dperiod);

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FunctionPeriodicSource.class, data, params);
        SourceInfo.setSourceInfo(bop, getClass());
        return JavaFunctional.addJavaOutput(this, bop, tupleTypeClass);
    }
    
    /**
     * Declare a new source stream that calls
     * {@code data.get()} periodically. Each non-null value
     * returned will appear on the returned stream.
     * Thus each call to {code data.get()} will result in
     * zero tuples or one tuple on the stream.
     * 
     * @param data
     *            Function that produces that data for the stream.
     * @param period Approximate period {code data.get()} will be called.
     * @param unit Time unit of {@code period}.
     * @param tupleTypeClass
     *            Class type {@code T} of the returned stream.
     * @return New stream containing the tuples returned by
     *         {@code data.get()}.
     */
    public <T> TStream<T> periodicSource(Supplier<T> data,
            long period, TimeUnit unit, Class<T> tupleTypeClass) {
        return periodicMultiSource(new SingleToIterableSupplier<T>(data),
                period, unit, tupleTypeClass);
    }
    
    /**
     * Declare an endless source stream.
     * {@code data.get()} will be called repeatably.
     * Each non-null returned value will be present on the stream.
     * 
     * @param data
     *            Supplier of the tuples.
     * @param tupleTypeClass
     *            Class type {@code T} of the returned stream.
     * @return New stream containing the tuples from calls to {@code data.get()}
     *         .
     */
    public <T> TStream<T> endlessSource(Supplier<T> data,
            Class<T> tupleTypeClass) {
        return source(EndlessSupplier.supplier(data), tupleTypeClass);
    }

    /**
     * Declare an endless source stream.
     * {@code data.apply(n)} will be called repeatably, where {@code n} is the iteration number,
     * starting at zero. Each
     * non-null returned value will be present on the stream.
     * 
     * @param data
     *            Supplier of the tuples.
     * @param tupleTypeClass
     *            Class type {@code T} of the returned stream.
     * @return New stream containing the tuples from calls to
     *         {@code data.apply(n)}.
     */
    public <T> TStream<T> endlessSourceN(Function<Long, T> data,
            Class<T> tupleTypeClass) {
        return source(EndlessSupplier.supplierN(data), tupleTypeClass);
    }

    /**
     * Declare a limited source stream, where the number of tuples is limited to
     * {@code count}. {@code data.get()} will be called {@code count} number of
     * times. Each non-null returned value will be present on the stream.
     * 
     * @param data
     *            Supplier of the tuples.
     * @param count
     *            Maximum number of tuples that will be seen on the stream.
     * @param tupleTypeClass
     *            Class type {@code T} of the returned stream.
     * @return New stream containing the tuples from calls to {@code data.get()}
     *         .
     */
    public <T> TStream<T> limitedSource(final Supplier<T> data,
            final long count, Class<T> tupleTypeClass) {
        if (count < 0)
            throw new IllegalArgumentException(Long.toString(count));
        return source(LimitedSupplier.supplier(data, count), tupleTypeClass);
    }

    /**
     * Declare a limited source stream, where the number of tuples is limited to
     * {@code count}. {@code data.apply(n)} will be called {@code count} number
     * of times, where {@code n} is the iteration number, starting at zero. Each
     * non-null returned value will be present on the stream.
     * 
     * @param data
     *            Supplier of the tuples.
     * @param count
     *            Maximum number of tuples that will be seen on the stream.
     * @param tupleTypeClass
     *            Class type {@code T} of the returned stream.
     * @return New stream containing the tuples from calls to
     *         {@code data.apply(n)}.
     */
    public <T> TStream<T> limitedSourceN(final Function<Long, T> data,
            final long count, Class<T> tupleTypeClass) {
        if (count < 0)
            throw new IllegalArgumentException(Long.toString(count));
        return source(LimitedSupplier.supplierN(data, count), tupleTypeClass);
    }

    /**
     * Declare a stream that is a subscription to {@code topic}.
     * A topic is published using {@link TStream#publish(String)}.
     * Subscribers are matched to published streams when the {@code topic}
     * is an exact match and the type of the stream ({@code T},
     * {@code tupleTypeClass}) is an exact match.
     * <BR>
     * Publish-subscribe is a many to many relationship,
     * multiple streams from multiple applications may
     * be published on the same topic and type. Multiple
     * subscribers may subscribe to a topic and type.
     * <BR>
     * A subscription will match all publishers using the
     * same topic and tuple type. Tuples on the published
     * streams will appear on the returned stream, as
     * a single stream.
     * <BR>
     * The subscription is dynamic, the returned stream
     * will subscribe to a matching stream published by
     * a newly submitted application (a job), and stops a
     * subscription when an running job is cancelled.
     * <P>
     * Publish-subscribe only works when the topology is
     * submitted to a {@link com.ibm.streamsx.topology.context.StreamsContext.Type#DISTRIBUTED}
     * context. This allows different applications (or
     * even within the same application) to communicate
     * using published streams.
     * </P>
     * @param topic Topic to subscribe to.
     * @param tupleTypeClass Type to subscribe to.
     * @return Stream the will contain tuples from matching publishers.
     */
    public <T> TStream<T> subscribe(String topic, Class<T> tupleTypeClass) {
        SPLMapping<T> mapping = Schemas.getSPLMapping(tupleTypeClass);
        SPLStream splImport = SPLStreams.subscribe(this, topic,
                mapping.getSchema());

        return new StreamImpl<T>(this, splImport.output(), tupleTypeClass);
    }

    /**
     * Resolves the jar dependencies, sets the respective parameters. Internal
     * use only.
     * 
     * @throws Exception
     */
    public Map<String, Object> finalizeGraph(StreamsContext.Type contextType,
            Map<String, Object> config) throws Exception {

        Map<String, Object> graphItems = new HashMap<>();
        dependencyResolver.resolveDependencies(config);
        
        finalizeConfig();

        if (tester != null)
            tester.finalizeGraph(contextType, graphItems);

        return graphItems;
    }
    
    public void addThirdPartyDependency(String location) {
       JavaFunctional.addThirdPartyDependency(this, location); 
    }
    
    /**
     * Add a third-party dependency to all eligible operators.
     */
    public void addThirdPartyDependency(Class<?> clazz) {
        JavaFunctional.addThirdPartyDependency(this, clazz); 
    }
    
    private void finalizeConfig() {
        JSONObject jsonConfig = builder().getConfig();
        
        for (String key : config.keySet()) {
            JSONObject cfg = getJSONConfig(jsonConfig, key);            
            addConfig(cfg, key, config.get(key));
        }
    }
    
    
    private static boolean isSPLConfig(String key) {
        return key.startsWith(InternalProperties.SPL_PREFIX);
    }
    
    private JSONObject getJSONConfig(JSONObject jsonConfig, String key) {
        
        if (isSPLConfig(key)) {
            JSONObject splConfig = (JSONObject) jsonConfig.get("spl");
            if (splConfig == null) {
                splConfig = new JSONObject();
                jsonConfig.put("spl", splConfig);
                return splConfig;
            }
        }
        return jsonConfig;
    }
    
    private static String jsonConfigName(String key) {
        if (key.startsWith(SPL_PREFIX)) {
            return key.substring(SPL_PREFIX.length());
        }
        return null;
    }
    
    private void addConfig(JSONObject cfg, String key, Object value) {
        if (value instanceof Collection) {
            JSONArray sa = new JSONArray();
            @SuppressWarnings("unchecked")
            Collection<String> strings = (Collection<String>) value;
            for (String sv : strings) {
                sa.add(sv);
            }
            
            cfg.put(jsonConfigName(key), sa);
        }
    }

    /**
     * Get the tester for this topology.
     * Testing added into the topology  through
     * the returned {@link Tester} only impacts
     * the topology if submitted to a {@link StreamsContext}
     * of type
     * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#EMBEDDED_TESTER},
     * {@link com.ibm.streamsx.topology.context.StreamsContext.Type#STANDALONE_TESTER},
     * or {@link com.ibm.streamsx.topology.context.StreamsContext.Type#DISTRIBUTED_TESTER}.
     * @return tester for this topology.
     */
    public Tester getTester() {
        if (tester == null)
            tester = new TupleCollection(this);

        return tester;
    }

    /**
     * Internal use only.
     * @return the dependencyResolver
     */
    public DependencyResolver getDependencyResolver() {
        return dependencyResolver;
    }

    @Override
    public GraphBuilder builder() {
        return builder;
    }

    /**
     * Determine the default name and package namespace
     */
    private String[] defaultNamespaceName(boolean withName) {
        String[] names = new String[2];
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        
        String packageName = null;
        String topologyName = null;
        for (int i = 0; i < stack.length; i++) {
            StackTraceElement ste = stack[i];
            if (ste.getClassName().equals(Topology.class.getName())) {
                if (i + 2 < stack.length) {
                    StackTraceElement caller = stack[i + 2];
                    String className = caller.getClassName();
                    if (withName) {
                        topologyName = caller.getMethodName();
                        if ("main".equals(topologyName)) {
                            if (className.contains("."))
                                topologyName = className.substring(className
                                        .lastIndexOf('.') + 1);
                            else
                                topologyName = className;
                        }
                    }
                                      
                    if (className.contains("."))
                        packageName = className.substring(0,
                                className.lastIndexOf('.'));
                }
                break;
            }
        }
        
        if (!withName && topologyName == null)
            topologyName = "Topology";

        if (packageName == null) {
            packageName = getName().toLowerCase(Locale.US);
        }
        
        names[0] = packageName;
        names[1] = topologyName;

        return names;
    }
}
