/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_SCALA;
import static com.ibm.streamsx.topology.internal.core.InternalProperties.SPL_PREFIX;
import static com.ibm.streamsx.topology.internal.core.TypeDiscoverer.getTupleName;
import static com.ibm.streamsx.topology.spi.builder.Invoker.invokeSource;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.GraphBuilder;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.core.DependencyResolver;
import com.ibm.streamsx.topology.internal.core.InternalProperties;
import com.ibm.streamsx.topology.internal.core.JavaFunctional;
import com.ibm.streamsx.topology.internal.core.JavaFunctionalOps;
import com.ibm.streamsx.topology.internal.core.SPLStreamBridge;
import com.ibm.streamsx.topology.internal.core.SourceInfo;
import com.ibm.streamsx.topology.internal.core.SubmissionParameterFactory;
import com.ibm.streamsx.topology.internal.core.TypeDiscoverer;
import com.ibm.streamsx.topology.internal.functional.SubmissionParameter;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.logic.Constants;
import com.ibm.streamsx.topology.internal.logic.EndlessSupplier;
import com.ibm.streamsx.topology.internal.logic.LimitedSupplier;
import com.ibm.streamsx.topology.internal.logic.LogicUtils;
import com.ibm.streamsx.topology.internal.logic.SingleToIterableSupplier;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;
import com.ibm.streamsx.topology.json.JSONSchemas;
import com.ibm.streamsx.topology.spi.builder.LayoutInfo;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * A declaration of a topology of streaming data.
 * 
 * This class provides some fundamental generic methods to create source
 * streams, such as {@link #source(Supplier)},
 * {@link #subscribe(String, Class)}, {@link #strings(String...)}. <BR>
 * Utility methods in the {@code com.ibm.streamsx.topology.streams} package
 * provide specific source streams, or transformations on streams with specific
 * types.
 * 
 * <P>
 * A {@code Topology} has a namespace and a name. When a Streams application is
 * created application name will be {@code namespace::name}. Thus:
 * <UL>
 * <LI>the Streams application bundle will be named {@code namespace.name.sab}, </LI>
 * <LI>when submitted the job name (if not supplied) will be {@code namespace::name_jobid}.</LI>
 * </UL>
 * Note that if a namespace or name has non ASCII characters then actual values used
 * will be modified to only contain ASCII characters.
 * </P>
 * 
 */
public class Topology implements TopologyElement {
    
    /**
     * Logger used for the Topology API, name {@code com.ibm.streamsx.topology}.
     */
    public static Logger TOPOLOGY_LOGGER = Logger.getLogger("com.ibm.streamsx.topology");
    
    private final String namespace;
    private final String name;


    private final DependencyResolver dependencyResolver;

    private final GraphBuilder builder;
    
    private final Map<String,Object> config = new HashMap<>();

    /**
     * Optional tester of the topology.
     */
    private ConditionTesterImpl tester;

    
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
        namespace = defaultNames[0];
        name = defaultNames[1];
        builder = new GraphBuilder(namespace, name);
        
        checkForScala(defaultNames);
    }
    
    /**
     * Create an new topology with a given name.
     * 
     * @param name Name of the topology.
     */
    public Topology(String name) {
        this.name = requireNonNull(name);
        String[] defaultNames = defaultNamespaceName(false);
        this.namespace = defaultNames[0];
        dependencyResolver = new DependencyResolver(this);
        builder = new GraphBuilder(namespace, name);
        
        checkForScala(defaultNames);
    }
    
    /**
     * Create an new topology with a given name and namespace.
     * 
     * @param namespace Namespace of the topology.
     * @param name Name of the topology.
     * 
     * @since 1.7
     */
    public Topology(String namespace, String name) {
        this.name = requireNonNull(name);
        this.namespace = requireNonNull(namespace);
        dependencyResolver = new DependencyResolver(this);
        builder = new GraphBuilder(namespace, name);
        
        checkForScala(defaultNamespaceName(false));        
    }
    
    /**
     * If Scala is in the class path then automatically
     * add the Scala library to the bundle.
     */
    private void checkForScala(String[] defaultNames) {
        try {
            Class<?> csf = Class.forName("scala.Function");
            if (csf.getProtectionDomain().getCodeSource() != null)
                addClassDependency(csf);
            else {
                // Loaded from system loader, look for SCALA_HOME
                String scalaHome = System.getenv("SCALA_HOME");
                if (scalaHome != null) {
                    File scalaLib = new File(scalaHome, "lib/scala-library.jar");
                    addJarDependency(scalaLib.getAbsolutePath());
                }
            }
            
        } catch (ClassNotFoundException e) {
            // not using Scala!
        }
        
        // Name of source file declaring the topology.
        String fileName = defaultNames[2];
        if (fileName != null && fileName.endsWith(".scala"))
            builder.getConfig().addProperty(LANGUAGE, LANGUAGE_SCALA);
    }

    /**
     * Name of this topology.
     * @return Name of this topology.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Namespace of this topology.
     * @return Namespace of this topology.
     * 
     * @since 1.7
     */
    public String getNamespace() {
        return namespace;
    }
    
    public Map<String,Object> getConfig() {
        return config;
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
        return _source(new Constants<String>(Arrays.asList(tuples)), String.class, "Strings");
    }

    /**
     * Create a stream of {@code Number} tuples.
     * 
     * @param tuples
     * @return Stream containing {@code tuples}.
     */
    public TStream<Number> numbers(Number... tuples) {
        return _source(new Constants<Number>(Arrays.asList(tuples)), Number.class, "Numbers");
    }
    
    /**
     * Create a stream containing all the tuples in {@code data}.
     * 
     * @param data
     *            List of tuples.
     * @return Declared stream containing tuples from {@code data}.
     */
    public <T> TStream<T> constants(final List<T> data) {
        if (data == null)
            throw new NullPointerException();
        
        Type constantType = TypeDiscoverer.determineStreamTypeFromFunctionArg(List.class, 0, data);
        
        return _source(new Constants<T>(data), constantType, "Constants");
    }

    /**
     * Declare a new source stream that iterates over the return of
     * {@code Iterable<T> get()} from {@code data}. Once all the tuples from
     * {@code data.get()} have been submitted on the stream, no more tuples are
     * submitted. In some cases the iteration may never complete leading
     * to an endless stream.
     * 
     * @param data
     *            Function that produces that data for the stream.
     * @return New stream containing the tuples from the iterator returned by
     *         {@code data.get()}.
     */
    public <T> TStream<T> source(Supplier<Iterable<T>> data) {
        Type tupleType = TypeDiscoverer.determineStreamTypeNested(Supplier.class, 0, Iterable.class, data);
        return _source(data, tupleType, "Source");
    }
    
    private <T> TStream<T> _source(Supplier<Iterable<T>> data,
            Type tupleType, String layoutKind) {
                
        String opName = LogicUtils.functionName(data);
        if (data instanceof Constants) {
            opName = getTupleName(tupleType) + opName;
        }
        
        JsonObject invokeInfo = new JsonObject();
        com.ibm.streamsx.topology.spi.builder.SourceInfo.addSourceInfo(invokeInfo, getClass());
        invokeInfo.addProperty("name", opName);
        LayoutInfo.kind(invokeInfo, layoutKind);
        
        return invokeSource(this, JavaFunctionalOps.SOURCE_KIND, invokeInfo,
                data, tupleType, null, null);
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
     * @return New stream containing the tuples from the iterator returned by
     *         {@code data.get()}.
     */
    public <T> TStream<T> periodicMultiSource(Supplier<Iterable<T>> data,
            long period, TimeUnit unit) {
        
        Type tupleType = TypeDiscoverer.determineStreamTypeNested(Supplier.class, 0, Iterable.class, data);
        
        return _periodicMultiSource(data, period, unit, tupleType);
    }
    
    private <T> TStream<T> _periodicMultiSource(Supplier<Iterable<T>> data,
            long period, TimeUnit unit,
           Type tupleType) {
        
        String opName = LogicUtils.functionName(data);
        if (data instanceof Constants) {
            opName = TypeDiscoverer.getTupleName(tupleType) + opName;
        }
        
        double dperiod = ((double) unit.toMillis(period)) / 1000.0;
        Map<String,Object> params = new HashMap<>();
        params.put("period", dperiod);

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                JavaFunctionalOps.PERIODIC_MULTI_SOURCE_KIND, data, params);
        SourceInfo.setSourceInfo(bop, getClass());
        return JavaFunctional.addJavaOutput(this, bop, tupleType, true);
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
     * @return New stream containing the tuples returned by
     *         {@code data.get()}.
     */
    public <T> TStream<T> periodicSource(Supplier<T> data,
            long period, TimeUnit unit) {
        
        Type tupleType = TypeDiscoverer.determineStreamType(data, null);
        
        return _periodicMultiSource(new SingleToIterableSupplier<T>(data),
                period, unit, tupleType);
    }
    
    /**
     * Declare an endless source stream.
     * {@code data.get()} will be called repeatably.
     * Each non-null returned value will be present on the stream.
     * 
     * @param data
     *            Supplier of the tuples.
     * @return New stream containing the tuples from calls to {@code data.get()}
     *         .
     */
    public <T> TStream<T> endlessSource(Supplier<T> data) {
        
        Type tupleType = TypeDiscoverer.determineStreamType(data, null);
        return _source(EndlessSupplier.supplier(data), tupleType, "Source");
    }
    
    /**
     * Declare an endless source stream.
     * {@code data.apply(n)} will be called repeatably, where {@code n} is the iteration number,
     * starting at zero. Each
     * non-null returned value will be present on the stream.
     * 
     * @param data
     *            Supplier of the tuples.
     * @return New stream containing the tuples from calls to
     *         {@code data.apply(n)}.
     */
    public <T> TStream<T> endlessSourceN(Function<Long, T> data) {
        
        Type tupleType = TypeDiscoverer.determineStreamType(data, null);
        
        return _source(EndlessSupplier.supplierN(data), tupleType, "Source");
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
     * @return New stream containing the tuples from calls to {@code data.get()}
     *         .
     */
    public <T> TStream<T> limitedSource(final Supplier<T> data,
            final long count) {
        if (count < 0)
            throw new IllegalArgumentException(Long.toString(count));
        
        Type tupleType = TypeDiscoverer.determineStreamType(data, null);
        
        return _source(LimitedSupplier.supplier(data, count), tupleType, "Source");
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
     * @return New stream containing the tuples from calls to
     *         {@code data.apply(n)}.
     */
    public <T> TStream<T> limitedSourceN(final Function<Long, T> data,
            final long count) {
        if (count < 0)
            throw new IllegalArgumentException(Long.toString(count));
        
        Type tupleType = TypeDiscoverer.determineStreamType(data, null);
        
        return _source(LimitedSupplier.supplierN(data, count), tupleType, "Source");
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
     * or {@link com.ibm.streamsx.topology.context.StreamsContext.Type#STREAMING_ANALYTICS_SERVICE}
     * context. This allows different applications (or
     * even within the same application) to communicate
     * using published streams.
     * </P>
     * <P>
     * If {@code tupleTypeClass} is {@code JSONObject.class} then the
     * subscription is the generic IBM Streams schema for JSON
     * ({@link JSONSchemas#JSON}). Streams of type {@code JSONObject}
     * are always published and subscribed using the generic schema
     * to allow interchange between applications implemented in
     * different languages.
     * </P>
     * @param topic Topic to subscribe to.
     * @param tupleTypeClass Type to subscribe to.
     * @return Stream the will contain tuples from matching publishers.
     * 
     * @see TStream#publish(String)
     * @see com.ibm.streamsx.topology.spl.SPLStreams#subscribe(TopologyElement, String, com.ibm.streams.operator.StreamSchema)
     */
    public <T> TStream<T> subscribe(String topic, Class<T> tupleTypeClass) {
        checkTopicFilter(topic);
        
        return SPLStreamBridge.subscribe(this, topic, tupleTypeClass);
    }
    
    /**
     * Declare a stream that is a subscription to {@code topic}.
     * 
     * Differs from {@link #subscribe(String, Class)} in that it
     * supports {@code topic} as a submission time parameter, for example
     * using the topic defined by the submission parameter {@code eventTopic}:
     * 
     * <pre>
     * <code>
     * Supplier<String> topicParam = topology.createSubmissionParameter("eventTopic", String.class);
     * TStream<String> events = topology.subscribe(topicParam, String.class);
     * </code>
     * </pre>

     * @param topic Topic to subscribe to.
     * @param tupleTypeClass Type to subscribe to.
     * @return Stream the will contain tuples from matching publishers.
     * 
     * @see #subscribe(String, Class)
     * 
     * @since 1.8
     */
    public <T> TStream<T> subscribe(Supplier<String> topic, Class<T> tupleTypeClass) {        
        return SPLStreamBridge.subscribe(this, topic, tupleTypeClass);
    }
    
    /**
     * Topic filter:
     *  - must not be zero length
     *  - must not contain nul
     * @param filter
     */
    private void checkTopicFilter(String filter) {
        
        boolean badFilter = false;
        if (filter.isEmpty() || filter.indexOf('\u0000') != -1)
        {
            badFilter = true;
        }
        
        // Test # position.
        if (!badFilter && filter.indexOf('#') != -1) {
            // # by itself is ok
            if ("#".equals(filter))
                ;
            else {
                // must end with /# and only have one # at the end
                if (filter.indexOf('#') != filter.length() - 1)
                    badFilter = true;
                else if (!filter.endsWith("/#"))
                    badFilter = true;
            }
        }
        
        // Test + positions - must be at a level by themselves.
        if (!badFilter && filter.indexOf('+') != -1) {
            StringTokenizer st = new StringTokenizer(filter, "/");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (token.indexOf('+') == -1)
                    continue;
                // If + exists it must be by itself
                if ("+".equals(token))
                    continue;
                badFilter = true;
            }
        }
        
        if (badFilter)
            throw new IllegalArgumentException(Messages.getString("TOPOLOGY_INVALID_TOPIC_FILTER", filter));
    }

    /**
     * Resolves the jar dependencies, sets the respective parameters. Internal
     * use only.
     * <BR>
     * Not intended to be called by applications, may be removed at any time.
     * 
     * @throws Exception
     */
    public void finalizeGraph(StreamsContext<?> context) throws Exception {
        
        if (hasTester())
            tester.finalizeGraph(context);

        dependencyResolver.resolveDependencies();
        
        finalizeConfig();
    }
    
    /**
     * Includes a jar file, specified by the {@code location} String, into 
     * the application runtime. For example, the following code includes the 
     * myResource.jar jar file such that it could be used when running in a 
     * non-EMBEDDED context.
     * <pre><code>
     * Topology top = new Topology("myTopology");
     * top.addJarDependency("./libs/myResource.jar");
     * </pre></code> 
     * For running embedded, simply adding the jar to the classpath when 
     * compiling/running is sufficient.
     * 
     * @param location The location of a jar to be included in the application's
     * runtime when submitting with a DISTRIBUTED or STANDALONE context.
     */
    public void addJarDependency(String location) {
       JavaFunctional.addJarDependency(this, location); 
    }
    
    /**
     * Ensures that a class file is loaded into the application's runtime. 
     * If the .class file  is contained in a jar file, the jar file is also included
     * in the application's runtime. If the .class file is in the directory
     * structure of a package, the top-level directory is converted to a jar file 
     * and included in the application's runtime. <br><br>
     * For example, if the class exists in the following package directory structure:
     * <pre><code>
     * upperDir
     *     |_com
     *         |_foo
     *             |_bar
     *             |   |_bar.class
     *             |   |_baz.class
     *             |_fiz
     *                 |_buzz.class
     * </pre></code>
     * and addClassDependency is invoked as follows:
     * <pre><code>
     * Topology top = new Topology("myTopology");
     * top.addJarDependency(bar.class);
     * </pre></code> 
     * Then the entire contents of upperDir is turned into a jar file and included 
     * in the application's runtime -- this includes baz and buzz, not just bar!
     * <br><br>As with
     * {@link com.ibm.streamsx.topology.Topology#addJarDependency(String location)},
     * this is only required when using additional jars  when running in 
     * a non-EMBEDDED context.
     * @param clazz The class of a resource to be included in the 
     * application's runtime when submitting with a DISTRIBUTED or STANDALONE 
     * context.
     */
    public void addClassDependency(Class<?> clazz) {
        JavaFunctional.addClassDependency(this, clazz); 
    }

    /**
     * Add file or directory tree {@code location} to directory
     * {@code dstDirName} in the application bundle, making it
     * available to the topology's SPL operators at runtime - e.g.,
     * an operator configuration file in "etc" in the application directory.
     * <p>
     * Use {@link #addClassDependency(Class)} or {@link #addJarDependency(String)}
     * to add class or jar dependencies.
     * <p>
     * Functional logic implementations that need to access resources should
     * package the resources in a jar or classes directory, add that to the
     * topology as a dependency using {@code addJarDependency(String)}
     * or {@code addClassDependency(Class)} and access them as resources from
     * the class loader as described here:
     *   <a href="https://docs.oracle.com/javase/tutorial/deployment/webstart/retrievingResources.html">https://docs.oracle.com/javase/tutorial/deployment/webstart/retrievingResources.html</a>
     * <p>
     * Legal values for {@code dstDirName} are {@code etc} or {@code opt}.
     * <p>
     * e.g.,
     * <pre>
     * // add "myConfigFile" to the bundle's "etc" directory
     * addFileDependency("etc", "/tmp/myConfigFile");
     * 
     * // add "myApp" directory tree to the bundle's "etc" directory
     * addFileDependency("etc", "/tmp/myApp");
     * </pre>
     * @param location path to a file or directory
     * @param dstDirName name of directory in the bundle
     * 
     * @throws IllegalArgumentException if {@code dstDirName} is not {@code etc}
     *     or {@code opt}, or {@code location} is not a file or directory.
     */
    public void addFileDependency(String location, String dstDirName) {
        dependencyResolver.addFileDependency(location, dstDirName);
    }
    
    private void finalizeConfig() {
        JsonObject jsonConfig = builder().getConfig();
        
        for (String key : config.keySet()) {
            JsonObject cfg = getJSONConfig(jsonConfig, key);            
            addConfig(cfg, key, config.get(key));
        }
    }
    
    
    private static boolean isSPLConfig(String key) {
        return key.startsWith(InternalProperties.SPL_PREFIX);
    }
    
    private JsonObject getJSONConfig(JsonObject jsonConfig, String key) {
        
        if (isSPLConfig(key)) {
            return GsonUtilities.objectCreate(jsonConfig, "spl");
        }
        return jsonConfig;
    }
    
    private static String jsonConfigName(String key) {
        if (key.startsWith(SPL_PREFIX)) {
            return key.substring(SPL_PREFIX.length());
        }
        return null;
    }
    
    private void addConfig(JsonObject cfg, String key, Object value) {        
        GsonUtilities.addToObject(cfg, jsonConfigName(key), value);
    }

    /**
     * Get the tester for this topology. if the tester was
     * not already created it is created.
     * Testing added into the topology through
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
            tester = new ConditionTesterImpl(this);

        return tester;
    }
    /**
     * Has the tester been created for this topology.
     * Returns true if {@link #getTester()} has been called.
     * @return True if the tester has been created, false otherwise.
     */
    public final boolean hasTester() {
        return tester != null;
    }
    
    /**
     * Checkpoint the state of the graph periodically.
     * Each stateful element in the topology
     * checkpoints its state periodically according to
     * {@code period} and {@code unit}. Every element persists
     * its state autonomously, asynchronously with processing
     * its streams.
     * <BR>
     * Upon a failure of an element's container the element
     * will restart in a new container using its last
     * checkpointed state. If no state is available, due
     * to a failure before the first checkpoint, then the
     * element reverts to its initial state.
     * <P>
     * For stream processing elements defined by Java functions
     * (such as {@link #source(Supplier)} and
     * {@link TStream#transform(Function)}) the state is the
     * serialized form of the object representing the function.
     * Synchronization is applied to ensure that checkpointed state of
     * the object does not include inconsistencies due to ongoing stream processing.
     * <BR>
     * If the function object is immutable then no checkpointing occurs for that element.
     * A function object is taken as mutable if any of these conditions are true:
     * <UL>
     * <LI>It contains at least one non-transient, non-final instance field.</LI>
     * <LI>A final instance field is a reference to a mutable object.
     * Note that identification of what is a immutable object may be limited and
     * so in some cases function objects may be checkpointed even though
     * they are immutable. 
     * </LI>
     * </UL>
     * Otherwise the function object is taken as immutable.
     * </P>
     * <P>
     * Checkpointing is only supported in distributed contexts.
     * </P>
     * 
     * @param period Approximate period for checkpointing.
     * @param unit Time unit of {@code period}.
     */
    public void checkpointPeriod(long period, TimeUnit unit) {
        JsonObject checkpoint = new JsonObject();
        checkpoint.addProperty("mode", "periodic");
        checkpoint.addProperty("period", period);
        checkpoint.addProperty("unit", unit.name());
        
        builder().getConfig().add("checkpoint", checkpoint);
    }

    /**
     * Internal use only.
     * <BR>
     * Not intended to be called by applications, may be removed at any time.
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
        String[] names = new String[3];
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        
        String packageName = null;
        String topologyName = null;
        String fileName = null;
        for (int i = 0; i < stack.length; i++) {
            StackTraceElement ste = stack[i];
            if (ste.getClassName().equals(Topology.class.getName())) {
                if (i + 2 < stack.length) {
                    StackTraceElement caller = stack[i + 2];
                    fileName = caller.getFileName();
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
        names[2] = fileName;

        return names;
    }

    /**
     * Create a submission parameter without a default value.
     * <p>
     * A submission parameter is a handle for a {@code T} whose actual value
     * is not defined until topology submission time.  Submission
     * parameters enable the creation of more reusable topology bundles.
     * <p>
     * A submission parameter has a {@code name}.  The name must be unique
     * within the topology.
     * <p>
     * The parameter is a {@link Supplier}. 
     * Prior to submitting the topology, while constructing the topology,
     * {@code parameter.get()} returns null.
     * <p>
     * When the topology is submitted, {@code parameter.get()}
     * in the executing topology returns the actual submission time value
     * (or the default value see {@link #createSubmissionParameter(String, Object)}).
     * <p>
     * Submission parameters may be used within functional logic.
     * e.g.,
     * <pre>{@code
     * Supplier<Integer> threshold = topology.createSubmissionParameter("threshold", 100);
     * TStream<Integer> s = ...;
     * // with a Java8 lambda expression
     * TStream<Integer> filtered1 = s.filter(v -> v > threshold.get());
     * // without
     * TStream<Integer> filtered2 = s.filter(new Predicate() {
     *      public boolean test(Integer v) {
     *          return v > threshold.get();
     *      }} );
     * }</pre>
     * <p>
     * Submission parameters may also be used for values in various
     * cases such as {@link TStream#parallel(Supplier)} width value
     * and MQTT connector configuration and topic values.
     * e.g.,
     * <pre>{@code
     * Supplier<Integer> width = topology.createSubmissionParameter("width", 1);
     * TStream<String> s = ...;
     * TStream<String> parallel_start = s.parallel(width);
     * TStream<String> in_parallel = parallel_start.filter(...).transform(...);
     * TStream<String> joined_parallel_streams = in_parallel.endParallel();
     * }</pre>
     * <p>
     * Finally, submission parameters may be used in Java Primitive Operator
     * and SPL Operator parameter values.
     * <p>
     * The submission parameter's name is used to supply an actual value
     * at topology submission time
     * via {@link StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)}
     * and {@link ContextProperties#SUBMISSION_PARAMS},
     * or when submitting a topology bundle for execution via other
     * execution runtime native mechanisms such as IBM Streams {@code streamtool}.
     * <p>
     * Topology submission behavior when a submission parameter 
     * lacking a default value is created and a value is not provided at
     * submission time is defined by the underlying topology execution runtime.
     * Submission fails for contexts {@code DISTRIBUTED}, {@code STANDALONE},
     * {@code ANALYTIC_SERVICE}, or {@code EMBEDDED}.
     *
     * @param name submission parameter name
     * @param valueClass class object for {@code T}
     * @return the {@code Supplier<T>} for the submission parameter
     * @throws IllegalArgumentException if {@code name} is null, empty,
     *  or has already been defined. 
     */
    public <T> Supplier<T> createSubmissionParameter(String name, Class<T> valueClass) {
        SubmissionParameter<T> sp = SubmissionParameterFactory.create(name, valueClass); 
        builder().createSubmissionParameter(name, SubmissionParameterFactory.asJSON(sp));
        return sp;
    }

    /**
     * Create a submission parameter with a default value.
     * <p>
     * See {@link #createSubmissionParameter(String, Class)} for a description
     * of submission parameters.
     * @param name submission parameter name
     * @param defaultValue default value if parameter isn't specified.
     * @return the {@code Supplier<T>} for the submission parameter
     * @throws IllegalArgumentException if {@code name} is null, empty,
     *  or has already been defined. 
     * @throws IllegalArgumentException if {@code defaultValue} is null
     */
    public <T> Supplier<T> createSubmissionParameter(String name, T defaultValue) {
        SubmissionParameter<T> sp = SubmissionParameterFactory.create(name, defaultValue);
        builder().createSubmissionParameter(name, SubmissionParameterFactory.asJSON(sp));
        return sp;
    }

    private boolean hasJCP;
    /**
     * Add job control plane to this topology. Creates a job control
     * plane, which other operators can use to communicate control information.
     * Job control plane is a JMX MBean Server that supports registration of
     * MBeans from operators and logic within the job. Operators then use their MBeans, or
     * the ones that are automatically registered in the job control plane, to
     * provide intra-job control, co-ordination, and configuration.
     * <P>
     * Multiple calls to this method may be made, a single job control plane
     * will be created.
     * </P>
     * <P>
     * A job control plane is required when a consistent region is used
     * and is added automatically by TODO-add link.
     * </P>
     * <P>
     * Job control plane is only supported in distributed contexts.
     * </P>
     * @since com.ibm.streamsx.topology 1.5
     */
    public void addJobControlPlane() {
        if (!hasJCP) {
            // no inputs, outputs or parameters.
            builder.addSPLOperator("JobControlPlane", "spl.control::JobControlPlane", Collections.emptyMap());
            hasJCP = true;
        }        
    }
}
