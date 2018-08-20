/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.builder.BVirtualMarker.UNION;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.CONSISTENT;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.HASH_ADDER;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.LANGUAGE_JAVA;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_FUNCTIONAL;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.MODEL_SPL;
import static com.ibm.streamsx.topology.internal.core.JavaFunctionalOps.FILTER_KIND;
import static com.ibm.streamsx.topology.internal.core.JavaFunctionalOps.FLAT_MAP_KIND;
import static com.ibm.streamsx.topology.internal.core.JavaFunctionalOps.FOR_EACH_KIND;
import static com.ibm.streamsx.topology.internal.core.JavaFunctionalOps.HASH_ADDER_KIND;
import static com.ibm.streamsx.topology.internal.core.JavaFunctionalOps.HASH_REMOVER_KIND;
import static com.ibm.streamsx.topology.internal.logic.ObjectUtils.serializeLogic;
import static com.ibm.streamsx.topology.logic.Logic.identity;
import static com.ibm.streamsx.topology.logic.Logic.notKeyed;
import static com.ibm.streamsx.topology.logic.Value.of;
import static java.util.Objects.requireNonNull;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BInputPort;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig;
import com.ibm.streamsx.topology.consistent.ConsistentRegionConfig.Trigger;
import com.ibm.streamsx.topology.context.Placeable;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.generator.operator.OpProperties;
import com.ibm.streamsx.topology.generator.port.PortProperties;
import com.ibm.streamsx.topology.internal.functional.ObjectSchemas;
import com.ibm.streamsx.topology.internal.functional.SubmissionParameter;
import com.ibm.streamsx.topology.internal.gson.JSON4JBridge;
import com.ibm.streamsx.topology.internal.logic.FirstOfSecondParameterIterator;
import com.ibm.streamsx.topology.internal.logic.KeyFunctionHasher;
import com.ibm.streamsx.topology.internal.logic.LogicUtils;
import com.ibm.streamsx.topology.internal.logic.Print;
import com.ibm.streamsx.topology.internal.logic.RandomSample;
import com.ibm.streamsx.topology.internal.logic.Throttle;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.logic.Logic;
import com.ibm.streamsx.topology.spi.builder.Invoker;
import com.ibm.streamsx.topology.spi.builder.LayoutInfo;
import com.ibm.streamsx.topology.spi.runtime.TupleSerializer;

public class StreamImpl<T> extends TupleContainer<T> implements TStream<T> {

    private final BOutput output;
    
    /**
     * Tuple serializer for tuples on this stream.
     * Only supported through the SPI interface
     * and not for functional operators invoked
     * directly through topology. Virtual operators
     * such as union, parallel etc. are supported.
     */
    private final Optional<TupleSerializer> serializer;

    @Override
    public BOutput output() {
        return output;
    }
    
    public StreamImpl(TopologyElement te, BOutput output, Type tupleType) {
        this(te, output, tupleType, Optional.empty());
    }

    public StreamImpl(TopologyElement te, BOutput output, Type tupleType,
            Optional<TupleSerializer> serializer) {
        super(te, tupleType);
        this.output = output;
        this.serializer = serializer;
    }
    
    /**
     * Get a simple name to be used for naming operators,
     * returns Object when the class name is not used.
     */
    protected String getTupleName() {
        return TypeDiscoverer.getTupleName(getTupleType());
    }
    
    @Override
    public TStream<T> filter(Predicate<T> filter) {
        String opName = LogicUtils.functionName(filter);

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                FILTER_KIND, filter).layoutKind("Filter");
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        connectTo(bop, true, null);
        
       return addMatchingOutput(bop, refineType(Predicate.class, 0, filter));
    }
    
    protected TStream<T> addMatchingOutput(BOperatorInvocation bop, Type tupleType) {
        return JavaFunctional.addJavaOutput(this, bop, tupleType, true);
    }
    protected TStream<T> addMatchingStream(BOutput output) {
        return new StreamImpl<T>(this, output, getTupleType(), serializer);
    }
    
    /**
     * Try to refine a type down to a Class without generics.
     */
    private Type refineType(Class<?> interfaceClass, int arg, Object object) {
        Type tupleType = getTupleType();
        if (!(tupleType instanceof Class)) {
            // try and refine the type down.
            Type type = TypeDiscoverer.determineStreamTypeFromFunctionArg(interfaceClass, arg, object);
            if (type instanceof Class)
                tupleType = type;
        }
        return tupleType;
    }

    @Override
    public final TSink sink(Consumer<T> sinker) {
        return forEach(sinker);
    }
    
    @Override
    public final TSink forEach(Consumer<T> action) {
        
        String opName = LogicUtils.functionName(action);
        
        JsonObject invokeInfo = new JsonObject();
        invokeInfo.addProperty("name", opName);
        LayoutInfo.kind(invokeInfo, "ForEach");
        com.ibm.streamsx.topology.spi.builder.SourceInfo.addSourceInfo(invokeInfo, getClass());
              
        return Invoker.invokeForEach(this, FOR_EACH_KIND, invokeInfo,
                action, null, null);
    }

    @Override
    public <U> TStream<U> transform(Function<T, U> transformer) {
        return map(transformer);
    }
    
    @Override
    public <U> TStream<U> map(Function<T, U> mapper) {
        return _transform(mapper, 
                TypeDiscoverer.determineStreamType(mapper, null));
    }
    
    private <U> TStream<U> _transform(Function<T, U> transformer, Type tupleType) {
                
        String opName = LogicUtils.functionName(transformer);

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                JavaFunctionalOps.MAP_KIND, transformer).layoutKind("Map");
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        BInputPort inputPort = connectTo(bop, true, null);
        // By default add a queue
        inputPort.addQueue(true);
        return JavaFunctional.addJavaOutput(this, bop, tupleType, true);
    }
    
    private TStream<T> _modify(UnaryOperator<T> transformer, Type tupleType) {
        
        String opName = LogicUtils.functionName(transformer);

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                JavaFunctionalOps.MAP_KIND, transformer).layoutKind("Modify");
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        BInputPort inputPort = connectTo(bop, true, null);
        // By default add a queue
        inputPort.addQueue(true);
        return this.addMatchingOutput(bop, tupleType);
    }
    


    @Override
    public TStream<T> modify(UnaryOperator<T> modifier) {
        return _modify(modifier, refineType(UnaryOperator.class, 0, modifier));
    }

    @Override
    public <U> TStream<U> multiTransform(Function<T, Iterable<U>> transformer) {
        return flatMap(transformer);
    }
    
    @Override
    public <U> TStream<U> flatMap(Function<T, Iterable<U>> mapper) {
        
        return _flatMap(mapper,
                TypeDiscoverer.determineStreamTypeNested(Function.class, 1, Iterable.class, mapper));
    }
    
    private <U> TStream<U> _flatMap(Function<T, Iterable<U>> transformer, Type tupleType) {
    
        String opName = LogicUtils.functionName(transformer);

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName, FLAT_MAP_KIND, transformer).layoutKind("FlatMap");
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        BInputPort inputPort = connectTo(bop, true, null);
        // By default add a queue
        inputPort.addQueue(true);

        return JavaFunctional.addJavaOutput(this, bop, tupleType, true);
    }

    @Override
    public TStream<T> union(TStream<T> other) {
        if (other == this)
            return this;
        return union(Collections.singleton(other));
    }

    @SuppressWarnings("unchecked")
    @Override
    public TStream<T> union(Set<TStream<T>> others) {
        if (others.isEmpty())
            return this;
        
        Set<TStream<T>> allStreams = new HashSet<>();
        allStreams.addAll(others);
        allStreams.add(this);
        // Check we don't have just a single stream.
        if (allStreams.size() == 1)
            return this;
        
                
        List<TStream<T>> sourceStreams = new ArrayList<>();
        sourceStreams.addAll(allStreams);
        
        String schema = output()._type();
        Type tupleType = getTupleType();

        // Unwrap all streams so that we do not add the same stream twice
        // in multiple unions or explicitly and in a union.
        Set<BOutput> outputs = new HashSet<>();
        for (int i = 0; i < sourceStreams.size(); i++) {
            
            TStream<T> s = sourceStreams.get(i);
                       
            // Schemas can be different as the schema
            // defaults to the generic java object if
            // the type cannot be determined even if
            // it is a type that uses a special schema,
            // E..g TStream<String>.
            if (!schema.equals(s.output()._type())) {
                if (s.getTupleClass() != null) {
                    // This stream has the direct schema!
                    schema = s.output()._type();
                    assert getTupleClass() == null;
                    tupleType = s.getTupleClass();
                    if (i != 0) {
                        // Didn't discover it first
                        // reset to go through the list
                        // again. Note this assumes there
                        // are just two options for the schema
                        // generic or direct
                        i = -1; // to get back to 0.
                        outputs.clear();
                        continue;
                    }
                } else {     
                    assert tupleType instanceof Class;
                    s = s.asType((Class<T>) tupleType);                 
                    assert s.output()._type().equals(schema);
                    sourceStreams.set(i, s);
                }
            }
            
            outputs.add(s.output());
        }
        
        BOutput unionOutput = builder().addUnion(outputs);

        return new StreamImpl<T>(this, unionOutput, tupleType, serializer);
    }

    @Override
    public TSink print() {
         final TSink print = forEach(new Print<T>());
         print.operator().layoutKind("Print");
         return print;
    }

    @Override
    public TStream<T> sample(final double fraction) {
        if (fraction < 0.0 || fraction > 1.0)
            throw new IllegalArgumentException();
        TStream<T> sample = filter(new RandomSample<T>(fraction));
        sample.operator().layoutKind("Sample");
        return sample.invocationName(String.format("Sample %.2f%%", fraction*100.0));
    }

    @Override
    public TWindow<T,Object> last(int count) {
        return new WindowDefinition<T,Object>(this, count);
    }

    @Override
    public TWindow<T,Object> window(TWindow<?,?> window) {
        return new WindowDefinition<T,Object>(this, window);
    }

    @Override
    public TWindow<T,Object> last(long time, TimeUnit unit) {
        if (time <= 0)
            throw new IllegalArgumentException(Messages.getString("CORE_WINDOW_DURATION_OF_ZERO"));
        return new WindowDefinition<T,Object>(this, time, unit);
    }

    @Override
    public TWindow<T,Object> last() {
        return last(1);
    }
    
    @Override
    public <J, U> TStream<J> join(TWindow<U,?> window,
            BiFunction<T, List<U>, J> joiner) {
        
        Type tupleType = TypeDiscoverer.determineStreamTypeFromFunctionArg(BiFunction.class, 2, joiner);
        
        return ((WindowDefinition<U,?>) window).joinInternal(this, null, joiner, tupleType);
    }
    
    @Override
    public <J, U> TStream<J> joinLast(
            TStream<U> lastStream,
            BiFunction<T, U, J> joiner) {
        Function<T,Object> nkt = notKeyed();
        Function<U,Object> nku = notKeyed();
        return joinLast(nkt, lastStream, nku, joiner);
    }
    
    @Override
    public <J, U, K> TStream<J> joinLast(
            Function<? super T,? extends K> keyer,
            TStream<U> lastStream,
            Function<? super U, ? extends K> lastStreamKeyer,
            BiFunction<T, U, J> joiner) {
        
        TWindow<U,K> window = lastStream.last().key(lastStreamKeyer);
        
        Type tupleType = TypeDiscoverer.determineStreamTypeFromFunctionArg(BiFunction.class, 2, joiner);
        
        BiFunction<T,List<U>, J> wrapperJoiner = new FirstOfSecondParameterIterator<>(joiner);
        
        return ((WindowDefinition<U,K>) window).joinInternal(this, keyer, wrapperJoiner, tupleType);
    }
    
    @Override
    public <J, U, K> TStream<J> join(
            Function<T,K> keyer,
            TWindow<U,K> window,            
            BiFunction<T, List<U>, J> joiner) {
        
        Type tupleType = TypeDiscoverer.determineStreamTypeFromFunctionArg(BiFunction.class, 2, joiner);
        
        return ((WindowDefinition<U,K>) window).joinInternal(this, keyer, joiner, tupleType);
        
    }
    

    @Override
    public final void publish(String topic) {
    	publish(topic, false);
    }
    
    @Override
    public final void publish(Supplier<String> topic) {
        _publish(topic, false);       
    }
    
    @Override
    public final void publish(Supplier<String> topic, boolean allowFilter) {
        _publish(topic, allowFilter);        
    }
    
    private static void filtersNotAllowed(boolean allowFilter) {
    	if (allowFilter)
    		throw new IllegalArgumentException(Messages.getString("CORE_TSTREAM_TUPLE_TYPE"));
    }
    
    @Override
    public final void publish(String topic, boolean allowFilter) {
        
        checkTopicName(topic);
        
        _publish(topic, allowFilter);
    }
    
    protected void _publish(Object topic, boolean allowFilter) {
    	
    	Type tupleType = getTupleType();
        
        if (JSON4JBridge.isJson4JClass(tupleType)) {
        	filtersNotAllowed(allowFilter);
        	
            SPLStreamBridge.publishJSON(this, topic);
            return;
        }
        
        
        BOperatorInvocation op;
        if (ObjectSchemas.usesDirectSchema(tupleType)) {
        	// Don't allow filtering against schemas that Streams
        	// would not allow a filter against.
        	if (String.class != tupleType)
        		filtersNotAllowed(allowFilter);
        	
            // Publish as a stream consumable by SPL & Java/Scala
        	Map<String,Object> publishParms = new HashMap<>();
        	publishParms.put("topic", topic);
        	publishParms.put("allowFilter", allowFilter);
        	
            op = builder().addSPLOperator("Publish",
                    "com.ibm.streamsx.topology.topic::Publish",
                    publishParms);
 
        } else if (getTupleClass() != null){
        	filtersNotAllowed(allowFilter);
        	
            // Publish as a stream consumable only by Java/Scala
            Map<String,Object> params = new HashMap<>();
            params.put("topic", topic);
            params.put("class", getTupleClass().getName());
            op = builder().addSPLOperator("Publish",
                    "com.ibm.streamsx.topology.topic::PublishJava",
                    params);
        } else {
            throw new IllegalStateException(Messages.getString("CORE_TSTREAM_TUPLE_GENERIC_TYPE"));
        }

        SourceInfo.setSourceInfo(op, StreamImpl.class);
        this.connectTo(op, false, null);
    }
    
    /**
     * Topic name:
     *  - must not be zero length
     *  - must not contain nul
     *  - must not contain wildcard characters
     * @param topic
     */
    protected void checkTopicName(String topic) {
        
        if (topic.isEmpty()
                || topic.indexOf('\u0000') != -1
                || topic.indexOf('+') != -1
                || topic.indexOf('#') != -1
                )
        {
            throw new IllegalArgumentException(Messages.getString("CORE_INVALID_TOPIC_NAME", topic));
        }
    }
    
    @Override
    public TStream<T> parallel(Supplier<Integer> width, Routing routing) {
        
        switch (requireNonNull(routing)) {
        case ROUND_ROBIN:
        case BROADCAST:
            return _parallel(width, routing, null);
            
        case HASH_PARTITIONED:
            UnaryOperator<T> identity = Logic.identity();
            return _parallel(width, routing, identity);
            
        case KEY_PARTITIONED:
            throw new IllegalArgumentException(Messages.getString("CORE_ROUTING_KEY_PARTITIONED"));
        default:
            throw new UnsupportedOperationException(Messages.getString("CORE_UNSUPPORTED_ROUTING", routing));
        }
    }
    
    @Override
    public TStream<T> parallel(Supplier<Integer> width,
            Function<T, ?> keyer) {
        if (keyer == null)
            throw new IllegalArgumentException(Messages.getString("CORE_KEYER_IS_NULL"));
        return _parallel(width, Routing.KEY_PARTITIONED, keyer);
    }
    
    private TStream<T> _parallel(Supplier<Integer> width, Routing routing, Function<T,?> keyer) {

        if (width == null)
            throw new IllegalArgumentException(PortProperties.WIDTH);
        Integer widthVal;
        if (width.get() != null)
            widthVal = width.get();
        else if (width instanceof SubmissionParameter<?>)
            widthVal = ((SubmissionParameter<Integer>)width).getDefaultValue();
        else
            throw new IllegalArgumentException(Messages.getString("CORE_ILLEGAL_WIDTH_NULL"));
        if (widthVal != null && widthVal <= 0)
            throw new IllegalArgumentException(Messages.getString("CORE_ILLEGAL_WIDTH_VALUE"));

        BOutput toBeParallelized = output();
        boolean isPartitioned = false;        
        if (keyer != null) {

            final ToIntFunction<T> hasher = new KeyFunctionHasher<>(keyer);
            
            BOperatorInvocation hashAdder = JavaFunctional.addFunctionalOperator(this,
                    "HashAdder",
                    HASH_ADDER_KIND, hasher);

            hashAdder._json().addProperty(HASH_ADDER, true);
            
            if (serializer.isPresent()) {
                hashAdder.setParameter("inputSerializer", serializeLogic(serializer.get()));
                JavaFunctional.addDependency(this, hashAdder, serializer.get().getClass());
                // If we know the tuple type then just have a dependency on that
                // otherwise the this stream will have the correct dependencies but
                // may have more than we need.
                if (getTupleType() != null)
                    JavaFunctional.addDependency(this, hashAdder, getTupleType());
                else
                    JavaFunctional.copyDependencies(this, operator(), hashAdder);
            }
            
            hashAdder.layout().addProperty("hidden", true);
            BInputPort ip = connectTo(hashAdder, true, null);

            String hashSchema = ObjectSchemas.schemaWithHash(ip._schema());
            toBeParallelized = hashAdder.addOutput(hashSchema);
            isPartitioned = true;
        }
                
        BOutput parallelOutput = builder().parallel(toBeParallelized, routing.name(), width);
        if (isPartitioned) {
            parallelOutput._json().addProperty(PortProperties.PARTITIONED, true);
            JsonArray partitionKeys = new JsonArray();
            partitionKeys.add(new JsonPrimitive("__spl_hash"));
            parallelOutput._json().add(PortProperties.PARTITION_KEYS, partitionKeys);
            // Add hash remover
            StreamImpl<T> parallelStream = new StreamImpl<T>(this,
                    parallelOutput, getTupleType(), serializer);
            BOperatorInvocation hashRemover = builder().addOperator(
                    "HashRemover", HASH_REMOVER_KIND, null);
            hashRemover.setModel(MODEL_SPL, LANGUAGE_JAVA);
            
            hashRemover.layout().addProperty("hidden", true);
            
            @SuppressWarnings("unused")
            BInputPort pip = parallelStream.connectTo(hashRemover, true, null);
            parallelOutput = hashRemover.addOutput(output._type());
        }

        return addMatchingStream(parallelOutput);
    }

    @Override
    public TStream<T> parallel(int width) {
        return parallel(of(width), TStream.Routing.ROUND_ROBIN);
    }

    @Override
    public TStream<T> parallel(Supplier<Integer> width) {
        return parallel(width, TStream.Routing.ROUND_ROBIN);
    }
    
    @Override
    public TStream<T> setParallel(Supplier<Integer> width){
    	BOutputPort output = (BOutputPort)this.output;
    	output.operator().addConfig(OpProperties.PARALLEL, true);
    	output.operator().addConfig(OpProperties.WIDTH, width.get());
    	
    	return this;
    }

    @Override
    public TStream<T> endParallel() {
        BOutput end = output();
        
        // SPL requires a single stream connected
        // to a composite output port
        if (UNION.isThis(end.operator().kind()))
            end = builder().addPassThroughOperator(end);

        return addMatchingStream(builder().unparallel(end));
    }

    @Override
    public TStream<T> throttle(final long delay, final TimeUnit unit) {

        final long delayms = unit.toMillis(delay);
        
        TStream<T> throttle = modify(new Throttle<T>(delayms));
        throttle.operator().layoutKind("Throttle");
        return throttle;
    }

    /**
     * Connect this stream to a downstream operator. If input is null then a new
     * input port will be created, otherwise it will be used to connect to this
     * stream. Returns input or the new port if input was null.
     */
    @Override
    public BInputPort connectTo(BOperatorInvocation receivingBop, boolean functional,
            BInputPort input) {

        // We go through the JavaFunctional code to ensure
        // that we correctly add the dependent jars into the
        // class path of the operator.
        if (functional)
            return JavaFunctional.connectTo(this, output(), getTupleType(),
                receivingBop, input);
        
        return receivingBop.inputFrom(output, input);
    }

    @Override
    public TStream<T> isolate() {
        BOutput toBeIsolated = output();
        if (builder().isInLowLatencyRegion(toBeIsolated))
                throw new IllegalStateException(Messages.getString("CORE_ISOLATE_IN_LOW_LATENCY_REGION"));
        BOutput isolatedOutput = builder().isolate(toBeIsolated); 
        return addMatchingStream(isolatedOutput);
    }
    
    @Override
    public TStream<T> autonomous() {
        BOutput autonomousOutput = builder().autonomous(output()); 
        return addMatchingStream(autonomousOutput);
    }
    
    @Override
    public TStream<T> setConsistent(ConsistentRegionConfig config) {

        if (!isPlaceable())
            throw new IllegalStateException();

        topology().addJobControlPlane();

        // Create an object representing the consistent region
        // converting times to seconds as doubles.
        JsonObject crann = new JsonObject();
        crann.addProperty("trigger", config.getTrigger().name());

        if (Trigger.PERIODIC == config.getTrigger())
            crann.addProperty("period", toSeconds(config.getTimeUnit(), config.getPeriod()));
        crann.addProperty("drainTimeout", toSeconds(config.getTimeUnit(), config.getDrainTimeout()));
        crann.addProperty("resetTimeout", toSeconds(config.getTimeUnit(), config.getResetTimeout()));
        crann.addProperty("maxConsecutiveResetAttempts", config.getMaxConsecutiveResetAttempts());

        output().operator()._json().add(CONSISTENT, crann);

        return this;
    }
    
    private static double toSeconds(TimeUnit unit, long duration) {
        return unit.toMillis(duration) / 1000.0;
    }

    @Override
    public TStream<T> lowLatency() {
        BOutput toBeLowLatency = output();
        BOutput lowLatencyOutput = builder().lowLatency(toBeLowLatency);
        return addMatchingStream(lowLatencyOutput);
    }

    @Override
    public TStream<T> endLowLatency() {
        BOutput toEndLowLatency = output();
        
        // SPL requires a single stream connected
        // to a composite output port
        if (UNION.isThis(toEndLowLatency.operator().kind()))
            toEndLowLatency = builder().addPassThroughOperator(toEndLowLatency);
        
        BOutput endedLowLatency = builder().endLowLatency(toEndLowLatency);
        return addMatchingStream(endedLowLatency);

    }

    @Override
    public List<TStream<T>> split(int n, ToIntFunction<T> splitter) {
        if (n <= 0)
            throw new IllegalArgumentException("n");
        
        List<TStream<T>> l = new ArrayList<>(n);
        
        String opName = LogicUtils.functionName(splitter);

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                opName,
                JavaFunctionalOps.SPLIT_KIND, splitter).layoutKind("Split");
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        connectTo(bop, true, null);
        
        Type outputType = refineType(ToIntFunction.class, 0, splitter);
        for (int i = 0; i < n; i++) {
            TStream<T> splitOutput = JavaFunctional.addJavaOutput(this, bop, outputType, false);
            l.add(splitOutput);
        }

        return l;
    }
    
    /**
     * Get a stream that is typed to tupleClass,
     * adds a dependency on the type.
     */
    @Override
    public TStream<T> asType(Class<T> tupleClass) {
        if (tupleClass.equals(getTupleClass()))
            return this;
        
        // Is a schema change needed?
        if (ObjectSchemas.usesDirectSchema(tupleClass) &&
                !ObjectSchemas.getMappingSchema(tupleClass).equals(output()._type())) {
            TStream<T> newStream = fixDirectSchema(tupleClass);
            if (newStream != null)
                return newStream;
        }
        
        BOperatorInvocation bop = (BOperatorInvocation) output().operator();
        if (JavaFunctionalOps.isFunctional(bop)) {
            return JavaFunctional.getJavaTStream(this, bop, (BOutputPort) output(),
                    tupleClass, serializer);
        }
        
        // TODO
        throw new UnsupportedOperationException();
    }
    
    private TStream<T> fixDirectSchema(Class<T> tupleClass) {
        if (MODEL_FUNCTIONAL.equals(output().operator().model())) {
            
            String schema = output()._type();
            if (schema.equals(ObjectSchemas.JAVA_OBJECT_SCHEMA)) {
                
                
                // If no connections can just change the schema directly.
                if (!output().isConnected()) {
                
                    String directSchema = ObjectSchemas.getMappingSchema(tupleClass);                   
                    output()._json().addProperty("type", directSchema);
                    return null;
                }
            }
        }

        BOperatorInvocation bop = JavaFunctional.addFunctionalOperator(this,
                "SchemaFix",
                JavaFunctionalOps.MAP_KIND, identity());
        SourceInfo.setSourceInfo(bop, StreamImpl.class);
        connectTo(bop, true, null);
        return JavaFunctional.addJavaOutput(this, bop, tupleClass, true);
    }
    
    @Override
    public boolean isPlaceable() {       
        return !output().operator().isVirtual();
    }
    
    @Override
    public BOperatorInvocation operator() {
        if (isPlaceable())
            return (BOperatorInvocation) (output().operator());
        throw new IllegalStateException(Messages.getString("CORE_ILLEGAL_OPERATION_PLACEABLE"));
    }

    @Override
    public TStream<T> colocate(Placeable<?>... elements) {
        PlacementInfo.colocate(this, elements);
            
        return this;
    }

    @Override
    public TStream<T> addResourceTags(String... tags) {
        PlacementInfo.addResourceTags(this, tags);
        return this;              
    }

    @Override
    public Set<String> getResourceTags() {
        return PlacementInfo.getResourceTags(this);
    }
    
    @Override
    public TStream<T> invocationName(String name) {
        if (!isPlaceable())
            throw new IllegalStateException();
        
        builder().renameOp(operator(), Objects.requireNonNull(name));
        return this;
    }
}
