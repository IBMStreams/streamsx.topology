package com.ibm.streamsx.topology.internal.core;

import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

import com.ibm.streamsx.topology.TKeyedStream;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TWindow;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOutput;
import com.ibm.streamsx.topology.builder.BOutputPort;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.UnaryOperator;

class KeyedStreamImpl<T,K> extends StreamImpl<T> implements TKeyedStream<T, K> {

    private final Function<T,K> keyGetter;
    
    KeyedStreamImpl(TopologyElement te, BOutput output, Type tupleType, Function<T,K> keyGetter) {
        super(te, output, tupleType);
        this.keyGetter = keyGetter;
        
        if (output instanceof BOutputPort) {
            BOutputPort port = (BOutputPort) output;

            JavaFunctional.addDependency(te, port.operator(), tupleType);

            Type keyType = TypeDiscoverer.determineStreamTypeFromFunctionArg(
                    Function.class, 1, keyGetter);

            JavaFunctional.addDependency(te, port.operator(), keyType);
        }
    }
    
    @Override
    public Function<T, K> getKeyFunction() {
        return keyGetter;
    }
    
    @Override
    public boolean isKeyed() {
        return true;
    }
    
    /*
     * Override window methods to return a keyed window.
     */

    @Override
    public TWindow<T, K> last() {
        return super.last().key(getKeyFunction());
    }
    @Override
    public TWindow<T, K> last(int count) {
        return super.last(count).key(getKeyFunction());
    }
    @Override
    public TWindow<T, K> last(long time, TimeUnit unit) {
        return super.last(time, unit).key(getKeyFunction());
    }
    
    /*
     * Override stream methods to return a keyed window.
     */
    private TKeyedStream<T,K> _key(TStream<T> stream) {
        return stream.key(getKeyFunction());
    }
    
    @Override
    public TKeyedStream<T,K> endLowLatency() {
        return _key(super.endLowLatency());
    }
    @Override
    public TKeyedStream<T,K> filter(Predicate<T> filter) {
        return _key(super.filter(filter));
    }
    @Override
    public TKeyedStream<T,K> lowLatency() {
        return _key(super.lowLatency());
    }
    @Override
    public TKeyedStream<T,K> modify(UnaryOperator<T> modifier) {
        return _key(super.modify(modifier));
    }
    @Override
    public TKeyedStream<T,K> sample(double fraction) {
        return _key(super.sample(fraction));
    }
    @Override
    public TKeyedStream<T,K> throttle(long delay, TimeUnit unit) {
        return _key(super.throttle(delay, unit));
    }
    @Override
    public TKeyedStream<T,K> unparallel() {
        return _key(super.unparallel());
    } 
   
    @Override
    public TWindow<T, K> window(TWindow<?, ?> window) {
        return super.window(window).key(getKeyFunction());
    }
       
    /*
     * TKeyedStream specific methods.
     */ 
}
