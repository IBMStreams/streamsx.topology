/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getLogicObject;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.metrics.Metric.Kind;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.functional.window.KeyPartitioner;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

@InputPorts(@InputPortSet(cardinality = 1, windowingMode = WindowMode.Windowed))
@OutputPorts(@OutputPortSet(cardinality = 1))
public abstract class FunctionWindow extends FunctionFunctor {
        
    private String keyGetter;

    private Metric nPartitions;

    @Override
    public void initialize(OperatorContext context) throws Exception {
        super.initialize(context);

        StreamWindow<Tuple> window = getInput(0).getStreamWindow();

        createWindowListener(window);

        if (window.isPartitioned()) {
            if (getKeyGetter() == null)
                throw new IllegalStateException("Missing keyGetter function");
            
            SPLMapping<Object> input0Mapping = getInputMapping(
                    this, 0);
            Function<Object,Object> functionKeyGetter = getLogicObject(getKeyGetter());
            window.registerPartitioner(new KeyPartitioner(input0Mapping,
                    functionKeyGetter));
        }
    }

    public Metric getnPartitions() {
        return nPartitions;
    }

    @CustomMetric(kind = Kind.GAUGE)
    public void setnPartitions(Metric nPartitions) {
        this.nPartitions = nPartitions;
    }

    abstract void createWindowListener(StreamWindow<Tuple> window)
            throws Exception;

    public String getKeyGetter() {
        return keyGetter;
    }

    /**
     * Key getter for the window.
     */
    @Parameter(optional=true)
    public void setKeyGetter(String keyGetter) {
        this.keyGetter = keyGetter;
    }
}
