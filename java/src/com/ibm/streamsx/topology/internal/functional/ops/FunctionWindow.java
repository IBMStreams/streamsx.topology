/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;

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
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.internal.functional.window.KeyablePartitioner;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.tuple.Keyable;

@InputPorts(@InputPortSet(cardinality = 1, windowingMode = WindowMode.Windowed))
@OutputPorts(@OutputPortSet(cardinality = 1))
public abstract class FunctionWindow<T> extends FunctionFunctor {

    private Metric nPartitions;

    @Override
    public void initialize(OperatorContext context) throws Exception {
        super.initialize(context);

        StreamWindow<Tuple> window = getInput(0).getStreamWindow();

        createWindowListener(window);

        if (window.isPartitioned()) {
            SPLMapping<? extends Keyable<?>> input0Mapping = getInputMapping(
                    this, 0);
            window.registerPartitioner(new KeyablePartitioner(input0Mapping));
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
            throws ClassNotFoundException;
}
