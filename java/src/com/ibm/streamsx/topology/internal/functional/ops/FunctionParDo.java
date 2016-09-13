/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.ops;

import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getOutputMapping;
import static com.ibm.streamsx.topology.internal.functional.FunctionalHelper.getInputMapping;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.window.StreamWindow;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.internal.functional.FunctionalHandler;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.internal.functional.window.ViewList;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * This class is the counterpart for {@link org.apache.beam.sdk.transforms.ParDo ParDo}
 * transform in Beam. {@link FunctionParDo} is a multi-input multi-output 
 * transform. Internally, it takes a user-defined {@link BiFunction} whose first 
 * parameter is the tuple from the main input stream, second parameter is a map
 * of tuples in the window from all sideinputs, and its return type is a map
 * of tuples to all outputs. 
 * */
@PrimitiveOperator
@InputPorts({
        @InputPortSet(cardinality = 1),
        @InputPortSet(cardinality = -1, windowingMode = WindowMode.Windowed) })
@OutputPortSet(cardinality = -1)
public class FunctionParDo<InputT> extends FunctionFunctor {

    private FunctionalHandler<BiFunction<InputT, 
                                         Map<Integer, List<?>>, 
                                         Map<Integer, List<?>>>> doFnHandler;
    private SPLMapping<InputT> inputMapping;
    private List<SPLMapping<?>> outputMappings;
    private int oNum;
    private int iNum;
    private List<StreamingOutput<OutputTuple>> oports; 
    private List<StreamingInput<Tuple>> iports;
    private List<ViewList> views;

    @Override
    public void initialize(OperatorContext context) throws Exception {
        super.initialize(context);

        doFnHandler = createLogicHandler();
        
        OperatorContext ctxt = getOperatorContext();
        oports = ctxt.getStreamingOutputs();
        oNum = oports.size();

        iports = ctxt.getStreamingInputs();
        iNum = iports.size();

        // with side inputs
        if (iNum > 1) {
            views = new ArrayList<ViewList> ();
            StreamWindow<Tuple> window = null;
            for (int i = 1; i < iNum; ++i) {
                window = iports.get(i).getStreamWindow();
                views.add(new ViewList<Object>(window, this, i));
            }
        }
        
        inputMapping = getInputMapping(this, 0);

        outputMappings = new ArrayList<SPLMapping<?>>(oNum);
        for (int i = 0; i < oNum; ++i) {
            outputMappings.add(getOutputMapping(this, i));
        }
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple splTuple)
            throws Exception {
        // types: input, views, outputs
        final BiFunction<InputT, 
                        Map<Integer, List<?>>, 
                        Map<Integer, List<?>>> doFn = 
            doFnHandler.getLogic();

        // only tuples from main input triggers DoFn
        if (stream.getPortNumber() != 0)
            return;

        Map<Integer, List<?>> viewMap = 
            new HashMap<Integer, List<?>> ();
        List<?> view = null;
        for (int i = 0; i < iNum - 1; ++i) {
            view = views.get(i).getView();
            if (view != null) {
                viewMap.put(i, view);
            }
        }

        InputT tuple = inputMapping.convertFrom(splTuple);
        Map<Integer, List<?>> outputs;

        synchronized (doFn) {
            outputs = doFn.apply(tuple, viewMap);
        }

        if (outputs != null) {
            for (Map.Entry<Integer, List<?>> entry: outputs.entrySet()) {
                if (entry.getValue() != null && entry.getValue().size() > 0) {
                    int portId = entry.getKey();
                    submitOutputs(entry.getValue(), oports.get(portId), 
                        outputMappings.get(portId));
                }
            }
        }
    }

    private <T> void submitOutputs(List<T> tuples, 
        StreamingOutput<OutputTuple> oport, SPLMapping<?> outputMapping) throws Exception {
        @SuppressWarnings("unchecked")
        SPLMapping<T> typedMapping = (SPLMapping<T>) outputMapping;
        for (T tuple: tuples) {
            oport.submit(typedMapping.convertTo(tuple));
        }
    }

}
