/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package testjava;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.RString;

@PrimitiveOperator
@InputPortSet(cardinality = 1)
@OutputPortSet(cardinality = 1)
public class NoOpJavaPrimitive extends AbstractOperator {

    @Override
    public void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        
        String str = tuple.getString(0);
        
        getOutput(0).submitAsTuple(new RString(str));
    }
    
}