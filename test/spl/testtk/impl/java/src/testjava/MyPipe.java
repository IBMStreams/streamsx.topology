/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
 */
package testjava;

import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.PrimitiveOperator;

import com.ibm.streamsx.topology.spi.operators.Pipe;
import java.util.function.Function;

@PrimitiveOperator
@Libraries("impl/java/lib/com.ibm.streamsx.topology.jar")
public class MyPipe extends Pipe {

    public void initialize() throws Exception {
        super.initialize();
           
        ((Function<Object,Object>) getLogic()).apply(this.submitter());
    }
}
