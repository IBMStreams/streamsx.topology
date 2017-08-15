package com.ibm.streamsx.topology.internal.functional.operators;

import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator
@Libraries("lib/com.ibm.streamsx.topology.api.jar")
public class Source extends com.ibm.streamsx.topology.spi.operators.Source {
}