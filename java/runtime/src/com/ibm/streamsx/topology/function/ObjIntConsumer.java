package com.ibm.streamsx.topology.function;

import java.io.Serializable;

public interface ObjIntConsumer<T> extends Serializable {
    void accept(T v, int i);
}
