package com.ibm.streamsx.rest;

import java.io.IOException;

class ResultImpl<T,R> implements Result<T, R> {
    
    private final String id;
    private final ElementSupplier<T> elementGetter;
    private final R rawResult;
    ResultImpl(String id, ElementSupplier<T> elementGetter, R rawResult) {
        this.id = id;
        this.elementGetter = elementGetter;
        this.rawResult = rawResult;
    }

    @Override
    public final R getRawResut() {
        return rawResult;
    }

    @Override
    public final T getElement() throws IOException {
        return elementGetter.get();
    }

    @Override
    public final String getId() {
        return id;
    }
    
    @FunctionalInterface
    static interface ElementSupplier<T> {
        T get() throws IOException;
    }
}
