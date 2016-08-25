/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test.perf;

import java.io.Serializable;
import java.math.BigDecimal;

public class TestValue implements Serializable {

    private static final long serialVersionUID = 1L;
    public long i;
    public long l;
    public double d;
    public String s;

    public TestSubValue tsv;

    public TestValue(TestValue other) {
        this.i = other.i;
        this.l = other.l;
        this.d = other.d;
        this.s = other.s;
        this.tsv = other.tsv;
    }

    public TestValue(Long c) {
        this.i = c;
        this.l = c * 234532452l;
        this.d = ((double) c) * 34643653.234;
        this.s = "TestValue:" + c;
        this.tsv = new TestSubValue();
        tsv.bd = new BigDecimal(d);
    }
}
