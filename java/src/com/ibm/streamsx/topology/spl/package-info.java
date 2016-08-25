/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2016  
 */
/**
 * Integration between Streams SPL and Java applications.
 * Using methods in {@link com.ibm.streamsx.topology.spl.SPL SPL} and
 * {@link com.ibm.streamsx.topology.spl.JavaPrimitive Java primitives}
 * a topology can include invocations of any SPL primitive or composite operator.
 * <h4>Operator Kind</h4>
 * An SPL operator is invoked by its <em>kind</em>.
 * An operator's kind is its namespace followed by the
 * operator name separated with '{@code ::}'. For example
 * the {@code FileSource} operator from the SPL Standard toolkit must be specified
 * as:
 * <BR>{@code spl.adapter::FileSource}.
 * <h4>Operator Parameters</h4>
 * Invocation of an {@link com.ibm.streamsx.topology.spl.SPL SPL} operator (including
 * {@link com.ibm.streamsx.topology.spl.JavaPrimitive Java primitives})
 * optionally includes a set of parameters. The parameters are provided as
 * a {@code Map<String,Object>}. The {@code String} key is the parameter
 * name and the value is the parameter value.
 * <BR>
 * Types are mapped as:
 * <UL>
 * <LI><B>SPL type</B> : <B>Java type</B></LI>
 * <LI>{@code boolean} : {@code java.lang.Boolean}</LI>
 * <LI>{@code int8} : {@code java.lang.Byte}</LI>
 * <LI>{@code int16} : {@code java.lang.Short}</LI>
 * <LI>{@code int32} : {@code java.lang.Integer}</LI>
 * <LI>{@code int64} : {@code java.lang.Long}</LI>
 * <LI>{@code float32} : {@code java.lang.Float}</LI>
 * <LI>{@code float64} : {@code java.lang.Double}</LI>
 * <LI>{@code rstring} : {@code java.lang.String}</LI>
 * <LI>{@code uint8} : Object returned by {@code SPL.createValue(Byte value, MetaType.UINT8)}</LI>
 * <LI>{@code uint16} : Object returned by {@code SPL.createValue(Short value, MetaType.UINT16)}</LI>
 * <LI>{@code uint32} : Object returned by {@code SPL.createValue(Integer value, MetaType.UINT32)}</LI>
 * <LI>{@code uint64} : Object returned by {@code SPL.createValue(Long value, MetaType.UINT64)}</LI>
 * <LI>{@code ustring} : Object returned by {@code SPL.createValue(String value, MetaType.USTRING)}</LI>
 * <LI><em>enum</em> : Java enumeration with constants matching the SPL enum's constants.</LI>
 * </UL>
 * When a primitive operator's parameter cardinality allows multiple values the parameter
 * value is a {@code java.util.Collection} containing values of the correct type, typically
 * a {@code java.util.List} or {@code java.util.Set}.
 * <BR>
 * Operator parameters can also be passed as submission parameters, see
 * {@link com.ibm.streamsx.topology.spl.SPL} for examples.
 * <h4>Full use of SPL capabilities</h4>
 * Only invocation of SPL operators is supported, functionality such as SPL logic clauses,
 * function invocations, annotations etc. are not supported.
 * These limitations can typically be worked around by creating an SPL composite operator
 * that uses the full language capabilities of SPL and then invoking the composite from this API.
 */
package com.ibm.streamsx.topology.spl;

