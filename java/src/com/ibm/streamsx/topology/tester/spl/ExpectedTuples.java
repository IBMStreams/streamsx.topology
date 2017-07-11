package com.ibm.streamsx.topology.tester.spl;

import java.util.ArrayList;
import java.util.List;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/**
 * List of expected tuples.
 * Utility class to build a list
 * of expected SPL tuples for testing.
 *
 */
public class ExpectedTuples {
    
    private final static Tuple[] EMPTY = new Tuple[0];
    
    private final StreamSchema schema;
    private final List<Tuple> tuples = new ArrayList<>();
    
    /**
     * Create an initially empty ExpectedTuples.
     * @param schema
     */
    public ExpectedTuples(StreamSchema schema) {
        this.schema = schema;
    }
    
    /**
     * Get the schema of the tuples.
     * @return schema of the tuples.
     */
    public StreamSchema getSchema() {
        return schema;
    }
    
    /**
     * Get the expected tuples. 
     * Modifying the returned list modifies
     * the expected tuples.
     *  
     * @return list of expected tuples.
     */
    public List<Tuple> getTuples() {
        return tuples;
    }
    
    /**
     * Add a tuple to expected list.
     * The schema of the tuple must be the
     * same as {@link #getSchema()}.
     * @param tuple Tuple to be added to expected list.
     * @return This.
     */
    public ExpectedTuples add(Tuple tuple) {
        checkSchema(tuple.getStreamSchema());
        getTuples().add(tuple);
        return this;
    }
    
    /**
     * Add a tuple to expected list.
     * Equivalent to:
     * {@code add(getSchema().getTuple(values))}
     * with the exception that if an attribute is
     * of type {@code rstring} then a String object may be used.
     * <P>
     * Calls can be chained together to add multiple tuples.
     * For example with a schema of {@code tuple<rstring id, int32 value>}
     * this may be used to add two tuples:
     * <pre>
     * <code>
     * ExpectedTuples expected = new ExpectedTuples(schema);
     * 
     * expected.addAsTuple("a21", 33).addAsTuple("c43", 932);
     * </code>
     * </pre>
     * </P>

     * @param values Attribute values for the tuple.
     * @return This.
     */
    public ExpectedTuples addAsTuple(Object ...values) {
        for (int i = 0; i < values.length; i++) {
            if (getSchema().getAttribute(i).getType().getMetaType() == MetaType.RSTRING) {
                if (values[i] instanceof String)
                    values[i] = new RString(values[i].toString());
            }
        }
        return add(getSchema().getTuple(values));
    }
    
    private void checkSchema(StreamSchema schema) {
        if (!getSchema().equals(schema))
            throw new IllegalArgumentException("Mismatched schemas, expecting " + getSchema() + " passed " + schema);
    }
    
    /**
     * Create a condition for {@code stream} having the tuples
     * {@link #getTuples()} in the same order.
     * {@code stream.getSchema()} must be equal to {@link #getSchema()}.
     * 
     * @param stream Stream the condition is for.
     * @return Condition that will be valid if {@code stream} has the same tuples as {@link #getTuples()}.
     * 
     * @see Tester#tupleContents(SPLStream, Tuple...)
     */
    public Condition<List<Tuple>> contents(SPLStream stream) {
        checkSchema(stream.getSchema());
                    
        return stream.topology().getTester().tupleContents(stream, getTuples().toArray(EMPTY));
    }
}
