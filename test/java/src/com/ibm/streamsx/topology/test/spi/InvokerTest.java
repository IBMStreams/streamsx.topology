package com.ibm.streamsx.topology.test.spi;


import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Test;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.TStream.Routing;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.internal.core.JavaFunctionalOps;
import com.ibm.streamsx.topology.spi.builder.Invoker;
import com.ibm.streamsx.topology.spi.runtime.TupleSerializer;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.streams.StringStreams;
import com.ibm.streamsx.topology.test.TestTopology;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

public class InvokerTest extends TestTopology {
    
    // TEMP -  move to TestTopology
    private void checkUdpSupported() {
        assumeTrue(SC_OK);
        assumeTrue(getTesterType() == StreamsContext.Type.STANDALONE_TESTER ||
                getTesterType() == StreamsContext.Type.DISTRIBUTED_TESTER);
    }
    
    public static class ByteSerializer implements TupleSerializer {

        private static final long serialVersionUID = 1L;

        @Override
        public void serialize(Object tuple, OutputStream output) throws IOException {
            output.write(((Byte) tuple).byteValue());       
        }

        @Override
        public Object deserialize(InputStream input) throws IOException, ClassNotFoundException {
            return Byte.valueOf((byte) input.read());
        }    
    }

    public static class ByteData implements Supplier<Iterable<Byte>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<Byte> get() {
            List<Byte> data = new ArrayList<>();
            data.add((byte) 1);
            data.add((byte) 17);
            data.add((byte) 93);
            data.add((byte) -43);
            return data;
        }
        
    }
    

    public static class ByteAdder implements Consumer<Byte>, Function<Object,Object> {
        private static final long serialVersionUID = 1L;
        
        private final int delta;
        
        public ByteAdder(int delta) {
            this.delta = delta;
        }
        
        private Consumer<Object> submitter;
        @SuppressWarnings("unchecked")
        @Override
        public Object apply(Object submitter) {
            this.submitter = (Consumer<Object>) submitter;
            return this;
        }   
        
        @Override
        public void accept(Byte v) {
            submitter.accept(new Byte((byte)(v + delta)));   
        }      
    }
       
    @Test
    public void testSource2PipeSerializers() throws Exception {
        
        assumeSPLOk();

        Topology topology = newTopology();
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        
        JsonObject sname = new JsonObject();
        sname.addProperty("name", "S");
        TStream<Byte> s = Invoker.invokeSource(topology, JavaFunctionalOps.SOURCE_KIND,
                sname,
                new ByteData(), Byte.class, new ByteSerializer(), null);
        
        s = s.isolate();
        
        JsonObject pname = new JsonObject();
        pname.addProperty("name", "P");
        @SuppressWarnings("unchecked")
        TStream<Byte> sp = (TStream<Byte>) Invoker.invokePipe(
                "testjava::MyPipe",
                s,
                pname,
                new ByteAdder(72), Byte.class,
                new ByteSerializer(), null,
                null);
        
        sp = sp.isolate();
         
        TStream<String> ss = StringStreams.toString(sp);
        
        Tester tester = topology.getTester();
        
        Condition<List<String>> contents = tester.stringContents(ss, "73", "89", "-91", "29");
        
        Condition<Long> count = tester.tupleCount(ss, 4);

        complete(topology.getTester(), count, 10, TimeUnit.SECONDS);
        assertTrue(count.valid());
        assertTrue(contents.valid());
    }
    
    @Test
    public void testParallelHashSerializers() throws Exception {
        
        checkUdpSupported();

        Topology topology = newTopology();
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        
        JsonObject sname = new JsonObject();
        sname.addProperty("name", "S");
        TStream<Byte> s = Invoker.invokeSource(topology, JavaFunctionalOps.SOURCE_KIND,
                sname,
                new ByteData(), Byte.class, new ByteSerializer(), null);
             
        s = s.parallel(() -> 3, Routing.HASH_PARTITIONED);
        
        JsonObject pname = new JsonObject();
        pname.addProperty("name", "P");
        @SuppressWarnings("unchecked")
        TStream<Byte> sp = (TStream<Byte>) Invoker.invokePipe(
                "testjava::MyPipe",
                s,
                pname,
                new ByteAdder(32), Byte.class,
                new ByteSerializer(), new ByteSerializer(),
                null);
         
        
        sp = sp.endParallel();
        JsonObject fname = new JsonObject();
        fname.addProperty("name", "F");
        
        @SuppressWarnings("unchecked")
        TStream<Byte> fsp = (TStream<Byte>) Invoker.invokePipe(
                "testjava::MyPipe",
                sp,
                pname,
                new ByteAdder(0), Byte.class,
                new ByteSerializer(), null,
                null);
        
        TStream<String> ss = StringStreams.toString(fsp);
        
        Tester tester = topology.getTester();
        
        Condition<List<String>> contents = tester.stringContentsUnordered(ss, "33", "49", "125", "-11");
        
        Condition<Long> count = tester.tupleCount(ss, 4);

        complete(topology.getTester(), count, 10, TimeUnit.SECONDS);
        assertTrue(count.valid());
        assertTrue(contents.valid());
    }
    
    @Test
    public void testParallelHashSerializersWithVirtuals() throws Exception {
        
        checkUdpSupported();

        Topology topology = newTopology();
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        
        JsonObject sname = new JsonObject();
        sname.addProperty("name", "S1");
        TStream<Byte> s = Invoker.invokeSource(topology, JavaFunctionalOps.SOURCE_KIND,
                sname,
                new ByteData(), Byte.class, new ByteSerializer(), null);
        
        JsonObject sname2 = new JsonObject();
        sname2.addProperty("name", "S2");
        TStream<Byte> s2 = Invoker.invokeSource(topology, JavaFunctionalOps.SOURCE_KIND,
                sname2,
                new ByteData(), Byte.class, new ByteSerializer(), null);
        
        s = s.union(s2);     
        s = s.isolate();       
        s = s.autonomous();
        
        s = s.lowLatency();
        
        JsonObject llname = new JsonObject();
        llname.addProperty("name", "LL");
        @SuppressWarnings("unchecked")
        TStream<Byte> ll = (TStream<Byte>) Invoker.invokePipe(
                "testjava::MyPipe",
                s,
                llname,
                new ByteAdder(3), Byte.class,
                new ByteSerializer(), new ByteSerializer(),
                null);
        s = ll.endLowLatency();
             
        s = s.parallel(() -> 3, Routing.HASH_PARTITIONED);
        
        JsonObject pname = new JsonObject();
        pname.addProperty("name", "P");
        @SuppressWarnings("unchecked")
        TStream<Byte> sp = (TStream<Byte>) Invoker.invokePipe(
                "testjava::MyPipe",
                s,
                pname,
                new ByteAdder(29), Byte.class,
                new ByteSerializer(), new ByteSerializer(),
                null);
         
        
        sp = sp.endParallel();
        JsonObject fname = new JsonObject();
        fname.addProperty("name", "F");
        
        @SuppressWarnings("unchecked")
        TStream<Byte> fsp = (TStream<Byte>) Invoker.invokePipe(
                "testjava::MyPipe",
                sp,
                pname,
                new ByteAdder(0), Byte.class,
                new ByteSerializer(), null,
                null);
        
        TStream<String> ss = StringStreams.toString(fsp);
        
        Tester tester = topology.getTester();
        
        Condition<List<String>> contents = tester.stringContentsUnordered(ss,
                "33", "49", "125", "-11", "33", "49", "125", "-11");
        
        Condition<Long> count = tester.tupleCount(ss, 8);

        complete(topology.getTester(), count, 10, TimeUnit.SECONDS);
        assertTrue(count.valid());
        assertTrue(contents.valid());
    }
    
    @Test
    public void testParallelHashSerializersWithAdjcentUDP() throws Exception {
        
        checkUdpSupported();

        Topology topology = newTopology();
        SPL.addToolkit(topology, new File(getTestRoot(), "spl/testtk"));
        
        JsonObject sname = new JsonObject();
        sname.addProperty("name", "S");
        TStream<Byte> s = Invoker.invokeSource(topology, JavaFunctionalOps.SOURCE_KIND,
                sname,
                new ByteData(), Byte.class, new ByteSerializer(), null);
        
        s = s.setParallel(()->2);
        s = s.endParallel();
             
        s = s.parallel(() -> 3, Routing.HASH_PARTITIONED);
        
        JsonObject pname = new JsonObject();
        pname.addProperty("name", "P");
        @SuppressWarnings("unchecked")
        TStream<Byte> sp = (TStream<Byte>) Invoker.invokePipe(
                "testjava::MyPipe",
                s,
                pname,
                new ByteAdder(32), Byte.class,
                new ByteSerializer(), new ByteSerializer(),
                null);
         
        
        sp = sp.endParallel();
        JsonObject fname = new JsonObject();
        fname.addProperty("name", "F");
        
        @SuppressWarnings("unchecked")
        TStream<Byte> fsp = (TStream<Byte>) Invoker.invokePipe(
                "testjava::MyPipe",
                sp,
                pname,
                new ByteAdder(0), Byte.class,
                new ByteSerializer(), null,
                null);
        
        TStream<String> ss = StringStreams.toString(fsp);
        
        Tester tester = topology.getTester();
        
        // Note the source is in a parallel region of width 2 wo we get twice as many tuples.
        Condition<List<String>> contents = tester.stringContentsUnordered(ss, "33", "49", "125", "-11", "33", "49", "125", "-11");       
        Condition<Long> count = tester.tupleCount(ss, 8);

        complete(topology.getTester(), count, 10, TimeUnit.SECONDS);
        assertTrue(count.valid());
        assertTrue(contents.valid());
    }
}
