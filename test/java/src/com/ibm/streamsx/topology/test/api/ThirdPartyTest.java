package com.ibm.streamsx.topology.test.api;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

import java.util.List;

import static org.junit.Assert.assertTrue;
import ThirdParty.ThirdPartyResource;
import org.junit.Test;

public class ThirdPartyTest {
    @Test
    public void includeThirdPartyJar() throws Exception {
	String resourceDir = System.getProperty("topology.test.resource_dir");

        final Topology topology = new Topology("BasicStream");
        topology.addJarDependency(resourceDir + "/ThirdPartyResource.jar");
        TStream<String> source = topology.strings("1", "2", "3");
        TStream<String> thirdPartyOutput = source.transform(thirdPartyStaticTransform()
							    , String.class);
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(thirdPartyOutput, 3);
        Condition<List<String>> stringContents = tester.stringContents(thirdPartyOutput, 
                "This was returned from a third-party method1",
                "This was returned from a third-party method2",
                "This was returned from a third-party method3");
	StreamsContextFactory
	    .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
	    .submit(topology).get();
	assertTrue(expectedCount.valid());
        assertTrue(stringContents.valid());
    }

    @Test
    public void includeThirdPartyClass() throws Exception {
        final Topology topology = new Topology("BasicStream");
        topology.addClassDependency(ThirdPartyResource.class);
        TStream<String> source = topology.strings("1", "2", "3");
        TStream<String> thirdPartyOutput = source.transform(thirdPartyTransform()
							    , String.class);
        
        Tester tester = topology.getTester();
        Condition<Long> expectedCount = tester.tupleCount(thirdPartyOutput, 3);
        Condition<List<String>> stringContents = tester.stringContents(thirdPartyOutput, 
                "This string was set.1",
                "This string was set.2",
                "This string was set.3");
        StreamsContextFactory
	    .getStreamsContext(StreamsContext.Type.STANDALONE_TESTER)
	    .submit(topology).get();
        assertTrue(expectedCount.valid());
        assertTrue(stringContents.valid());
    }

    @SuppressWarnings("serial")
    private static Function<String,String> thirdPartyStaticTransform(){
	return new Function<String, String>(){
            @Override
            public String apply(String v) {
                return ThirdPartyResource.thirdPartyStaticMethod() + v;
            }
        };
    }

    @SuppressWarnings("serial")
    private static Function<String,String> thirdPartyTransform(){
	return new Function<String, String>(){
            private ThirdPartyResource tpr = new ThirdPartyResource("This string was set.");
            
	    @Override
	    public String apply(String v) {            
                return tpr.thirdPartyMethod() + v;
            }
        };
    }
}
