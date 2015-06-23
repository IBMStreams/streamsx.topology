package com.ibm.streamsx.topology.test.api;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import ThirdParty.ThirdPartyResource;
import org.junit.Test;

public class ThirdPartyTest {
    @Test
    public void includeThirdPartyJar() throws Exception {
        final Topology topology = new Topology("BasicStream");
        topology.addThirdPartyDependency("resources/ThirdPartyResource.jar");
        TStream<String> source = topology.strings("1", "2", "3");
        TStream<String> thirdPartyOutput = source.transform(thirdPartyStaticTransform()
							    , String.class);
        
        Tester tester = topology.getTester();
        Condition expectedCount = tester.tupleCount(thirdPartyOutput, 3);
        Condition regionCount = tester.stringContents(thirdPartyOutput, 
                "This was returned from a third-party method1",
                "This was returned from a third-party method2",
                "This was returned from a third-party method3");
    }
    
    @Test
    public void includeThirdPartyClass() throws Exception {
        final Topology topology = new Topology("BasicStream");
        topology.addThirdPartyDependency(ThirdPartyResource.class);
        TStream<String> source = topology.strings("1", "2", "3");
        TStream<String> thirdPartyOutput = source.transform(thirdPartyTransform()
							    , String.class);
        
        Tester tester = topology.getTester();
        Condition expectedCount = tester.tupleCount(thirdPartyOutput, 3);
        Condition regionCount = tester.stringContents(thirdPartyOutput, 
                "This string was set.1",
                "This string was set.2",
                "This string was set.3");
    }

    @SuppressWarnings("serial")
    private static Function<String,String> thirdPartyStaticTransform(){
	return new Function<String, String>(){
            @Override
            public String apply(String v) {
                return ThirdPartyResource.thirdPartyStaticMethod();
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
