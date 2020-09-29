package topo1;

import java.util.Random;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Supplier;

public class App {
    public static void main(String[] args) throws Exception{

        Topology topology = new Topology("temperatureSensor");
        Random random = new Random();

        TStream<Double> readings = topology.endlessSource(new Supplier<Double>(){
            @Override
            public Double get() {
                return random.nextGaussian();
            }
        });

        readings.print();

        //StreamsContextFactory.getEmbedded().submit(topology);
        StreamsContextFactory.getStreamsContext("BUNDLE").submit(topology);	

    }
}
