/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020  
 */
package simple;

import java.util.Arrays;
import java.util.Random;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.TWindow;

/**
 * Sample topology application. This Java application builds a
 * simple topology that demonstrates the usage of submission time parameter for time based window. <BR>
 * The application implements the typical pattern of code that declares a
 * topology followed by submission of the topology to a Streams context
 * {@code com.ibm.streamsx.topology.context.StreamsContext}.
 * <BR>
 * This demonstrates the mechanics of declaring a topology with submission time parameter and executing it.
 * <P>
 * This may be executed from the {@code samples/java/functional} directory as:
 * <UL>
 * <LI>{@code ant run.temperaturesensor} - Using Apache Ant, this will run in standalone
 * mode.</LI>
 * <LI>{@code ant run.temperaturesensor.bundle} - Using Apache Ant, this will create the bundle only.</LI>
 * <LI>
 * {@code java -jar funcsamples.jar:../com.ibm.streamsx.topology/lib/com.ibm.streamsx.topology.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar
 *  simple.TemperatureSensor [CONTEXT_TYPE] 
 * } - Run directly from the command line.
 * </LI>
 * If no arguments are provided then the topology is executed in bundle mode.
 * <BR>
 * <i>CONTEXT_TYPE</i> is one of:
 * <UL>
 * <LI>{@code DISTRIBUTED} - Run as an IBM Streams distributed
 * application.</LI>
 * <LI>{@code STANDALONE} - Run as an IBM Streams standalone
 * application.</LI>
 * <LI>{@code BUNDLE} - Create an IBM Streams application bundle.</LI>
 * <LI>{@code TOOLKIT} - Create an IBM Streams application toolkit.</LI>
 * </UL>
 * </LI>
 * <LI>
 * An application execution within your IDE once you set the class path to include the correct jars.</LI>
 * </UL>
 * </P>
 */
public class TemperatureSensor {

    /**
     * Sample topology application.
     * This Java application builds a simple topology
     * that demonstrates the usage of submission time parameter for time based window
     * and finally prints the aggregates values to standard output.
     * <BR>
     * The application implements the typical pattern
     * of code that declares a topology followed by
     * submission of the topology to a Streams context
     * (@code com.ibm.streamsx.topology.context.StreamsContext}.

     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        /*
         * Create the container for the topology that will
         * hold the streams of tuples.
         */
        Topology topology = new Topology("TemperatureSensor");

        Random random = new Random();

        @SuppressWarnings("unchecked")
        TStream<Double> readings = topology.endlessSource(new Supplier<Double>(){
            @Override
            public Double get() {
                return random.nextGaussian();
            }

        });

        Supplier<Integer> time = topology.createSubmissionParameter("time", 5);
        TWindow<Double, ?> lastNSeconds = readings.lastSeconds(time);
        
        TStream<Double> maxTemp = lastNSeconds.aggregate(new Function<List<Double>, Double>(){
            @Override
            public Double apply(List<Double> temps) {
                Double max = temps.get(0);
                for(Double temp : temps){
                    if(temp > max)
                        max = temp;
                }
                return max;
            }

        });
        maxTemp.print();

        if (args.length == 0)
            StreamsContextFactory.getStreamsContext("BUNDLE").submit(topology).get();
        else
            StreamsContextFactory.getStreamsContext(args[0]).submit(topology)
                    .get();
    }
}
