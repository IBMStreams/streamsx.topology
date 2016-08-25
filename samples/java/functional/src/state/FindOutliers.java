/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package state;

import java.util.Random;

import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;

/**
 * Finds outliers from a sequence of doubles (e.g. simulating a sensor reading).
 * 
 * Demonstrates function logic that maintains state across tuples.
 *
 */
public class FindOutliers {

    public static void main(String[] args) throws Exception {

        final double threshold = args.length == 0 ? 2.0 : Double
                .parseDouble(args[0]);

        Topology t = new Topology("StandardDeviationFilter");

        final Random rand = new Random();

        // Produce a stream of random double values with a normal
        // distribution, mean 0.0 and standard deviation 1.
        TStream<Double> values = t.limitedSource(new Supplier<Double>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Double get() {
                return rand.nextGaussian();
            }

        }, 100000);

        /*
         * Filters the values based on calculating the mean and standard
         * deviation from the incoming data. In this case only outliers are
         * present in the output stream outliers. A outlier is defined as one
         * more than (threshold*standard deviation) from the mean.
         * 
         * This demonstrates an anonymous functional logic class that is
         * stateful. The two fields mean and sd maintain their values across
         * multiple invocations of the test method, that is for multiple tuples.
         * 
         * Note both Mean & StandardDeviation classes are serializable.
         */
        TStream<Double> outliers = values.filter(new Predicate<Double>() {

            private static final long serialVersionUID = 1L;
            private final Mean mean = new Mean();
            private final StandardDeviation sd = new StandardDeviation();

            @Override
            public boolean test(Double tuple) {
                mean.increment(tuple);
                sd.increment(tuple);

                double multpleSd = threshold * sd.getResult();
                double absMean = Math.abs(mean.getResult());
                double absTuple = Math.abs(tuple);

                return absTuple > absMean + multpleSd;
            }
        });

        outliers.print();

        StreamsContextFactory.getEmbedded().submit(t).get();
    }
}
