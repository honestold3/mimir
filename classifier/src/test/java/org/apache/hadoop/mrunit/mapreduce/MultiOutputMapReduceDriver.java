package org.apache.hadoop.mrunit.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

@SuppressWarnings("rawtypes")
public class MultiOutputMapReduceDriver<K1, V1, K2 extends Comparable, V2, K3, V3> extends
        MapReduceDriver<K1, V1, K2, V2, K3, V3> {

    public MultiOutputMapReduceDriver() {
        super();
    }

    public MultiOutputMapReduceDriver(Mapper<K1, V1, K2, V2> m, Reducer<K2, V2, K3, V3> r) {
        super(m, r);
    }

    @Override
    public List<Pair<K3, V3>> run() throws IOException {

        List<Pair<K2, V2>> mapOutputs = new ArrayList<Pair<K2, V2>>();

        // run map component
        for (Pair<K1, V1> input : inputList) {
            LOG.debug("Mapping input " + input.toString() + ")");

            mapOutputs.addAll(new MapDriver<K1, V1, K2, V2>(getMapper()).withInput(
                    input).withCounters(getCounters()).withConfiguration(configuration).run());
        }

        List<Pair<K2, List<V2>>> reduceInputs = shuffle(mapOutputs);
        List<Pair<K3, V3>> reduceOutputs = new ArrayList<Pair<K3, V3>>();

        for (Pair<K2, List<V2>> input : reduceInputs) {
            K2 inputKey = input.getFirst();
            List<V2> inputValues = input.getSecond();
            StringBuilder sb = new StringBuilder();
            formatValueList(inputValues, sb);
            LOG.debug("Reducing input (" + inputKey.toString() + ", "
                    + sb.toString() + ")");

            reduceOutputs.addAll(new MultiOutputReduceDriver<K2, V2, K3, V3>(getReducer())
                    .withCounters(getCounters()).withConfiguration(configuration)
                    .withInputKey(inputKey).withInputValues(inputValues).run());
        }

        return reduceOutputs;

    }

}
