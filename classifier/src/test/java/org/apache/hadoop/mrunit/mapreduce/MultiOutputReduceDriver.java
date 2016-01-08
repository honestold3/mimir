package org.apache.hadoop.mrunit.mapreduce;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.mock.MockReduceContextWrapper;
import org.apache.hadoop.mrunit.types.Pair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class MultiOutputReduceDriver<K1, V1, K2, V2> extends ReduceDriver<K1, V1, K2, V2> {

    public MultiOutputReduceDriver() {
        super();
    }

    public MultiOutputReduceDriver(Reducer<K1, V1, K2, V2> r) {
        super(r);
    }

    @Override
    public List<Pair<K2, V2>> run() throws IOException {
        List<Pair<K1, List<V1>>> inputs = new ArrayList<Pair<K1, List<V1>>>();
        inputs.add(new Pair<K1, List<V1>>(inputKey, getInputValues()));

        try {
            MockReduceContextWrapper<K1, V1, K2, V2> wrapper =
                    new MockReduceContextWrapper<K1, V1, K2, V2>(inputs, getCounters(), getConfiguration());
            Reducer<K1, V1, K2, V2>.Context context = wrapper.getMockContext();

            when(context.getTaskAttemptID()).thenAnswer(new Answer<TaskAttemptID>() {
                @Override
                public TaskAttemptID answer(InvocationOnMock invocation) throws Throwable {
                    return new TaskAttemptID();
                }
            });

            getReducer().run(context);
            return wrapper.getOutputs();
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }

}