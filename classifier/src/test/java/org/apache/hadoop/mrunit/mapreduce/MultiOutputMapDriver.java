package org.apache.hadoop.mrunit.mapreduce;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mrunit.mapreduce.mock.MockInputSplit;
import org.apache.hadoop.mrunit.mapreduce.mock.MockMapContextWrapper;
import org.apache.hadoop.mrunit.types.Pair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

public class MultiOutputMapDriver<K1, V1, K2, V2> extends MapDriver<K1, V1, K2, V2>
{
    public MultiOutputMapDriver() { super(); }

    public MultiOutputMapDriver(Mapper<K1, V1, K2, V2> r) { super(r); }

    @Override
    public List<Pair<K2, V2>> run() throws IOException
    {
        List<Pair<K1, V1>> inputs = new ArrayList<Pair<K1, V1>>();
        inputs.add(new Pair<K1, V1>(inputKey, inputVal));

        try {
            final InputSplit inputSplit = new MockInputSplit();
            MockMapContextWrapper<K1, V1, K2, V2> wrapper =
                    new MockMapContextWrapper<K1, V1, K2, V2>(inputs, getCounters(), getConfiguration(), inputSplit);

            @SuppressWarnings("unchecked")
            Mapper<K1, V1, K2, V2>.Context context = wrapper.getMockContext();

            when(context.getTaskAttemptID()).thenAnswer(new Answer<TaskAttemptID>() {
                @Override
                public TaskAttemptID answer(InvocationOnMock invocation) throws Throwable {
                    return new TaskAttemptID();
                }
            });

            getMapper().run(context);
            return wrapper.getOutputs();
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }
}
