import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class AgeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count > 0) {
            context.write(new Text("Average Age"), new FloatWritable(sum / count));
        }
    }
}
