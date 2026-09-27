import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Problem4Reducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int max = 0;
    private ArrayList<String> maxHeads = new ArrayList<>();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values)
            sum += value.get();

        if (sum > max) {
            max = sum;
            maxHeads.clear();
            maxHeads.add(key.toString());
        }
        else if (sum == max) {
            maxHeads.add(key.toString());
        }
    }

    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        for (String head : maxHeads)
            context.write(new Text(head), new IntWritable(max));
    }
}
