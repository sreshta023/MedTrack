import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdmissionTrendReducer
        extends Reducer<Text, Text, Text, Text> {

    private final Text total = new Text();

    @Override
    public void reduce(Text key,
                       Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        int count = 0;

        for (Text value : values) {
            count += Integer.parseInt(value.toString());
        }

        total.set(String.valueOf(count));

        context.write(key, total);
    }
}