import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AdmissionTrendMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private final Text month = new Text();
    private final Text one = new Text("1");

    @Override
    public void map(LongWritable key,
                    Text value,
                    Context context)
            throws IOException, InterruptedException {

        // Skip header
        if (value.toString().startsWith("admissionId")) {
            return;
        }

        String[] parts = value.toString().split(",");

        // admitDate is column 4
        String admitDate = parts[3];

        // Extract YYYY-MM
        String admissionMonth = admitDate.substring(0, 7);

        month.set(admissionMonth);

        context.write(month, one);
    }
}