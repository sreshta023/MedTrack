import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Problem4Mapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private HashMap<String, String> deptMap = new HashMap<>();
    private final IntWritable one = new IntWritable(1);

    protected void setup(Context context)
            throws IOException, InterruptedException {

        BufferedReader br =
            new BufferedReader(new FileReader("departments.csv"));

        String line;

        while ((line = br.readLine()) != null) {
            if (line.startsWith("deptCode"))
                continue;

            String[] data = line.split(",");

            if (data.length >= 3)
                deptMap.put(data[0].trim(), data[2].trim());
        }

        br.close();
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("admissionId"))
            return;

        String[] data = line.split(",");

        if (data.length >= 3) {
            String deptCode = data[2].trim();
            String head = deptMap.get(deptCode);

            if (head != null)
                context.write(new Text(head), one);
        }
    }
}
