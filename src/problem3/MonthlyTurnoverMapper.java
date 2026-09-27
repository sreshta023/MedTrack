import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MonthlyTurnoverMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, String> deptMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        BufferedReader reader =
                new BufferedReader(
                        new FileReader("departments.csv")
                );

        String line;

        while ((line = reader.readLine()) != null) {

            if (line.startsWith("deptCode") || line.trim().isEmpty()) {
                continue;
            }

            String[] parts = line.split(",");

            if (parts.length >= 2) {
                deptMap.put(
                        parts[0].trim(),
                        parts[1].trim()
                );
            }
        }

        reader.close();
    }

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.startsWith("admissionId") || line.isEmpty()) {
            return;
        }

        String[] parts = line.split(",");

        if (parts.length < 5) {
            return;
        }

        String deptCode = parts[2].trim();
        String admitDate = parts[3].trim();

        String deptName = deptMap.get(deptCode);

        if (deptName == null) {
            return;
        }

        String month = admitDate.substring(0, 7);

        context.write(
                new Text(month),
                new Text(deptName)
        );
    }
}
