import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MedTrackMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, String[]> deptMap = new HashMap<>();
    private String problemType;

    private SimpleDateFormat sdf =
            new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void setup(Context context) throws IOException {

        problemType = context.getConfiguration()
                .get("problem.type", "P1");

        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {

            BufferedReader reader =
                    new BufferedReader(
                            new FileReader("departments.csv")
                    );

            String line;

            while ((line = reader.readLine()) != null) {

                if (line.startsWith("deptCode")
                        || line.trim().isEmpty()) {
                    continue;
                }

                String[] tokens = line.split(",");

                if (tokens.length >= 3) {

                    deptMap.put(
                            tokens[0].trim(),
                            new String[]{
                                    tokens[1].trim(),
                                    tokens[2].trim()
                            }
                    );
                }
            }

            reader.close();
        }
    }

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.startsWith("admissionId")
                || line.isEmpty()) {
            return;
        }

        String[] parts = line.split(",");

        if (parts.length < 5) {
            return;
        }

        String deptCode = parts[2].trim();
        String admitDate = parts[3].trim();
        String dischargeDate = parts[4].trim();

        String[] deptInfo = deptMap.get(deptCode);

        String deptName =
                (deptInfo != null)
                        ? deptInfo[0]
                        : "Unknown";

        String headOfDept =
                (deptInfo != null)
                        ? deptInfo[1]
                        : "Unknown";

        long stayDays = 0;

        try {

            stayDays =
                    (sdf.parse(dischargeDate).getTime()
                    - sdf.parse(admitDate).getTime())
                    / (1000 * 60 * 60 * 24);

        } catch (Exception ignored) {
        }

        String month =
                (admitDate.length() >= 7)
                        ? admitDate.substring(0, 7)
                        : "Unknown";


        // Problem 1:
        // Total number of patients admitted to each department

        if ("P1".equals(problemType)) {

            context.write(
                    new Text(deptName),
                    new Text("1")
            );
        }


        // Problem 2:
        // Average length of stay per department

        else if ("P2".equals(problemType)) {

            context.write(
                    new Text(deptName),
                    new Text(String.valueOf(stayDays))
            );
        }


        // Problem 3:
        // Find department with highest patient turnover in each month

        else if ("P3".equals(problemType)) {

            context.write(
                    new Text(month),
                    new Text(deptName)
            );
        }


        // Problem 4:
        // Find department head with most patient admissions overall

        else if ("P4".equals(problemType)) {

            context.write(
                    new Text("ALL"),
                    new Text(headOfDept)
            );
        }


        // Problem 5:
        // Month-wise admission trend for the hospital

        else if ("P5".equals(problemType)) {

            context.write(
                    new Text(month),
                    new Text("1")
            );
        }
    }
}