import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedTrackReducer extends Reducer<Text, Text, Text, Text> {

    private String problem;

    @Override
    protected void setup(Context context) {
        // Read the problem type passed from the driver (e.g., P1, P2, etc.)
        problem = context.getConfiguration().get("problem.type", "P1");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // --- Problem 2: Average stay days per department ---
        if ("P2".equals(problem)) {
            double totalDays = 0;
            int count = 0;

            for (Text val : values) {
                totalDays += Double.parseDouble(val.toString().trim());
                count++;
            }

            double avg = count > 0 ? (totalDays / count) : 0;
            context.write(key, new Text(String.format(": %.2f days", avg)));
        }

        // --- Problem 3: Department with highest turnover in each month ---
        else if ("P3".equals(problem)) {
            Map<String, Integer> counts = new HashMap<>();

            for (Text val : values) {
                String dept = val.toString().trim();
                counts.put(dept, counts.getOrDefault(dept, 0) + 1);
            }

            String topDept = "";
            int max = 0;
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                if (entry.getValue() > max) {
                    max = entry.getValue();
                    topDept = entry.getKey();
                }
            }

            context.write(key, new Text(": " + topDept + " (" + max + " patients)"));
        }

        // --- Problem 4: Department head with most admissions overall ---
        else if ("P4".equals(problem)) {
            Map<String, Integer> counts = new HashMap<>();

            for (Text val : values) {
                String head = val.toString().trim();
                counts.put(head, counts.getOrDefault(head, 0) + 1);
            }

            String topHead = "";
            int max = 0;
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                if (entry.getValue() > max) {
                    max = entry.getValue();
                    topHead = entry.getKey();
                }
            }

            context.write(new Text("Top Head"), new Text(": " + topHead + " (" + max + " patients)"));
        }

        // --- Problem 1 and Problem 5: Simple Sum of admissions ---
        else {
            int total = 0;
            for (Text val : values) {
                total += Integer.parseInt(val.toString().trim());
            }
            context.write(key, new Text(": " + total + " admissions"));
        }
    }
}