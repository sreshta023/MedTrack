import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MonthlyTurnoverReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key,
            Iterable<Text> values,
            Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> departmentCount =
                new HashMap<>();

        for (Text value : values) {

            String deptName = value.toString();

            departmentCount.put(
                    deptName,
                    departmentCount.getOrDefault(
                            deptName, 0
                    ) + 1
            );
        }

        String highestDepartment = "";
        int highestCount = 0;

        for (Map.Entry<String, Integer> entry
                : departmentCount.entrySet()) {

            if (entry.getValue() > highestCount) {

                highestDepartment = entry.getKey();
                highestCount = entry.getValue();
            }
        }

        context.write(
                key,
                new Text(
                        highestDepartment
                        + " (" + highestCount + " patients)"
                )
        );
    }
}
