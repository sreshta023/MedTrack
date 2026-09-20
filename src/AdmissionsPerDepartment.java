import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AdmissionsPerDepartment {

    public static class JoinMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        Map<String, String> departments = new HashMap<>();

        private final Text departmentName = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context)
                throws IOException {

            URI[] files = context.getCacheFiles();

            if (files != null) {

                for (URI file : files) {

                    String fileName =
                            new Path(file.getPath()).getName();

                    BufferedReader br = new BufferedReader(
                            new FileReader(fileName)
                    );

                    String line;

                    while ((line = br.readLine()) != null) {

                        if (line.startsWith("deptCode")) {
                            continue;
                        }

                        String[] parts = line.split(",");

                        if (parts.length >= 2) {

                            departments.put(
                                    parts[0].trim(),
                                    parts[1].trim()
                            );
                        }
                    }

                    br.close();
                }
            }
        }

        @Override
        public void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("admissionId")) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length >= 5) {

                String deptCode = parts[2].trim();

                String deptName =
                        departments.get(deptCode);

                if (deptName != null) {

                    departmentName.set(deptName);

                    context.write(
                            departmentName,
                            one
                    );
                }
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result =
                new IntWritable();

        @Override
        public void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            result.set(total);

            context.write(key, result);
        }
    }

    public static void main(String[] args)
            throws Exception {

        Configuration conf =
                new Configuration();

        Job job = Job.getInstance(
                conf,
                "Admissions Per Department"
        );

        job.setJarByClass(
                AdmissionsPerDepartment.class
        );

        job.setMapperClass(
                JoinMapper.class
        );

        job.setReducerClass(
                SumReducer.class
        );

        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                IntWritable.class
        );

        // Map-side Join file
        job.addCacheFile(
                new URI(
                    "hdfs://namenode:9000/medtrack/input/departments.csv"
                )
        );

        FileInputFormat.addInputPath(
                job,
                new Path(
                    "hdfs://namenode:9000/medtrack/input/admissions.csv"
                )
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(
                    "hdfs://namenode:9000/medtrack/output/problem1"
                )
        );

        System.exit(
                job.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}
