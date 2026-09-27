import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthlyTurnoverDriver {

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {
            System.exit(-1);
        }

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(
                        conf,
                        "Monthly Patient Turnover"
                );

        job.setJarByClass(
                MonthlyTurnoverDriver.class
        );

        // Map-side Join
        job.addCacheFile(
                new URI(
                        "/medtrack/input/departments.csv#departments.csv"
                )
        );

        job.setMapperClass(
                MonthlyTurnoverMapper.class
        );

        job.setReducerClass(
                MonthlyTurnoverReducer.class
        );

        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                Text.class
        );

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1
        );
    }
}
