import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem4Driver {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                "Usage: Problem4Driver <admissions> <departments> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf, "MedTrack Problem 4 - HOD Most Admissions");

        job.setJarByClass(Problem4Driver.class);

        job.setMapperClass(Problem4Mapper.class);
        job.setReducerClass(Problem4Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // departments.csv is the small lookup file
        // Add it to Hadoop Distributed Cache
        job.addCacheFile(
            new URI(args[1] + "#departments.csv"));

        FileInputFormat.addInputPath(
            job, new Path(args[0]));

        FileOutputFormat.setOutputPath(
            job, new Path(args[2]));

        // One reducer is required to find the global maximum
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
