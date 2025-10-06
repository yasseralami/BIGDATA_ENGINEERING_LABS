package edu.ensias.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobWC {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        // specifier la classe du job
        job.setJarByClass(JobWC.class);
        //specifier les classes mapper et reducer
        job.setMapperClass(edu.ensias.hadoop.WCMapper.class);
        job.setReducerClass(edu.ensias.hadoop.WCReducer.class);
        //specifie les types des cles et valeurs du reducer
        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);

        FileInputFormat.addInputPath( job, new Path(args[0]));
        FileOutputFormat.setOutputPath( job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        



    }
    
}
