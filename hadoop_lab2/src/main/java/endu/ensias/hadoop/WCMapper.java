package edu.ensias.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
   //public static final IntWritable one = new IntWritable(1);
   //public static final Text word = new Text();
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
       for (String w : words) {
              //word.set(w);
              context.write(new Text(w), new IntWritable(1));

       }
   }
    
}