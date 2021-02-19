package com.eecs476;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files
import java.io.FileWriter;

public class TransactionFormat {
    public static class Mapper1_rating
            extends Mapper<LongWritable, Text, Text, Text>{

        // Output: id, timestamp

        Text keyEmit = new Text();
        Text valEmit = new Text();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String parts[] = line.split(",");
            keyEmit.set(parts[0]);
            valEmit.set(parts[1]);
            context.write(keyEmit, valEmit);
        }
    }

    public static class Reducer1
            extends Reducer<Text,Text,Text,Text> {
        

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<String> set = new HashSet<String>();
            String index="";
            for (Text value : values) {
                set.add(value.toString());
            }
            for (String s: set) {
                if (index == "") {
                    index = s;
                } else {
                    index = index + "," + s;
                }
            }
            context.write(key, new Text(index));
        }
    }

    private static String ratingsFile;
    private static String outputScheme;
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("--ratingsFile")) {
                ratingsFile = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        if (ratingsFile == null || outputScheme == null) {
            throw new RuntimeException("Either outputpath or input path are not defined");
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476w21");         // required for this to work on GreatLakes


        Job mergeJob = Job.getInstance(conf, "mergeJob");
        mergeJob.setJarByClass(TransactionFormat.class);

        mergeJob.setMapperClass(Mapper1_rating.class);
        mergeJob.setReducerClass(Reducer1.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        mergeJob.setMapOutputKeyClass(Text.class);
        mergeJob.setMapOutputValueClass(Text.class);

        // set reducer output key and value class
        mergeJob.setOutputKeyClass(Text.class);
        mergeJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(mergeJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(mergeJob, new Path(outputScheme));

        mergeJob.waitForCompletion(true);
    }
}
