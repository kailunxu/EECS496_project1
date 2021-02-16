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

public class AssociationRules {
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
            System.out.println("enter." + line);
        }
    }

    public static class Reducer1
            extends Reducer<Text,Text,Text,Text> {
        

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            
            String index="";
            ArrayList<String> list = new ArrayList<String>();
            for (Text value : values) {
                list.add(value.toString());
            }
            // Collections.sort(list);
            for (String s: list) {
                if (index.equals("") == false) {
                    index = index + "," + s;
                } else {
                    index = index + s;
                }
            }
            context.write(key, new Text(index));
        }
    }

    public static class Mapper2
            extends Mapper<LongWritable, Text, Text, Text>{

        // Output: id, timestamp

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String parts[] = line.split(",");
            System.out.println("enter." + line);
            int size = parts.length;
            if (k == 2) {
                for (int i = 1; i < size; ++i) {
                    for (int j = i + 1; j < size; ++j) {
                        context.write(new Text(parts[i]), new Text(parts[j]));
                        
                        context.write(new Text(parts[j]), new Text(parts[i]));
                    }
                }
            }
        }
    }

    
    public static class Reducer2
            extends Reducer<Text,Text,Text,Text> {
        

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            System.out.println("enter." + key.toString());
            Integer currents = 0;
            ArrayList<String> list = new ArrayList<String>();
            for (Text value: values) {
                currents += 1;
                list.add(value.toString());
            }
            
            for (String s: list) {
                context.write(key, new Text(s + "," + currents.toString()));
                // context.write(key, new Text("good"));
            }
            // if (s <= currents) {
            //     context.write(new Text(index), new Text(currents.toString()));
            // }
        }
    }


    public static class Mapper3
            extends Mapper<LongWritable, Text, Text, Text>{

        // Output: id, timestamp

        Text keyEmit = new Text();
        Text valEmit = new Text();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String parts[] = line.split(",");
            context.write(new Text(parts[0] + "_" + parts[1]), new Text(parts[2]));
        }
    }

    public static class Reducer3
            extends Reducer<Text,Text,Text,Text> {
        

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Integer currents = 0;
            Double alls = 0.0;
            for (Text value: values) {
                currents += 1;
                alls = Double.parseDouble(value.toString());
            }
            String all[] = key.toString().split("_");
            
            if (s <= currents) {
                context.write(new Text(all[0] + "," + all[1]), new Text(Double.toString(currents/alls * 2.0)));
            }
        }
    }

    private static Integer k;
    private static Integer s;
    
    private static String ratingsFile;
    private static String outputScheme;
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("--ratingsFile")) {
                ratingsFile = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else if (args[i].equals("-k")) {
                k = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-s")) {
                s = Integer.parseInt(args[++i]);
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
        mergeJob.setJarByClass(AssociationRules.class);
        mergeJob.setNumReduceTasks(1);

        mergeJob.setMapperClass(Mapper1_rating.class);
        mergeJob.setReducerClass(Reducer1.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        mergeJob.setMapOutputKeyClass(Text.class);
        mergeJob.setMapOutputValueClass(Text.class);

        // set reducer output key and value class
        mergeJob.setOutputKeyClass(Text.class);
        mergeJob.setOutputValueClass(Text.class);

        // MultipleInputs.addInputPath(mergeJob, new Path(ratingsFile), TextInputFormat.class,
        // Mapper1_rating.class);
        // MultipleInputs.addInputPath(mergeJob, new Path(genresFile), TextInputFormat.class,
        // Mapper1_genre.class);
        // MultipleInputs.addInputPath(mergeJob, new Path(movieNameFile), TextInputFormat.class,
        // Mapper1_name.class);
        FileInputFormat.addInputPath(mergeJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(mergeJob, new Path(outputScheme + "1"));

        mergeJob.waitForCompletion(true);
        
        Job outputJob = Job.getInstance(conf, "outputJob");
        outputJob.setJarByClass(AssociationRules.class);
        outputJob.setNumReduceTasks(1);

        outputJob.setMapperClass(Mapper2.class);
        outputJob.setReducerClass(Reducer2.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        outputJob.setMapOutputKeyClass(Text.class);
        outputJob.setMapOutputValueClass(Text.class);

        // set reducer output key and value class
        outputJob.setOutputKeyClass(Text.class);
        outputJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(outputJob, new Path(outputScheme + "1"));
        FileOutputFormat.setOutputPath(outputJob, new Path(outputScheme + "2"));

        outputJob.waitForCompletion(true);


        Job finalJob = Job.getInstance(conf, "outputJob");
        finalJob.setJarByClass(AssociationRules.class);
        finalJob.setNumReduceTasks(1);

        finalJob.setMapperClass(Mapper3.class);
        finalJob.setReducerClass(Reducer3.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        finalJob.setMapOutputKeyClass(Text.class);
        finalJob.setMapOutputValueClass(Text.class);

        // set reducer output key and value class
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(finalJob, new Path(outputScheme + "2"));
        FileOutputFormat.setOutputPath(finalJob, new Path(outputScheme + "3"));

        finalJob.waitForCompletion(true);
    }
}
https://github.com/kailunxu/EECS496_project1.git