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

public class FrequentItemsets {


    

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
            Collections.sort(list);
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
        Integer k = 0;
        
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("k"));
            
        }
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String parts[] = line.split(",");
            int size = parts.length;
            if (k == 2) {
                for (int i = 1; i < size; ++i) {
                    for (int j = i + 1; j < size; ++j) {
                        context.write(new Text(parts[i] + "_" + parts[j]), new Text(parts[0]));
                    }
                }
            }
            if (k == 3) {
                for (int i = 1; i < size; ++i) {
                    for (int j = i + 1; j < size; ++j) {
                        for (int l = j + 1; l < size; ++l) {
                            context.write(new Text(parts[i] + "_" + parts[j] + "_" + parts[l]), new Text(parts[0]));
                        }
                    }
                }
            }
        }
    }

    
    public static class Reducer2
            extends Reducer<Text,Text,Text,Text> {
        
        Integer s = 0;
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            s = Integer.parseInt(conf.get("s"));
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Integer currents = 0;
            for (Text value: values) {
                currents += 1;
            }
            String all[] = key.toString().split("_");
            String index = "";
            for (String k : all) {
                if (index.equals("") == false) {
                    index = index + "," + k;
                } else {
                    index = index + k;
                }
            }
            if (s <= currents) {
                context.write(new Text(index), new Text(currents.toString()));
            }
        }
    }

    public static String ratingsFile;
    public static String outputScheme;

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Integer k = 0;
        Integer s = 0;
    
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
        conf.set("k", k.toString());
        conf.set("s", s.toString());

        Job mergeJob = Job.getInstance(conf, "mergeJob");
        mergeJob.setJarByClass(FrequentItemsets.class);
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
        outputJob.setJarByClass(FrequentItemsets.class);
        outputJob.setNumReduceTasks(10);

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
    }
}
