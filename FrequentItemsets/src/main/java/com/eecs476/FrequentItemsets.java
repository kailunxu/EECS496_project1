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

    public static class Mapper2
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        // Output: id, timestamp
        public List<String> nextrecords = new ArrayList<String>();
        public ArrayList<String> allitems = new ArrayList<String>();

        private final static IntWritable one = new IntWritable(1);
        private String isDirectory;
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String record = conf.get("map.record.file");
            isDirectory = conf.get("map.record.isDirectory");
            if(!isDirectory.equals("true")){
                nextrecords = Assistance.getNextRecord(record);
            }
        }
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String parts[] = line.split(",");
            
            if(!isDirectory.equals("false")){
                for(int i = 1; i < parts.length; ++i) {
                    context.write(new Text(parts[i]), one);
                }
            } else {
                Set<String> dstr = new HashSet<String>();
            
                for(int i = 1; i < parts.length; ++i){
                    dstr.add(parts[i]);
                }

                for(int i = 0; i < nextrecords.size();i++){
                    for (int j = i + 1; j < nextrecords.size(); ++j) {
                        String word = "";
                        if(dstr.contains(nextrecords.get(i))&&dstr.contains(nextrecords.get(j))){
                            word = nextrecords.get(i) + "," + nextrecords.get(j);
                            context.write(new Text(word), one);
                        }
                    }
                }
            }
        }
    }

    
    public static class Reducer2
            extends Reducer<Text,IntWritable,Text,Text> {
        
        Integer s = 0;
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            s = Integer.parseInt(conf.get("s"));
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Integer currents = 0;
            for (IntWritable value: values) {
                currents += 1;
            }
            if (currents >= s) {
                context.write(key, new Text(currents.toString()));
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
        conf.set("map.record.file", ratingsFile);

        conf.set("map.record.isDirectory", "true");
        
        Job outputJob = Job.getInstance(conf, "outputJob");
        outputJob.setJarByClass(FrequentItemsets.class);

        outputJob.setMapperClass(Mapper2.class);
        outputJob.setReducerClass(Reducer2.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        outputJob.setMapOutputKeyClass(Text.class);
        outputJob.setMapOutputValueClass(IntWritable.class);

        // set reducer output key and value class
        outputJob.setOutputKeyClass(Text.class);
        outputJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(outputJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(outputJob, new Path(outputScheme + "1"));

        outputJob.waitForCompletion(true);

        // Assitance.SaveNextRecords(outputScheme + "2", "output", 0);
        Integer i = 1;
        while (i < k) {
            conf.set("map.record.isDirectory", "false");
            conf.set("map.record.file", outputScheme + i + "/part-r-00000");
            Job outputJob2 = Job.getInstance(conf, "outputJob");
            outputJob2.setJarByClass(FrequentItemsets.class);
            outputJob2.setNumReduceTasks(1);

            outputJob2.setMapperClass(Mapper2.class);
            outputJob2.setReducerClass(Reducer2.class);

            outputJob2.setMapOutputKeyClass(Text.class);
            outputJob2.setMapOutputValueClass(IntWritable.class);

            // set reducer output key and value class
            outputJob2.setOutputKeyClass(Text.class);
            outputJob2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(outputJob2, new Path(ratingsFile));
            FileOutputFormat.setOutputPath(outputJob2, new Path(outputScheme + (i + 1)));

            outputJob2.waitForCompletion(true);
            i += 1;
        }
    }
}
