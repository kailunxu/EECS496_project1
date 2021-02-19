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

    public static class Mapper2
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        // Output: id, timestamp

        private final static IntWritable one = new IntWritable(1);
        

        protected void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
        }
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String parts[] = line.split(",");
            
            
            for(int i = 1; i < parts.length; ++i) {
                context.write(new Text(parts[i]), one);
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

    public static class Mapper3
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        // Output: id, timestamp
        public List<List<String>> nextrecords = new ArrayList<List<String>>();
        private final static IntWritable one = new IntWritable(1);
        

        protected void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String record = conf.get("map.record.file");
            String k = conf.get("k");
            nextrecords = Assistance.getNextRecord(record);
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String parts[] = line.split(",");
            System.out.println("enter." + line);

            List<String> dstr = new ArrayList<String>();
        
            for(int i = 1; i < parts.length; ++i){
                dstr.add(parts[i]);
            }

            for(int i = 0; i < nextrecords.size();i++){
                String word = "";
                System.out.println("enter." + nextrecords.get(i));
                if(dstr.containsAll(nextrecords.get(i))){
                    word = nextrecords.get(i).get(0) + "_" + nextrecords.get(i).get(1);
                    context.write(new Text(word), one);
                }
            }
        }
    }

    
    public static class Reducer3
            extends Reducer<Text,IntWritable,Text,Text> {
        
        Integer s = 0;
        public Map<String, Integer> numberrecords = new HashMap<String, Integer>();
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            s = Integer.parseInt(conf.get("s"));
            String record = conf.get("map.record.file");
            numberrecords = Assistance.getMap(record);
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Integer currents = 0;
            for (IntWritable value: values) {
                currents += 1;
            }
            String all[] = key.toString().split("_");
            Double num1 = Double.valueOf(numberrecords.get(all[0]));
            Double num2 = Double.valueOf(numberrecords.get(all[1]));

            if (currents >= s) {
                context.write(new Text(all[0] + "->" + all[1]), new Text(Double.toString(currents/num1)));
                context.write(new Text(all[1] + "->" + all[0]), new Text(Double.toString(currents/num2)));
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


        Job outputJob = Job.getInstance(conf, "outputJob");
        outputJob.setJarByClass(AssociationRules.class);
        outputJob.setNumReduceTasks(1);

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


        conf.set("map.record.file", outputScheme + "1/part-r-00000");

        Job outputJob2 = Job.getInstance(conf, "outputJob");
        outputJob2.setJarByClass(AssociationRules.class);
        outputJob2.setNumReduceTasks(1);

        outputJob2.setMapperClass(Mapper3.class);
        outputJob2.setReducerClass(Reducer3.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        outputJob2.setMapOutputKeyClass(Text.class);
        outputJob2.setMapOutputValueClass(IntWritable.class);

        // set reducer output key and value class
        outputJob2.setOutputKeyClass(Text.class);
        outputJob2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(outputJob2, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(outputJob2, new Path(outputScheme + "2"));

        outputJob2.waitForCompletion(true);

    }
}
