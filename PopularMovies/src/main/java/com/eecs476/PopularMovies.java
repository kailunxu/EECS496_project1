package com.eecs476;

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
/*
IntWritable is the Hadoop variant of Integer which has been optimized for serialization in the Hadoop environment.
An integer would use the default Java Serialization which is very costly in Hadoop environment.
see: https://stackoverflow.com/questions/52361265/integer-and-intwritable-types-existence
 */

public class PopularMovies {

    public static class Mapper1_rating
            extends Mapper<LongWritable, Text, Text, Text>{

        // Output: id, timestamp

        Text keyEmit = new Text();
        Text valEmit = new Text();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println("enter." + line);
            String parts[] = line.split(",");
            keyEmit.set(parts[1]);
            valEmit.set(parts[2] + "_" + parts[3]);
            context.write(keyEmit, valEmit);
        }
    }

    public static class Mapper1_genre
            extends Mapper<LongWritable, Text, Text, Text>{

        // Output: id, genre

        Text keyEmit = new Text();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            
            String line = value.toString();
            System.out.println("enter." + line);
            String parts[] = line.split(",");
            int count = 0;
            for (String part: parts) {
                if (count == 0) {
                    keyEmit.set(part);
                } else {
                    context.write(keyEmit, new Text(part));
                }
                count++;
            }
        }
    }
    
    public static class Mapper1_name
            extends Mapper<LongWritable, Text, Text, Text>{


        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            
            String line = value.toString();
            System.out.println("enter." + line);
            String parts[] = line.split(",");
            String name = line.substring(parts[0].length() + 1);
            int count = 0;
            context.write(new Text(parts[0]), new Text("_" + name));
            
        }
    }

    public static class Reducer1
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map<Integer, Double> sumlist = new HashMap<Integer, Double>();
            Map<Integer, Integer> numlist = new HashMap<Integer, Integer>();
            List<String> genrelist = new ArrayList<>();
            String name = "";
            Text keyEmit = new Text();
            Text valEmit = new Text();
            for (Text value: values) {
                String val = value.toString();
                
                char myChar = val.charAt(0);
                
                if (Character.isDigit(myChar)) {
                    String timestamp = val.split("_")[1];
                    String ratings = val.split("_")[0];
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(Long.parseLong(timestamp) * 1000);
                    int year = cal.get(Calendar.YEAR);
                    year = (year / 3) * 3;
                    if (numlist.containsKey(year)) {
                        sumlist.put(year, sumlist.get(year) + Double.parseDouble(ratings));
                        numlist.put(year, numlist.get(year) + 1);
                    } else {
                        sumlist.put(year, Double.parseDouble(ratings));
                        numlist.put(year, 1);
                    }
                } else if (myChar == '_') {
                    name = val.substring(1);
                } else {
                    genrelist.add(val);
                }
            }
            for (Integer i: numlist.keySet()) {
                Integer num = numlist.get(i);
                Double sum = sumlist.get(i);
                if (num != 0 && name.equals("") == false) {
                    for (String genre: genrelist) {
                        context.write(new Text(genre + "_" + i), new Text(sum.toString() + "_" + num.toString() + "_" + key.toString() + "_" + name));
                    }
                }
            }
        }
    }
    public static class Mapper2
            extends Mapper<LongWritable, Text, Text, Text>{
        
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String all[] = value.toString().split(",");
            String remain = value.toString().substring(all[0].length() + 1);
            context.write(new Text(all[0]), new Text(remain));
        }
    }

    public static class Reducer2
            extends Reducer<Text,Text,Text,Text> {
        

        // Input: genre_index,sum_num_id
        // List<Integer> convertlist;
        // protected void setup(Context context
        // ) throws IOException, InterruptedException {
        //     convertlist = new ArrayList<>();
        //     for (int i = 1995; i <= 2016; i = i + 3) {
        //         convertlist.add(i);
        //     }
        // }
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map<Integer, Integer> nummap = new HashMap<Integer, Integer>();
            Map<Integer, Double> summap = new HashMap<Integer, Double>();
            Map<Integer, String> namemap = new HashMap<Integer, String>();
            for (Text val : values) {
                String all[] = val.toString().split("_");
                String name = all[3];
                Integer id = Integer.parseInt(all[2]);
                Integer num = Integer.parseInt(all[1]);
                Double sum = Double.parseDouble(all[0]);
                if (nummap.containsKey(id)) {
                    nummap.put(id, nummap.get(id) + num);
                } else {
                    nummap.put(id, num);
                }
                if (summap.containsKey(id)) {
                    summap.put(id, summap.get(id) + sum);
                } else {
                    summap.put(id, sum);
                }
                namemap.put(id, name);
            }
            Integer maxid = 0;
            String maxname = "";
            Double maxnum = 0.0;
            for (Integer id: nummap.keySet()) {
                Integer num = nummap.get(id);
                Double sum = summap.get(id);
                String name = namemap.get(id);
                if (maxnum < sum/num || (maxnum == sum/num && (maxname == "" || maxname.compareTo(name) > 0))) {
                    maxid = id;
                    maxname = name;
                    maxnum = sum/num;
                }
            }
            String all[] = key.toString().split("_");
            Integer year = Integer.parseInt(all[1]);
            assert (maxid != 0 && maxname != "");
            context.write(new Text(year.toString() + "," + all[0]), new Text(maxnum.toString() + "," + maxid.toString() + "," + maxname));
        }
    }

    // public static class Mapper3
    //         extends Mapper<LongWritable, Text, Text, Text>{
        
    //     public void map(LongWritable key, Text value, Context context
    //     ) throws IOException, InterruptedException {
    //         String all[] = value.toString().split(",");
    //         context.write(new Text(all[3]), new Text(all[0] + "," + all[1] + "," + all[2]));
    //     }
    // }


    // public static class Reducer3
    //         extends Reducer<Text,Text,Text,Text> {
    //     public void reduce(Text key, Iterable<Text> values,
    //                        Context context
    //     ) throws IOException, InterruptedException {
    //         String name;
    //         for (Text val : values) {
    //             String all[] = val.toString().split(",");
    //             if (len(all) == 1) {
    //                 name = val.toString();
    //             }
    //         }
    //         for (Text val : values) {
    //             String all[] = val.toString().split(",");
    //             if (len(all) == 3) {
    //                 context.write(all[0] + "," + all[1] + "," + all[2] + "," + key.toString() + "," + name);
    //             }
    //         }
    //     }
    // }

    private static String ratingsFile;
    private static String genresFile;
    private static String outputScheme;
    private static String movieNameFile;
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("--ratingsFile")) {
                ratingsFile = args[++i];
            } else if (args[i].equals("--genresFile")) {
                genresFile = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else if (args[i].equals("--movieNameFile")) {
                movieNameFile = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        if (ratingsFile == null || genresFile == null || outputScheme == null || movieNameFile == null) {
            throw new RuntimeException("Either outputpath or input path are not defined");
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476w21");         // required for this to work on GreatLakes


        Job mergeJob = Job.getInstance(conf, "mergeJob");
        mergeJob.setJarByClass(PopularMovies.class);
        mergeJob.setNumReduceTasks(1);

        // mergeJob.setMapperClass(Mapper1_rating.class);
        mergeJob.setReducerClass(Reducer1.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        mergeJob.setMapOutputKeyClass(Text.class);
        mergeJob.setMapOutputValueClass(Text.class);

        // set reducer output key and value class
        mergeJob.setOutputKeyClass(Text.class);
        mergeJob.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(mergeJob, new Path(ratingsFile), TextInputFormat.class,
        Mapper1_rating.class);
        MultipleInputs.addInputPath(mergeJob, new Path(genresFile), TextInputFormat.class,
        Mapper1_genre.class);
        MultipleInputs.addInputPath(mergeJob, new Path(movieNameFile), TextInputFormat.class,
        Mapper1_name.class);
        // FileInputFormat.addInputPath(mergeJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(mergeJob, new Path(outputScheme + "1"));

        mergeJob.waitForCompletion(true);
        
        Job outputJob = Job.getInstance(conf, "outputJob");
        outputJob.setJarByClass(PopularMovies.class);
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
    }

}
