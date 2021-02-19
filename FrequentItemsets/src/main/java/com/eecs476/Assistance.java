package com.eecs476;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import java.util.*;
public class Assistance {

    public static List<String> getNextRecord(String nextrecord) {
        
        
        List<String> result = new ArrayList<String>();

        try {
            Path path = new Path(nextrecord);

            Configuration conf = new Configuration();

            FileSystem fileSystem = path.getFileSystem(conf);

            FSDataInputStream fsis = fileSystem.open(path);
            LineReader lineReader = new LineReader(fsis, conf);

            Text line = new Text();
            while (lineReader.readLine(line) > 0) {
                // ArrayList<Double> tempList = textToArray(line);
                String[] fields = line.toString().split(",");
                
                result.add(fields[0]);
            }
            lineReader.close();
            // result = connectRecord(result);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return result;
    }



}