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

    public static List<List<String>> getNextRecord(String nextrecord) {
        
        
        List<List<String>> result = new ArrayList<List<String>>();

        try {
            Path path = new Path(nextrecord);

            Configuration conf = new Configuration();

            FileSystem fileSystem = path.getFileSystem(conf);

            FSDataInputStream fsis = fileSystem.open(path);
            LineReader lineReader = new LineReader(fsis, conf);

            Text line = new Text();
            while (lineReader.readLine(line) > 0) {
                List<String> tempList = new ArrayList<String>();
                // ArrayList<Double> tempList = textToArray(line);
                String[] fields = line.toString().split(",");
                for (int i = 0; i < fields.length - 1; i++) {
                    tempList.add(fields[i]);
                }
                Collections.sort(tempList);
                result.add(tempList);
            }
            lineReader.close();
            result = connectRecord(result);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return result;
    }

    public static Map<String, Integer> getMap(String nextrecord) {
        
        Map<String, Integer> result = new HashMap<String, Integer>();

        try {
            Path path = new Path(nextrecord);

            Configuration conf = new Configuration();

            FileSystem fileSystem = path.getFileSystem(conf);

            FSDataInputStream fsis = fileSystem.open(path);
            LineReader lineReader = new LineReader(fsis, conf);

            Text line = new Text();
            while (lineReader.readLine(line) > 0) {
                List<String> tempList = new ArrayList<String>();
                String[] fields = line.toString().split(",");
                
                result.put(fields[0], Integer.parseInt(fields[1]));
            }
            lineReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static List<List<String>> connectRecord(List<List<String>> result) {

        List<List<String>> nextCandidateItemset = new ArrayList<List<String>>();
        for (int i = 0; i < result.size(); i++) {

            HashSet<String> hsSet = new HashSet<String>();
            HashSet<String> hsSettemp = new HashSet<String>();
            for (int k = 0; k < result.get(i).size(); k++) {
                hsSet.add(result.get(i).get(k));
            }
            int hsLength_before = hsSet.size();
            hsSettemp = (HashSet<String>) hsSet.clone();
            for (int h = i + 1; h < result.size(); h++) {
                hsSet = (HashSet<String>) hsSettemp.clone();
                for (int j = 0; j < result.get(h).size(); j++)
                    hsSet.add(result.get(h).get(j));
                int hsLength_after = hsSet.size();
                if (hsLength_before + 1 == hsLength_after
                        && isnotHave(hsSet, nextCandidateItemset)
                        && isSubSet(hsSet, result)) {
                    
                    List<String> tempList = new ArrayList<String>();
                    for (String Item: hsSet) {
                        tempList.add(Item);
                    }
                    Collections.sort(tempList);
                    nextCandidateItemset.add(tempList);
                }
            }
        }
        return nextCandidateItemset;
    }

    private static boolean isSubSet(HashSet<String> hsSet,
            List<List<String>> result) {

        List<String> tempList = new ArrayList<String>();

        for (String Item: hsSet) {
            tempList.add(Item);
        }
        Collections.sort(tempList); 
        List<List<String>> sublist = new ArrayList<List<String>>();

        for(int i = 0; i < tempList.size(); i++){
            List<String> temp = new ArrayList<String>();
            for(int j = 0; j < tempList.size(); j++){
                temp.add(tempList.get(j));
            }
            temp.remove(temp.get(i));
            sublist.add(temp);

        }
        if(result.containsAll(sublist)){
            return true;
        }

        return true;
    }

    private static boolean isnotHave(HashSet<String> hsSet,
            List<List<String>> nextCandidateItemset) {
        List<String> tempList = new ArrayList<String>();
        for (String Item: hsSet) {
            tempList.add(Item);
        }
        Collections.sort(tempList);
        if(nextCandidateItemset.contains(tempList)){
            return false;
        }
        return true;
    }

}