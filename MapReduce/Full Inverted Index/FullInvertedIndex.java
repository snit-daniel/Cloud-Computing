package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FullInvertedIndex {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text fileAndPosition = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            int fileNumber = Integer.parseInt(fileName.replaceAll("[^0-9]", ""));
            String line = value.toString();
            String[] words = line.split("\\s+");

            for (int i = 0; i < words.length; i++) {
                word.set(words[i]);
                fileAndPosition.set(fileNumber + "," + i);
                output.collect(word, fileAndPosition);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            StringBuilder result = new StringBuilder();
            while (values.hasNext()) {
                if (result.length() > 0) {
                    result.append(", ");
                }
                result.append("(").append(values.next().toString()).append(")");
            }
            output.collect(key, new Text("{" + result.toString() + "}"));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(FullInvertedIndex.class);
        conf.setJobName("fullinvertedindex");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
