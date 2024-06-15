package org.myorg;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class PartialInvertedIndex {
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
private Text word = new Text();
private IntWritable fileIndex = new IntWritable();
public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
String fileName = fileSplit.getPath().getName();
int fileNumber = Integer.parseInt(fileName.replaceAll("[^0-9]", ""));
fileIndex.set(fileNumber);
String line = value.toString();
StringTokenizer tokenizer = new StringTokenizer(line);
while (tokenizer.hasMoreTokens()) {
word.set(tokenizer.nextToken());
output.collect(word, fileIndex);
}
}
}
public static void main(String[] args) throws Exception {
JobConf conf = new JobConf(PartialInvertedIndex.class);
conf.setJobName("partialinvertedindex");
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(IntWritable.class);
conf.setMapperClass(Map.class);
conf.setReducerClass(Reduce.class);
conf.setInputFormat(TextInputFormat.class);
conf.setOutputFormat(TextOutputFormat.class);
FileInputFormat.setInputPaths(conf, new Path(args[0]));
FileOutputFormat.setOutputPath(conf, new Path(args[1]));
JobClient.runJob(conf);
}
}
