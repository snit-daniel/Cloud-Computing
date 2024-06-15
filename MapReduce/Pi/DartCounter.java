import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DartCounter {

    public static class DartMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private int radius;
    
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Check if the line contains the radius
            if (line.startsWith("Radius:")) {
                // Parse the radius value
                radius = Integer.parseInt(line.split(":")[1].trim());
            } else {
                // Split the line into fields
                String[] fields = line.split("\\s+");
                
                // Process the fields (assuming they represent points)
                if (fields.length == 2) {
                    // Calculate distance from origin and emit
                    int x = Integer.parseInt(fields[0]);
                    int y = Integer.parseInt(fields[1]);
                    double distance = Math.sqrt(x * x + y * y);
                    
                    // Emit points within the radius
                    if (distance <= radius) {
                        word.set("Inside");
                        context.write(word, one);
                    } else {
                        word.set("Outside");
                        context.write(word, one);
                    }
                }
            }
        }
    }
    
    public static class DartReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Dart Counter");
        job.setJarByClass(DartCounter.class);
        job.setMapperClass(DartMapper.class);
        job.setCombinerClass(DartReducer.class);
        job.setReducerClass(DartReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
