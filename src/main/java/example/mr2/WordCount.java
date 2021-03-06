package example.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount(), args));
    }


    @Override
    public int run(String[] args) throws Exception {
        return createJob(getConf()).waitForCompletion(true) ? 0 : 1;
    }

    public Job createJob(Configuration configuration) throws IOException {
        Job job = Job.getInstance(configuration, "Words frequency matrix");
        job.setJarByClass(this.getClass());

        Path input = new Path(configuration.get("INPUT"));
        Path output = new Path(configuration.get("OUTPUT"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // add Mapper and Reducer
        job.setMapperClass(WordsMapper.class);
        job.setReducerClass(FrequencyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    // mapper
    // "red red"
    public static class WordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        }

    }

    // reducer
    public static class FrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // red - > (1, 1)
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {

        }

    }

}