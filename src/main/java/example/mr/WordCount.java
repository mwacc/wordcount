package example.mr;

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

/**
 * hadoop jar ... example.mr.WordCount -DINPUT=... -DOUTPUT=...
 */
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
    public static class WordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable ONE = new IntWritable(1);
        private Text outKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\\s");
            for(String word : words) {
                outKey.set(word);
                context.write(outKey, ONE);
            }
        }
    }

    // reducer
    public static class FrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int n = 0;
            Iterator<IntWritable> it = values.iterator();
            while( it.hasNext() ) {
                n += it.next().get();
            }
            count.set(n);

            context.write(key, count);

            context.getCounter("STATS", "WORDS_COUNT").increment(n);
        }
    }
}