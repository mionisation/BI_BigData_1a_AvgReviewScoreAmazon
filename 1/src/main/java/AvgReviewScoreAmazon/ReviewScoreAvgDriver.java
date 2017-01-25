package AvgReviewScoreAmazon;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class ReviewScoreAvgDriver extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(ReviewScoreAvgDriver.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ReviewScoreAvgDriver(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  //TODO DELETE THIS
  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {   

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] prArr = line.split(",");
      String product = prArr[1].trim();
      String rating = prArr[2].trim();
      context.write(new Text(product), new DoubleWritable(Double.parseDouble(rating)));
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
        throws IOException, InterruptedException {
      double sum = 0;
      double amount = 0; 
      for (DoubleWritable count : counts) {
        sum += count.get();
        amount++;
      }
     sum /= amount;
      context.write(word, new DoubleWritable(sum));
    }
  }
}
