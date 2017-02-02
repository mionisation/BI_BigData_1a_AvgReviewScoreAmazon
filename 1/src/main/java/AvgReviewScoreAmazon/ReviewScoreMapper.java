package AvgReviewScoreAmazon;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewScoreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {   

	/**
	 * Extract the product number and its rating from the line of text
	 * provided. 
	 * Writes key-value pair (product number, rating) to the context for
	 * subsequent operations.
	 */
	@Override
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] prArr = line.split(",");
      String product = prArr[1].trim();
      String rating = prArr[2].trim();
      context.write(new Text(product), new DoubleWritable(Double.parseDouble(rating)));
    }
  }