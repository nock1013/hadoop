package mapred.exam.stock.option;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockOptionDriver extends Configured implements Tool {
	@Override
	public int run(String[] optionlist) throws Exception {
		GenericOptionsParser paser = new GenericOptionsParser(getConf(), optionlist);
		String[] otherAgrs = paser.getRemainingArgs();
		/*Configuration conf = new Configuration();*/
		Job job = new Job(getConf(), "stockoption");
		
		job.setMapperClass(StockOptionMapper.class);
		job.setReducerClass(StockOptionReducer.class);
		job.setJarByClass(StockOptionDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherAgrs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherAgrs[1]));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(),new StockOptionDriver(), args);
	}

}
