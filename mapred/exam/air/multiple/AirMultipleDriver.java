package mapred.exam.air.multiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 사용자가 -D 옵션을 이용해서 입력한 옵션값을프로그램 안에서 사용하기 위해서
 * 즉, Mapper가 사용할 수 있도록 전달
 * 1. Configured와 Tool을 상속해야 한다.
 *    => Configured는 환경설정 정보를 활용
 *    => Tool은 사용자정의 옵션을 사용하기 위해
 * 2. run메소드를 오버라이딩
 *    => run메소드안에 Driver에서 구현했던 모든 코드를 구현
 * 3. run메소드를 main메소드에서 실행되도록 호출해야 한다.
 *    => run메소드를 직접 호출 할 수 없고 ToolRunner를 이용해서 헬퍼클래스를 이용해서 호출
 */
public class AirMultipleDriver extends Configured implements Tool{
	@Override
	public int run(String[] optionlist) throws Exception {
		//run메소드는 사용자가 입력한 모든 옵션에 대한 정보를 String[]로 전달받는다.
		//-D를 입력하고 설정한 옵션과 사용자가 입력한 명령행매개변수를 분리하여 관리해야 한다.
		//getRemainingArgs()를 이용하여 공통옵션(-D와 입력한 값 이외의 입력값)
		//과 사용자가 입력한 옵션을 따로 분리한다. ->
		GenericOptionsParser parser = new GenericOptionsParser(getConf(), optionlist);
		String[] otherArgs = parser.getRemainingArgs();
		/*Configuration conf = new Configuration();*/
		Job job = new Job(getConf(),"air_multi");
		
		job.setMapperClass(AirMultipleMapper.class);
		job.setReducerClass(AirMultipleReducer.class);
		job.setJarByClass(AirMultipleDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		MultipleOutputs.addNamedOutput(job,"departure",TextOutputFormat.class,
										TextOutputFormat.class,IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job,"arrival",TextOutputFormat.class,
				TextOutputFormat.class,IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job,"departureNA",TextOutputFormat.class,
				TextOutputFormat.class,IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job,"arrivalNA",TextOutputFormat.class,
				TextOutputFormat.class,IntWritable.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new AirMultipleDriver(), args);
	}

}
