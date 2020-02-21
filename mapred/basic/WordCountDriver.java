package mapred.basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//맵리듀스를 실행하기 위한 일련의 작업을 처리하는 클래스
public class WordCountDriver {
	public static void main(String[] args) throws Exception{
		//1. 맵리듀스를 처리하기 위한 job을 생성
		Configuration conf = new Configuration();
		Job job = new Job(conf, "mywordcount");
		
		//2. 실제 job을 처리하기 위한 클래스가 어떤 클래스인지 등록
		//   실제 우리가 구현한 Mapper, Reducer, Driver를 등록
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setJarByClass(WordCountDriver.class);
		
		//3. HDFS에서 읽고 쓸 input데이터와 output데이터의 포맷을 정의
		//   => hdfs에 텍스트파일의 형태로 input/output을 처리
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//4. 리듀스의 출력데이터에 대한 키와 value의 타입을 정의
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//5. hdfs의 저장된 파일을 읽고 쓸 수 있는 Path객체를 정의
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//6. 1번~5번까지 설정한 내용을 기반으로 job이 실행되도록 명령
		job.waitForCompletion(true);
		
	}

}
