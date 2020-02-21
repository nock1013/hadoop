package mapred.exam.stock.option;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockOptionMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	String jobType;
	static final IntWritable one = new IntWritable(1);//output Value
	Text outputKey =new Text();//output key
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		jobType = context.getConfiguration().get("jobType");
	}

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if(key.get()>0) {
		String[] line = value.toString().split(",");
			if(line!=null & line.length>0) {
				//년도, 상승마감
				outputKey.set(line[2].substring(0,4));
				double result = Double.parseDouble(line[6]) - Double.parseDouble(line[3]);
				if(result>0 & jobType.equals("up")) {//상승마감
					context.write(outputKey, one);
				}else if(result<0 & jobType.equals("down")) {//하락
					context.write(outputKey, one);
				}else if(result==0 & jobType.equals("same")) {//동일
					context.write(outputKey, one);
				}
			}
		}
	}
}
