package mapred.basic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer - 데이터를 집계하는 역활
/*
 * 1. Reducer클래스를 상속
 * 2. reduce메소드를 오버라이딩 
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	//reduce작업의 결과를 저장할 변수
	IntWritable resultVal = new IntWritable();
	/*
	 * reduce메소드의 매개변수
	 * key => 입력데이터의 키타입
	 * values => 입력데이터의 값에 타입
	 * 			 Iterable 즉, 입력값들이 Iterable형태로 전달
	 * 			 {1,1,1,1,1,.....} 값은 IntWritable이지만
	 * 			  여러 개가 전달되므로 반복작업을 수행해야 한다.
	 * context => Mapper와 동일
	 * 			    맵리듀스 프레임워크 안에서 기본작업을 할 수 있도록 도와주는 역활
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable value:values) {//reduce에 전달된 입력데이터의 값을 꺼내서 모두 더하기
			sum = sum+value.get();
		}
		resultVal.set(sum);//계산된 결과를 IntWritable에 셋팅
		//reduce의 실행결과를 context에 write
		context.write(key, resultVal);
		
	}
	

}
