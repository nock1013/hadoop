package mapred.basic;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper -> 데이터를 분류하는 역활
//1. Mapper클래스를 상속한다.
//   => mapper에 전달될 input데이터의 key,value타입과
//		mapper의 실행결과로 출력되는 output데이터의 key,value타입을 정의

//2. map메소드를 오버라이딩해서 map작업을 수행하면서 처리할 내용을 구현
//   => 입력된 값을 분석하기 위한 메소드 : 입력된 데이터에 조건을 적용해서
//   						       원하는 데이터를 추출하기 위한 반복작업 수행
//   map메소드의 매개변수 - 입력데이터 키, 입력값, Context
//									  --------
//   Context - 맵리듀스 작업을 수행하며 맵메소드의 실행결과 
//			       즉, 출력데이터를 기록하고 shuffle하고 리듀서로 내보내는 작업을 수행하는 객체
//			       프레임워크 내부에서 기본작업을 처리하는 객체
//			       내부에서 머신들끼리 통신할때 필요한 여러가지 정보를 갖고 있다.
public class WordCountMapper 
 		extends Mapper<LongWritable, Text, Text, IntWritable> {
	//output데이터를 mapper의 실행결과로 내보낼 수 있도록 
	//키와 value를 저장하는 변수
	//output데이터의 value는 무조건 1이므로 상수로 정의
	static final IntWritable outputVal = new IntWritable(1);
	//output데이터의 key는 문자열이므로 Text타입으로 정의
	Text outputKey = new Text();
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//key는 linenumber ex) 1
		//value는 입력데이터의 한 라인에 해당하는 문장 ex)read a book
		StringTokenizer	st = new StringTokenizer(value.toString());
		while(st.hasMoreTokens()) {
			String token = st.nextToken();
			outputKey.set(token);//output데이터의 키를 셋팅
			//Context객체의 write메소드를 통해서 output으로
			//내보낼 데이터의 key와 value를 정의
			context.write(outputKey, outputVal);
		}
	}
	
}
