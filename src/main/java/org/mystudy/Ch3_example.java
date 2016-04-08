package org.mystudy;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class Ch3_example implements Serializable{	

	private static final long serialVersionUID = 4344341L;
	JavaSparkContext sc = null;
	
	public Ch3_example(){
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
		sc = new JavaSparkContext("local[2]", "First Spark App");
	}
	
	public static void main(String... strings) {
		Ch3_example ex = new Ch3_example();
//		ex.proc1();
//		ex.proc2();
		ex.proc3();	//aggregate
	}

	public void proc1() {
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-1 textFile()로 문자열 RDD 만들기");
		// JavaSparkContext 생성할 때 local[1] 로 하면, 순서대로 나오는데, local[2] 이상으로 하면, 순서가 섞인다.
		// 2개이상의 코어로 돌릴고 나서 합치면서,,, 순서를 고려하지 않는가 보다.
		JavaRDD<String> lines = sc.textFile("src/main/java/org/mystudy/Ch3_example.java");
//		JavaRDD<String> lines = sc.textFile("src\\main\\java\\org\\mystudy\\Ch3_example.java");		// 위와 같음
		lines.foreach(str -> System.out.println(str));	// local[2] 일 때, 순서가 섞여있다.
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-2 filter() 트랜스포메이션 호출");
		JavaRDD<String> javaLines = lines.filter(line -> line.contains("public"));
		javaLines.foreach(str -> System.out.println(str));
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-3 first() 액션 호출");
		System.out.println(javaLines.first());
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-4 메모리에  RDD 보존하기");
		javaLines.cache();			//	같음 : javaLines.persist(StorageLevel.MEMORY_ONLY());
		System.out.println(javaLines.count());
		javaLines.unpersist();		//안해주면, 메모리에 계속 남는다고 한다.-_-;
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-7 parallelize() 메소드");
		JavaRDD<String> lines2 = sc.parallelize(Arrays.asList("pandas", "I like pandas"));
		lines2.foreach(str -> System.out.println(str));
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-10 textFile() 메소드");	
		JavaRDD<String> lines3 = sc.textFile("src\\main\\java\\org\\mystudy\\Ch3_example.java");
		lines3.foreach(str -> System.out.println(str));
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-13,14 filter(), union() 트랜스포메이션");			
		JavaRDD<String> inputRDD = sc.textFile("src\\main\\java\\org\\mystudy\\Ch3_example.java");
		JavaRDD<String> publicRDD = inputRDD.filter(str -> str.contains("public"));
		JavaRDD<String> voidRDD =  inputRDD.filter(str -> str.contains("void"));
		JavaRDD<String> unionRDD = publicRDD.union(voidRDD);	//합집합
		unionRDD.foreach(str -> System.out.println(str));
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-17 액션을 사용한 자바에서의 에러 세기");	
		System.out.println("input had " + publicRDD.count() + " lines");
		System.out.println("Here are 8 examples");
		for(String line : publicRDD.take(8)){
			System.out.println(line);
		}

		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-22 자바에서 익명 내부 클래스로 함수 전달하기");
		// 현재 클래스에 public class Ch3_example implements Serializable 안하면, 
		// [Exception msg1]
		// Exception in thread "main" org.apache.spark.SparkException: Task not serializable
		JavaRDD<String> lines22 = inputRDD.filter(new Function<String, Boolean>(){
			public Boolean call(String x){
				return x.contains("public");
			}
		});

		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-23 이름 있는 클래스로 함수 전달하기");
		class ContainPublic implements Function<String, Boolean> {
			public Boolean call(String x){
				return x.contains("public");
			}
		}
		JavaRDD<String> lines23 = inputRDD.filter(new ContainPublic());
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-24 인자를 가지는 자바 함수 클래스");
		class Contains implements Function<String, Boolean> {
			private String query;
			public Contains(String query){
				this.query = query;
			}
			public Boolean call(String x){
				return x.contains(query);
			}
		}		
		JavaRDD<String> lines24 = inputRDD.filter(new Contains("public"));
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-25 자바8의 람다 표현식을 사용한 함수 전달");
		JavaRDD<String> lines25 = inputRDD.filter(s -> s.contains("public"));
		
	}
	public void proc2() {
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-28 자바에서 RDD의 값들을 제곱하기");
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4));
		JavaRDD<Integer> result = rdd.map(a -> a*a);
		System.out.println(org.apache.commons.lang3.StringUtils.join(result.collect(), ","));
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-31 여러 라인을 단어로 분해하는 자바의 flatMap()");
		JavaRDD<String> lines31 = sc.parallelize(Arrays.asList("hello world", "hi"));
		JavaRDD<String> words = lines31.flatMap(line -> Arrays.asList(line.split(" ")));
		System.out.println(words.first());
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("표3-2 map, flatMap, filter, distinct, sample");
		JavaRDD<Integer> table3_2 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,8,8));
		System.out.println(StringUtils.join(table3_2.map(a -> a+1).collect(), ","));
		System.out.println(StringUtils.join(table3_2.flatMap(a -> Arrays.asList(1*a, 2*a)).collect(), ","));	//1,2,2,4,3,6,4,8,5,10,6,12,7,14,8,16,8,16,8,16
		System.out.println(StringUtils.join(table3_2.filter(a -> a>4).collect(), ","));		//5,6,7,8,8,8
		System.out.println(StringUtils.join(table3_2.distinct().collect(), ","));			//4,6,8,2,1,3,7,5	//순서없음
		System.out.println(StringUtils.join(table3_2.sample(false, 0.5).collect(), ","));	//2,4,7,8,8,8
		System.out.println(StringUtils.join(table3_2.sample(false, 0.5).collect(), ","));	//4,5,6,7,8,8
		System.out.println(StringUtils.join(table3_2.sample(false, 0.5).collect(), ","));	//1,4,8,8
		System.out.println(StringUtils.join(table3_2.sample(false, 0.3).collect(), ","));	//2,4
		System.out.println(StringUtils.join(table3_2.sample(false, 0.5).collect(), ","));	//6,7,8,8
		System.out.println(StringUtils.join(table3_2.sample(false, 0.7).collect(), ","));	//1,3,4,5,7,8,8,8
		System.out.println(StringUtils.join(table3_2.sample(true, 0.5).collect(), ","));	//1,1,2,5,7,7,8
		System.out.println(StringUtils.join(table3_2.sample(true, 0.5).collect(), ","));	//6
		System.out.println(StringUtils.join(table3_2.sample(true, 0.5).collect(), ","));	//1,2,6,7,7
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("표3-3 union, intersection, subtract, cartesian");
		JavaRDD<Integer> A = sc.parallelize(Arrays.asList(1,2,3));
		JavaRDD<Integer> B = sc.parallelize(Arrays.asList(3,4,5));
		System.out.println(StringUtils.join(A.union(B).collect(), ","));			//합집합, 중복허용	//1,2,3,3,4,5
		System.out.println(StringUtils.join(A.intersection(B).collect(), ","));		//교집합			//3
		System.out.println(StringUtils.join(A.subtract(B).collect(), ","));			//A-B 차집합		//2,1
		System.out.println(StringUtils.join(A.cartesian(B).collect(), ","));		//카테시안 곱		//(1,3),(1,4),(1,5),(2,3),(3,3),(2,4),(2,5),(3,4),(3,5)
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-34 자바의 reduce()");
		Integer sum = rdd.reduce((a,b) -> a+b);
		System.out.println(sum);	//10
		
		System.out.println("자바의 fold()");
		Integer sum2 = rdd.fold(0, (a,b) -> a+b);
		System.out.println(sum2);	//10
		Integer sum3 = rdd.fold(2, (a,b) -> a+b);
		System.out.println(sum3);	//16
		
		System.out.println("////////////////////////////////////////////////");
		System.out.println("예제 3-37 자바의 aggregate()");
	}
	
	public void proc3() {
//		class AvgCount implements Serializable{							//public 으로 옮기니까 serializable 에러가 사라진다.
//			private static final long serialVersionUID = 1232323L;
//			
//			public int total;
//			public int num;
//			public AvgCount(int total, int num){
//				this.total = total;
//			}
//			public double avg(){
//				return this.total / (double) this.num;
//			}
//		}
		Function2<AvgCount, Integer, AvgCount> addAndCount = (avg, i) -> {
			avg.total += i;
			avg.num += 1;
			return avg;
		};
		Function2<AvgCount, AvgCount, AvgCount> combine = (avg1, avg2) -> {
			avg1.total += avg2.total;
			avg1.num += avg2.num;
			return avg1;
		};
		AvgCount initial = new AvgCount(0, 0);
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1,2,3,4));
		AvgCount result2 = rdd2.aggregate(initial, addAndCount, combine);	//[Exception msg2] Exception in thread "main" java.io.NotSerializableException: org.apache.spark.api.java.JavaSparkContext
		System.out.println(result2.avg());	//2.5
	}
}










