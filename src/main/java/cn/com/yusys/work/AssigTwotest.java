package cn.com.yusys.work;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.api.java.function.PairFlatMapFunction;

public class AssigTwotest {
	public static class Result implements Serializable{
		private String dst;
		private Integer distance;
		private String path;
		public Result() {
			dst = "";
			distance = -1;
			path = "";
		}
		
		
		public String getDst() {
			return dst;
		}
		public void setDst(String dst) {
			this.dst = dst;
		}
		public Integer getDistance() {
			return distance;
		}
		public void setDistance(Integer distance) {
			this.distance = distance;
		}
		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(dst).append(",").append(distance).append(",").append(path);
			return sb.toString();
		}
	}
	
	public static class Mycontainer implements Serializable{
		private Integer distance;
		private String path;
		private ArrayList<Tuple2<String, Integer>> edges;
		
		public Mycontainer() {
			distance = -1;
			path = "";
			edges = new ArrayList<Tuple2<String,Integer>>();
		}
		
		public Integer getDistance() {
			return distance;
		}
		public void setDistance(Integer distance) {
			this.distance = distance;
		}
		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}
		public ArrayList<Tuple2<String, Integer>> getEdges() {
			return edges;
		}
		public void setEdges(ArrayList<Tuple2<String, Integer>> edges) {
			this.edges = edges;
		}
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("[").append(distance).append(",").append(path).append(",").append(edges.toString()).append("]");
			return sb.toString();
		}
		
		
	}
	
	
	public static void main(String[] args) {
		String start = "N0";
		
		SparkConf conf = new SparkConf().setAppName("a2").setMaster("local");
		//?????
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input =  sc.textFile("/Users/mac/Documents/workspace/sparkDemo/src/main/java/cn/com/yusys/work/input.txt");
//        sc.textFile("graph").collect().forEach(System.out::println);;
        
        JavaPairRDD<String,Mycontainer>s = input.flatMapToPair(new PairFlatMapFunction<String,String,Tuple2<String,Integer>>(){

			@Override
			public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(String input) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<String, Tuple2<String, Integer>>> arrayList = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
				String[] lines = input.split(",");
				
				String src = lines[0];
				String dst = lines[1];
				Integer weight = Integer.parseInt(lines[2]);
				arrayList.add(new Tuple2<String, Tuple2<String, Integer>>(src, new Tuple2<>(dst,weight)));
				arrayList.add(new Tuple2<String, Tuple2<String, Integer>>(dst, new Tuple2<>("",-1)));
				return arrayList.iterator();
			}
        	
        }).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>,String,Mycontainer>(){

			@Override
			public Tuple2<String, Mycontainer> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> input)
					throws Exception {
				// TODO Auto-generated method stub
				String src =input._1;
				Mycontainer mycontainer = new Mycontainer();
				ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<Tuple2<String, Integer>>();
				for(Tuple2<String, Integer>t : input._2) {
					if(t._2 != -1) {
						arrayList.add(new Tuple2<String, Integer>(t._1,t._2));
					}
				}
				if(src.compareTo(start) == 0) {
					mycontainer.setDistance(0);
					mycontainer.setPath(start);
				}
				mycontainer.setEdges(arrayList);
				return new Tuple2<String, Mycontainer>(src, mycontainer);
			}
        	
        });
        
        int counter = (int)s.count();
        
        int ii= 0;
        while(ii < counter) {
	        s = s.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Mycontainer>,String, Object>(){
	
				@Override
				public Iterator<Tuple2<String, Object>> call(Tuple2<String, Mycontainer> input) throws Exception {
					// TODO Auto-generated method stub
					ArrayList<Tuple2<String, Object>>arrayList = new ArrayList<Tuple2<String, Object>>();
					String src = input._1;
					Mycontainer mycontainer =  input._2;
					String path = mycontainer.getPath();
					Integer distance = mycontainer.getDistance();
					if(mycontainer.getDistance() >= 0) {
						for(Tuple2<String, Integer>t:mycontainer.getEdges()) {
							String dst = t._1;
							Integer w = t._2;
							arrayList.add(new Tuple2<String, Object>(dst, new Tuple2<>(path+"-"+dst, distance + w)));
						}
					}
					arrayList.add(new Tuple2<String,Object>(src, mycontainer));
					return arrayList.iterator();
				}
	        	
	        }).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Object>>, String,Mycontainer>(){
	
				@Override
				public Tuple2<String, Mycontainer> call(Tuple2<String, Iterable<Object>> input) throws Exception {
					// TODO Auto-generated method stub
					String src = input._1;
					Mycontainer mycontainer = new Mycontainer();
					ArrayList<Tuple2<String,Integer>>arrayList = new ArrayList<Tuple2<String,Integer>>();
					
					
					
					for(Object o: input._2) {
						if(o instanceof Mycontainer) {
							mycontainer = (Mycontainer) o;
						}else {
							Tuple2<String,Integer> t = (Tuple2<String,Integer>) o;
							arrayList.add(new Tuple2<String,Integer>(t._1,t._2));
						}
					}
					
					if (arrayList.size() >0) {
						String minpath = arrayList.get(0)._1;
						Integer mindistance = arrayList.get(0)._2;
						for(int i = 1; i < arrayList.size(); i++) {
							if(arrayList.get(i)._2 < mindistance) {
								mindistance = arrayList.get(i)._2;
								minpath = arrayList.get(i)._1;
							}
						}
						
						if(mycontainer.getDistance() == -1) {
							mycontainer.setDistance(mindistance);
							mycontainer.setPath(minpath);
						}
						
						if(mindistance < mycontainer.getDistance()){
							mycontainer.setDistance(mindistance);
							mycontainer.setPath(minpath);
						}
					}
					
					return new Tuple2<String, Mycontainer>(src, mycontainer);
				}
	        	
	        });
	        ii++;
        }
        s.filter(new Function<Tuple2<String,Mycontainer>,Boolean>(){

			@Override
			public Boolean call(Tuple2<String, Mycontainer> input) throws Exception {
				// TODO Auto-generated method stub
				if(input._1.compareTo(start) == 0) {
					return false;
				}
				return true;
			}
        	
        }).map(new Function<Tuple2<String,Mycontainer>, Result>(){

			@Override
			public Result call(Tuple2<String, Mycontainer> input) throws Exception {
				// TODO Auto-generated method stub
				String src = input._1;
				Integer distance = input._2.getDistance();
				String path = input._2.getPath();
				Result result = new Result();
				result.setDst(src);
				result.setDistance(distance);
				result.setPath(path);
				return result;
			}
        	
        }).sortBy(new Function<Result, Integer>(){

			@Override
			public Integer call(Result input) throws Exception {
				// TODO Auto-generated method stub
				return input.getDistance();
			}
        	
        }, true, 1).saveAsTextFile("/Users/mac/Documents/workspace/sparkDemo/src/main/java/cn/com/yusys/work/output");
	}
}
