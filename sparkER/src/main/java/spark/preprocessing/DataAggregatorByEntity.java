package spark.preprocessing;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class DataAggregatorByEntity {

	
	public static JavaPairRDD<String, Set<Tuple2<String, String>>> run(
			JavaPairRDD<String, Tuple2<String, String>> triplesRDD,int partitions) {
		
		return 
				triplesRDD
				.aggregateByKey(new HashSet<Tuple2<String,String>>(), partitions,
			  		  	  new Function2<Set<Tuple2<String,String>>,Tuple2<String,String>,Set<Tuple2<String,String>>>(){
								private static final long serialVersionUID = 1L;
								@Override
								public Set<Tuple2<String,String>> call(Set<Tuple2<String,String>> set,Tuple2<String,String> po) 
								throws Exception {
									// TODO Auto-generated method stub
									if(po != null){
										set.add(po);
									}
									
									return set;
								}
						  }, 
			  		  	  new Function2<Set<Tuple2<String,String>>,Set<Tuple2<String,String>>,Set<Tuple2<String,String>>>(){

								private static final long serialVersionUID = 1L;
								@Override
								public Set<Tuple2<String,String>> call(Set<Tuple2<String,String>> set1,
														Set<Tuple2<String,String>> set2) 
								throws Exception {
									// TODO Auto-generated method stub
									set1.addAll(set2);
									return set1;
								}
						  });
	}
}
