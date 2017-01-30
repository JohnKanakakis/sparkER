package spark.statistics;

import java.util.ArrayList;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.hp.hpl.jena.vocabulary.OWL;

import scala.Tuple2;
import spark.HDFSUtils;

public class LinkStatistics {

	
	private static  String LINKS_STATS_FILE;// = "/user/kanakakis/groundTruth/link_stats_file";
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		LINKS_STATS_FILE = args[2];
		HDFSUtils.deleteHDFSFile(LINKS_STATS_FILE);
		SparkConf sparkConf = new SparkConf().setAppName("LinkStatistics");
    	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    	
    	
        JavaRDD<Tuple2<String, String>> links = ctx.objectFile(args[0]);
        
        JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> data = ctx.objectFile(args[1]);
		
		
		JavaPairRDD<String,String> realLinkedPairs = 
		data.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Set<Tuple2<String,String>>>,String,String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<String, Set<Tuple2<String, String>>> resource) throws Exception {
				// TODO Auto-generated method
				ArrayList<Tuple2<String,String>> result = new ArrayList<Tuple2<String,String>>();
				
				for(Tuple2<String,String> po : resource._2){
					if(po._1.equals(OWL.sameAs.toString())){
						result.add(new Tuple2<String,String>(resource._1,po._2));
					}
				}
				return result;
			}
		});
		
		/*JavaRDD<Tuple2<String, Set<Tuple2<String, Double>>>> oneToNLinks = 
        links.filter(new Function<Tuple2<String, Set<Tuple2<String, Double>>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, Double>>> link)
					throws Exception {
				// TODO Auto-generated method stub
				return (link._2.size() > 1);
			}
        });*/
		
       /* JavaRDD<Tuple2<String, Set<Tuple2<String, Double>>>> oneToOneLinks = 
        links.filter(new Function<Tuple2<String, Set<Tuple2<String, Double>>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, Double>>> link)
					throws Exception {
				// TODO Auto-generated method stub
				return (link._2.size() == 1);
			}
        }).persist(StorageLevel.MEMORY_AND_DISK_SER());*/
      
       
        //oneToOneLinks.saveAsTextFile(args[6]+"/oneToOneLinks");
        //oneToNLinks.saveAsTextFile(args[6]+"/oneToNLinks");
		
		JavaPairRDD<String,String> resultLinkedPairs = 
		links.mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> linkPair) throws Exception {
				
				return new Tuple2<String,String>(linkPair._1,linkPair._2);
			}
		});
		
		
	    
	    
	    JavaPairRDD<String, Tuple2<String, String>> comparisonPairs = realLinkedPairs.join(resultLinkedPairs).cache();
	    //comparisonPairs.saveAsTextFile("/user/kanakakis/groundTruth/comparisonPairs");
	    
	    JavaPairRDD<String, Tuple2<String, String>> trueMatches = 
	    comparisonPairs.filter(new Function<Tuple2<String, Tuple2<String, String>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<String, String>> comparisonPair) throws Exception {
				// TODO Auto-generated method stub
				String resultTarget = comparisonPair._2._1;
				String realTarget = comparisonPair._2._2;
				return resultTarget.equals(realTarget);
			}
	    });
	    
	    JavaPairRDD<String, Tuple2<String, String>> falseMatches = 
	    comparisonPairs.filter(new Function<Tuple2<String, Tuple2<String, String>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<String, String>> comparisonPair) throws Exception {
				// TODO Auto-generated method stub
				String resultTarget = comparisonPair._2._1;
				String realTarget = comparisonPair._2._2;
				return !resultTarget.equals(realTarget);
			}
	    });
	   
	    JavaPairRDD<String, String> missedMatches = realLinkedPairs.subtract(resultLinkedPairs).cache();
	    
		ArrayList<String> result = new ArrayList<String>();
	    result.add("links = "+links.count());
	    
	    long tp = trueMatches.count();
	    long fn = missedMatches.count();//Math.abs(resultLinkedPairs.count()-realLinkedPairs.count());
	    long fp = falseMatches.count();
	    double recall = ((double)tp/(tp+fn))*100;
	    //double precision = ((double)tp/(tp+fp))*100;
	    result.add("correct links (true matches) = "+ tp);
	    result.add("real link pairs "+realLinkedPairs.count());
	    result.add("wrong links (false matches) = "+ fp);
	    result.add("missed links  = "+ fn);
	   
	    result.add("recall = "+ recall + "%");
	    //result.add("oneToNlinks = "+oneToNLinks.count());
	    //result.add("precision = "+ precision + "%");
	    result.add("missed links \n"+ missedMatches.collect());
	    ctx.parallelize(result).coalesce(1).saveAsTextFile(LINKS_STATS_FILE);
    	ctx.close();
	}
}


