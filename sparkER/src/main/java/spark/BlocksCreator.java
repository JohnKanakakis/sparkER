package spark;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


/**
 * BlocksCreator generates the enriched blocks used for the linking task
 * @author John Kanakakis
 *
 */
public class BlocksCreator {

	
	public static Logger logger = LoggerFactory.getLogger(BlocksCreator.class);
	
	
	
	/**
	 * creates blocks from the join of the resources RDD and the resourceIndex RDD
	 * @param resourceIndex : (r_id,(block_key, r_id) )
	 * @param resources : (r_id, [info])
	 * @return blocks in the form of (block_key, { [r_id1|info1], [r_id2|info2], ..., [r_idN|infoN]})
	 */
	public static JavaPairRDD<String, Set<List<String>>> createBlocks( JavaPairRDD<String, Tuple2<String, String>> resourceIndex,
																		    JavaPairRDD<String, List<String>> resources) {
		
		Function2<Set<List<String>>,List<String>,Set<List<String>>> seqFunc = 
				new Function2<Set<List<String>>,List<String>,Set<List<String>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<List<String>> call(Set<List<String>> resourceInfoListPerKey, 
							List<String> resourceInfo) 
					throws Exception {
						
						resourceInfoListPerKey.add(resourceInfo);
						return resourceInfoListPerKey;
					}
		};
		
		Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>> combFunc = 
				new Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<List<String>> call(Set<List<String>> resourceInfoListPerKey_1, 
											Set<List<String>> resourceInfoListPerKey_2) 
					throws Exception {
						// TODO Auto-generated method stub
						resourceInfoListPerKey_1.addAll(resourceInfoListPerKey_2);
						return resourceInfoListPerKey_1;
					}
		};
		
		/* (r_id, (block_key,r_id) ) join (r_id, [info])  =>
		 * (r_id, ((block_key,r_id), [info]) ) =>
		 * (block_key, [r_id|info])
		 */
		return 
				resourceIndex.join(resources)
					  .mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple2<String, String>,List<String>>>,String,List<String>>(){
							private static final long serialVersionUID = 1L;
					
							@Override
							public Tuple2<String,List<String>> call(
									Tuple2<String,Tuple2<Tuple2<String, String>, List<String>>> pair) throws Exception {
								// TODO Auto-generated method stub
								Tuple2<String, String> wordResourceId_pair = pair._2._1;
								
								List<String> info = pair._2._2;
								
								String word = wordResourceId_pair._1;
								String r_id = wordResourceId_pair._2;
								if(info.size()%2 != 0){
								}else{
									info.add(0, r_id);
								}
								return new Tuple2<String, List<String>>(word,info);
							}
					  })
					  .aggregateByKey(new HashSet<List<String>>(),seqFunc,combFunc);
	}
}