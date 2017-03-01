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
	 * creates blocks from the join of the entities RDD and the entityIndex RDD
	 * @param entityIndex : (e_id,(token, e_id) )
	 * @param entities : (e_id, [info])
	 * @return blocks in the form of (token, { [e_id1|info1], [e_id2|info2], ..., [e_idN|infoN]})
	 */
	public static JavaPairRDD<String, Set<List<String>>> createBlocks( JavaPairRDD<String, Tuple2<String, String>> entityIndex,
																		    final JavaPairRDD<String, List<String>> entities) {
		
		
		//sequence function aggregates locally (at each node) the current list of entities per token (key)
		//with a new entity
		Function2<Set<List<String>>,List<String>,Set<List<String>>> seqFunc = 
				new Function2<Set<List<String>>,List<String>,Set<List<String>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<List<String>> call(Set<List<String>> entityInfoListPerKey, 
							List<String> entityInfo) 
					throws Exception {
						
						entityInfoListPerKey.add(entityInfo);
						return entityInfoListPerKey;
					}
		};
		
		//combination function merges two lists of entities per token (key) from two different nodes
		Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>> combFunc = 
				new Function2<Set<List<String>>,Set<List<String>>,Set<List<String>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<List<String>> call(Set<List<String>> entityInfoListPerKey_1, 
											Set<List<String>> entityInfoListPerKey_2) 
					throws Exception {
						
						entityInfoListPerKey_1.addAll(entityInfoListPerKey_2);
						return entityInfoListPerKey_1;
					}
		};
		
		/* (e_id, (token,e_id) ) join (e_id, [info])  =>
		 * (e_id, ((token,e_id), [info]) ) =>
		 * (token, [e_id|info])
		 */
		return 
				entityIndex.join(entities)
					  .mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple2<String, String>,List<String>>>,String,List<String>>(){
							private static final long serialVersionUID = 1L;
					
							@Override
							public Tuple2<String,List<String>> call(
									Tuple2<String,Tuple2<Tuple2<String, String>, List<String>>> pair) throws Exception {
								// TODO Auto-generated method stub
								Tuple2<String, String> tokenResourceId_pair = pair._2._1;
								
								List<String> info = pair._2._2;
								
								String token = tokenResourceId_pair._1;

								String r_id = tokenResourceId_pair._2;
								if(info.size()%2 != 0){
								}else{
									info.add(0, r_id);
								}
								return new Tuple2<String, List<String>>(token,info);
							}
					  })
					  .aggregateByKey(new HashSet<List<String>>(),seqFunc,combFunc);
	}
}