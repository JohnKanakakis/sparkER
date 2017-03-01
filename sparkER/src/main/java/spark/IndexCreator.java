package spark;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.io.config.KBInfo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * IndexCreator generates the tokenPairs RDD and the entityIndex RDD
 * @author John Kanakakis
 *
 */
public class IndexCreator {

	
	public static Logger logger = LoggerFactory.getLogger(IndexCreator.class);
	
	

	/**
	 * @param entitiesRDD : RDD in the form of (entityID, [info])
	 * @param skbB : source KBInfo
	 * @param tkbB : target KBInfo
	 * @param stopwords 
	 * @return tokenPairs in the form of (token, entityID)
	 */
	public static JavaPairRDD<String, String> getTokenPairs(JavaPairRDD<String,List<String>> entitiesRDD,
																  final Broadcast<byte[]> skbB,
																  final Broadcast<byte[]> tkbB,
																  Broadcast<List<String>> stopwords_B)
	{
		final KBInfo skb = (KBInfo)SparkUtils.deserialize(skbB.getValue());
		final KBInfo tkb = (KBInfo)SparkUtils.deserialize(tkbB.getValue());
		final Set<String> sourceProperties = new HashSet<String>(skb.getProperties());
		final Set<String> targetProperties = new HashSet<String>(tkb.getProperties());
		final Set<String> stopwords = new HashSet<String>(stopwords_B.getValue());
		
		
		// pair = (BKV, (valueOfProperty, entityID) )
		PairFlatMapFunction<Tuple2<String, List<String>>, String, String> createTokenPairsFunction = 
		
		new PairFlatMapFunction<Tuple2<String,List<String>>,String,String>(){
		
			private static final long serialVersionUID = 1L;
			
			@Override
			public Set<Tuple2<String, String>> call(Tuple2<String, 
													List<String>> entity)
			throws Exception {
			
				HashSet<Tuple2<String,String>> tokenPairs = 
						new HashSet<Tuple2<String,String>>();
				
				Set<String> kbProperties = null;
				//Tuple2<String, List<String>> entity = null;
				String e_id = null;
				List<String> r_info = null;
				String predicate = null;
				String object = null;
				String BKV = "";
				Tuple2<String,String> t;
				String datasetId = "";
				String[] tokens;
				
				//while(entities.hasNext()){
				//	entity = entities.next();
				e_id = entity._1;
				r_info = entity._2;
				datasetId = DatasetManager.getDatasetIdOfEntity(e_id);
				
				//kbproperties are the LIMES properties of the source or the target specification, 
				//depending on the datasetID of the entity
				
				if(datasetId.equals(tkb.getId())){
					kbProperties = targetProperties;
				}else if(datasetId.equals(skb.getId())){
					kbProperties = sourceProperties;
				}
				
				
				for(int i = 0; i < r_info.size()-1; i = i + 2){
					predicate = r_info.get(i);
				
					if(kbProperties.contains(predicate)){
						object = SparkUtils.eliminateDataTypeFromLiteral(r_info.get(i+1));
						BKV+=(object.replace("\"", "")+" ");
					}
				}
				tokens = BKV.toLowerCase().replaceAll("[^A-Za-z0-9 ]", " ").split(" ");
				
				for(int i = 0; i < tokens.length; i++){
					tokens[i] = tokens[i].trim();
					if(tokens[i].length() == 0) continue;
					
					
					if(stopwords.contains(tokens[i])) continue;
					
					t = new Tuple2<String,String>(tokens[i],e_id);
					tokenPairs.add(t);
				}
					
				//}
				return tokenPairs;	
			}
		};
		
		JavaPairRDD<String, String> tokenPairsRDD = entitiesRDD.flatMapToPair(createTokenPairsFunction);
		return tokenPairsRDD;
	}
	
	
	

	/**
	 * @param tokenPairs RDD in the form of (block_key, entityID)
	 * @return the entityIndex RDD in the form of (entityID, (block_key, entityID) )
	 */
	public static JavaPairRDD<String, Tuple2<String, String>> createIndex(JavaPairRDD<String, String> tokenPairs) {
		JavaPairRDD<String, Tuple2<String, String>> index 
		= tokenPairs.mapToPair(new PairFunction<Tuple2<String,String>,String,Tuple2<String,String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> tokenPair) 
			throws Exception {
				String entityId = tokenPair._2;
				return new Tuple2<String, Tuple2<String, String>>(entityId,tokenPair);
			}
		});
		
		return index;
	}
}
