package spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.execution.engine.SimpleExecutionEngine;
import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.io.cache.MemoryCache;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.mapping.Mapping;
import org.aksw.limes.core.io.preprocessing.Preprocessor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


/**
 * The Linker employs the LIMES Framework to generate the links
 * @author John Kanakakis
 *
 */
public class Linker {
	
	
	public static Logger logger = LoggerFactory.getLogger(Linker.class);
	
	

	/**
	 * @param <K>
	 * @param blocks RDD in the form of (block_key, {[r_id1|info1], [r_id2|info2], ..., [r_idN|infoN]})
	 * @param planBinary_B : the broadcasted execution plan of LIMES
	 * @param configBinary_B : the broadcasted configuration of LIMES
	 * @return links RDD in the form of (source r_id, target r_id)
	 */
	public static <K> JavaPairRDD<String, String> run(final JavaPairRDD<K, Set<List<String>>> blocks, 
			               			  final Broadcast<byte[]> planBinary_B,
			               			  final Broadcast<byte[]> configBinary_B) {
		
		final org.aksw.limes.core.io.config.Configuration config = (org.aksw.limes.core.io.config.Configuration) HDFSUtils.deserialize(configBinary_B.value());
		
		final NestedPlan plan = (NestedPlan) HDFSUtils.deserialize(planBinary_B.getValue());
		
		
		
		Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>> seqFunc = 
				new Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> resourceInfoSetPerKey, 
							Tuple2<String,Double> resourceInfo) 
					throws Exception {
						// TODO Auto-generated method stub
						resourceInfoSetPerKey.add(resourceInfo);
						return resourceInfoSetPerKey;
					}
		};
		
		Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>> combFunc = 
				new Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> resourceInfoSetPerKey_1, 
											Set<Tuple2<String,Double>> resourceInfoSetPerKey_2) 
					throws Exception {
						// TODO Auto-generated method stub
						resourceInfoSetPerKey_1.addAll(resourceInfoSetPerKey_2);
						return resourceInfoSetPerKey_1;
					}
		};
		
		
		PairFlatMapFunction<Iterator<Tuple2<K, Set<List<String>>>>, String, Tuple2<String, Double>> linkF =
				new PairFlatMapFunction<Iterator<Tuple2<K, Set<List<String>>>>, String, Tuple2<String,Double>>(){
					private static final long serialVersionUID = 1L;
					SimpleExecutionEngine engine = new SimpleExecutionEngine();
					
					
				    @Override
					public Iterable<Tuple2<String, Tuple2<String,Double>>> call(Iterator<Tuple2<K, Set<List<String>>>> blocksOfPartition) throws Exception {
						// TODO Auto-generated method stub
						ArrayList<Tuple2<String,Tuple2<String,Double>>> partitionLinks = 
								new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
						
						
						Tuple2<K, Set<List<String>>> block;
						while(blocksOfPartition.hasNext()){
							block = blocksOfPartition.next();
							partitionLinks.addAll(getLinksOfBlock(engine,config,plan,block._2));
						}
						
						return partitionLinks;
					}		
		};
		
		JavaPairRDD<String, String> links 
		= blocks.mapPartitionsToPair(linkF)
				.aggregateByKey(new HashSet<Tuple2<String,Double>>(), seqFunc, combFunc)
				.mapToPair(new PairFunction<Tuple2<String,Set<Tuple2<String,Double>>>,String,String>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, String> call(Tuple2<String, Set<Tuple2<String, Double>>> linkPair) 
					throws Exception {
						double maxSim = 0.0;
						String maxTarget = "";
						for(Tuple2<String,Double> link : linkPair._2){
							if(link._2 > maxSim){
								maxSim = link._2;
								maxTarget = link._1;
							}
						}
						return new Tuple2<String,String>(linkPair._1,maxTarget);
					}
				});
		return links;
	}


	private static List<Tuple2<String, Tuple2<String, Double>>> getLinksOfBlock(SimpleExecutionEngine engine, 
																			    Configuration config, 
																			    NestedPlan plan, 
																			    Set<List<String>> block) 
	{
		final KBInfo sourceKb = config.getSourceInfo();
		final KBInfo targetKb = config.getTargetInfo();
		final String sourceVar = sourceKb.getVar();
		final String targetVar = targetKb.getVar();
		final double thres = config.getAcceptanceThreshold();
		
		ArrayList<Tuple2<String,Tuple2<String,Double>>> localLinks = 
				new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
		
		int cnt1 = 0;
		int cnt2 = 0;
		String resourceId;
		String datasetId;
		for(List<String> resource : block){
			resourceId = resource.get(0);
			//resourcesRDD.lookup(resourceId);
			datasetId = DatasetManager.getDatasetIdOfEntity(resourceId);
		  	if(datasetId.equals(sourceKb.getId())){
		  		cnt1++;
		  	}else if(datasetId.equals(targetKb.getId())){
		  		cnt2++;
		  	}
		}
		
		MemoryCache sourceCache = new MemoryCache(cnt1);
	    MemoryCache targetCache = new MemoryCache(cnt2);
	    MemoryCache cache = null;
	   
		KBInfo kb = null;

		String subject;
		String predicate = null;
		String object = null;
		String value;
		
	   	for(List<String> resourceInfo : block){
	   	
	    	subject = resourceInfo.get(0);
	    	datasetId = DatasetManager.getDatasetIdOfEntity(subject);
	    	if(datasetId.equals(sourceKb.getId())){
	    		cache = sourceCache;
	    		kb = sourceKb;
	    		
	    	}else if(datasetId.equals(targetKb.getId())){
	    		cache = targetCache;
	    		kb = targetKb;
	    		
	    	}
	    	
	    	if(resourceInfo.size()%2 == 0){
	    		logger.error("malformed list "+resourceInfo);
	    		return null;
	    	}
	    	
	    	for(int i = 1; i < resourceInfo.size()-1; i = i+2){
	    		predicate = resourceInfo.get(i);
	    		
	    		if(kb.getProperties().contains(predicate)){
	    			object = Utils.eliminateDataTypeFromLiteral(resourceInfo.get(i+1));
		    		if(kb.getFunctions().get(predicate).keySet().size() == 0){
		    			
						cache.addTriple(subject, predicate, object);
		    		}
					else{
						for (String propertyDub : kb.getFunctions().get(predicate).keySet()) {
							value = Preprocessor.process(object, kb.getFunctions().get(predicate).get(propertyDub));
							cache.addTriple(subject, propertyDub, value);
						}
					}
					
				}
	    	}
	    }
		   	
		if(sourceCache.size() == 0 || targetCache.size() == 0) return localLinks;
	   	
		engine.configure(sourceCache, targetCache, sourceVar, targetVar);

		Mapping verificationMapping = engine.execute(plan);

		Tuple2<String,Double> tp = null;
		
		HashMap<String, Double> targets;
		for(String source: verificationMapping.getMap().keySet()){
			targets = verificationMapping.getMap().get(source);
			double maxSim = 0.0;
			double sim;
        	String maxTarget = "";
        	int loopCnt = 0;
			for(String target: targets.keySet()){
				if(loopCnt == 0){
        			maxTarget = target;
        			maxSim = targets.get(target).doubleValue();
        			loopCnt++;
        		}
        		sim = targets.get(target).doubleValue();
        		 
        		if(sim >= maxSim){
        			maxSim = sim;
        			maxTarget = target;
        		}
			}
			if(maxSim >= thres){
				tp = new Tuple2<String,Double>(DatasetManager.removeDatasetIdFromEntity(maxTarget),maxSim);
				localLinks.add(new Tuple2<String,Tuple2<String,Double>>(DatasetManager.removeDatasetIdFromEntity(source),tp));
        	}
		}
	   	return localLinks;
	}	 

	/*public static JavaPairRDD<String, String> runWithComparisonPairs(final JavaPairRDD<String, List<String>> resourcesRDD,
											  						 final JavaPairRDD<String, String> entityComparisonsRDD,
											  						 final Broadcast<byte[]> configBinary_B, 
											  						 final Broadcast<byte[]> planBinary_B) 
	{
		
		final org.aksw.limes.core.io.config.Configuration config = (org.aksw.limes.core.io.config.Configuration) Utils.deserialize(configBinary_B.value());
		final NestedPlan plan = (NestedPlan) Utils.deserialize(planBinary_B.getValue());
		final SimpleExecutionEngine engine = new SimpleExecutionEngine();
		
		
		
		JavaPairRDD<Tuple2<String, String>, Iterable<List<String>>> miniBlocks = 
		entityComparisonsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,String>,String,Tuple2<String,String>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Tuple2<String, String>>> call(Tuple2<String, String> entitiesPair)
			throws Exception {
				
				ArrayList<Tuple2<String, Tuple2<String, String>>> result = 
				new ArrayList<Tuple2<String, Tuple2<String, String>>>();
				result.add(new Tuple2<String,Tuple2<String, String>>(entitiesPair._1,entitiesPair));
				result.add(new Tuple2<String,Tuple2<String, String>>(entitiesPair._2,entitiesPair));
				return result;
			}
		})
		.join(resourcesRDD)
		.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, List<String>>>, Tuple2<String, String>,List<String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Tuple2<String, String>, List<String>> call(
					Tuple2<String, Tuple2<Tuple2<String, String>, List<String>>> t)
					throws Exception {
				
				return new Tuple2<Tuple2<String, String>,List<String>>(t._2._1,t._2._2);
			}
		})
		.groupByKey();
		
		
		
		
		
		return
		entityComparisonsRDD.filter(new Function<Tuple2<String,String>,Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> entitiesPair)
			throws Exception {
				
				List<String> entityInfo_1 = resourcesRDD.lookup(entitiesPair._1).get(0);
		    	List<String> entityInfo_2 = resourcesRDD.lookup(entitiesPair._2).get(0);
		    	
		    	Set<List<String>> block = new HashSet<List<String>>();
		    	block.add(entityInfo_1);
		    	block.add(entityInfo_2);
		    	
		    	List<Tuple2<String, Tuple2<String, Double>>> links = getLinksOfBlock(engine,config,plan,block);

		    	return links.size() > 0;
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> entitiesPair)
			throws Exception {
				
				String datasetId = DatasetManager.getDatasetIdOfResource(entitiesPair._1);
				String sourceEntity;
				String targetEntity;
		    	if(datasetId.equals(config.getSourceInfo().getId())){
		    		sourceEntity = entitiesPair._1;
		    		targetEntity = entitiesPair._2;
		    	}else{
		    		sourceEntity = entitiesPair._2;
		    		targetEntity = entitiesPair._1;
		    	}
		    		
				return new Tuple2<String,String>(sourceEntity,targetEntity);
			}
		});
		
		
	}*/
}
