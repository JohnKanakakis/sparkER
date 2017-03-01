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
	 * @param blocks RDD in the form of (token, {[e_id1|info1], [e_id2|info2], ..., [e_idN|infoN]})
	 * @param planBinary_B : the broadcasted execution plan of LIMES
	 * @param configBinary_B : the broadcasted configuration of LIMES
	 * @return links RDD in the form of (source e_id, target e_id)
	 */
	public static <K> JavaPairRDD<String, String> run(final JavaPairRDD<K, Set<List<String>>> blocks, 
			               			  final Broadcast<byte[]> planBinary_B,
			               			  final Broadcast<byte[]> configBinary_B) {
		
		final org.aksw.limes.core.io.config.Configuration config = (org.aksw.limes.core.io.config.Configuration) SparkUtils.deserialize(configBinary_B.value());
		
		final NestedPlan plan = (NestedPlan) SparkUtils.deserialize(planBinary_B.getValue());
		
		
		
		//for each source Entity (key), the sequence function aggregates locally (on a single node) the
		//current set of target tuples (<target entityID,simScore>) with a new target tuple
		
		Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>> seqFunc = 
				new Function2<Set<Tuple2<String,Double>>,Tuple2<String,Double>,Set<Tuple2<String,Double>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> targetWithSimTuplesSet, 
							Tuple2<String,Double> targetWithSimTuple) 
					throws Exception {
						// TODO Auto-generated method stub
						targetWithSimTuplesSet.add(targetWithSimTuple);
						return targetWithSimTuplesSet;
					}
		};
		
		//for each source Entity (key), the combination function aggregates globally (from two or more nodes) the
		//sets of target tuples (<target entityID,simScore>)
		Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>> combFunc = 
				
				new Function2<Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>,Set<Tuple2<String,Double>>>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Set<Tuple2<String,Double>> call(Set<Tuple2<String,Double>> targetWithSimTuplesSet_1, 
											Set<Tuple2<String,Double>> targetWithSimTuplesSet_2) 
					throws Exception {
						// TODO Auto-generated method stub
						targetWithSimTuplesSet_1.addAll(targetWithSimTuplesSet_2);
						return targetWithSimTuplesSet_1;
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
				
				//for each source, only the max similarity from all the targets is needed 
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


	// LIMES is used here to link the entities of each block
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
		String entityId;
		String datasetId;
		for(List<String> entityInfoList : block){
			entityId = entityInfoList.get(0);
			//resourcesRDD.lookup(resourceId);
			datasetId = DatasetManager.getDatasetIdOfEntity(entityId);
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
		
	   	for(List<String> entityInfoList : block){
	   	
	    	subject = entityInfoList.get(0);
	    	if(subject == null) continue;
	    	
	    	datasetId = DatasetManager.getDatasetIdOfEntity(subject);
	    	if(datasetId.equals(sourceKb.getId())){
	    		cache = sourceCache;
	    		kb = sourceKb;
	    		
	    	}else if(datasetId.equals(targetKb.getId())){
	    		cache = targetCache;
	    		kb = targetKb;
	    		
	    	}
	    	
	    	if(entityInfoList.size()%2 == 0){
	    		logger.error("malformed list "+entityInfoList);
	    		return null;
	    	}
	    	
	    	for(int i = 1; i < entityInfoList.size()-1; i = i+2){
	    		predicate = entityInfoList.get(i);
	    		
	    		if(predicate == null){
	    			logger.error("predicate found null for subject "+subject);
	    			continue;
	    		}
	    		if(kb.getProperties().contains(predicate)){
	    			object = SparkUtils.eliminateDataTypeFromLiteral(entityInfoList.get(i+1));
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

		Tuple2<String,Double> targetWithSimTuple = null;
		
		HashMap<String, Double> targets;
		for(String source: verificationMapping.getMap().keySet()){
			targets = verificationMapping.getMap().get(source);
			double maxSim = 0.0;
			double sim;
        	String maxTarget = "";
        	int loopCnt = 0;
			for(String target: targets.keySet()){
				
				if(target == null){
					logger.error("target found null");
					continue;
				}
				
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
			/*if(maxTarget == null){//just in case
				continue;
			}*/
			if(maxSim >= thres){
				targetWithSimTuple = new Tuple2<String,Double>(DatasetManager.removeDatasetIdFromEntity(maxTarget),maxSim);
				localLinks.add(new Tuple2<String,Tuple2<String,Double>>(DatasetManager.removeDatasetIdFromEntity(source),targetWithSimTuple));
        	}
		}
	   	return localLinks;
	}	 

	
}
