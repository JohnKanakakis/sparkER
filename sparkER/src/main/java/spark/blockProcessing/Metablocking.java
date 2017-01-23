package spark.blockProcessing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import scala.Tuple2;

public class Metablocking {

	
	protected static final String ENTITY_SEPARATOR = " ";

	public static JavaPairRDD<String, String> run(final JavaPairRDD<String, String> tokenPairsRDD, final int N){
		
		JavaPairRDD<String, List<String>> filteringResult = blockFiltering(tokenPairsRDD,N);
		JavaPairRDD<String, Iterable<List<String>>> preprocessingResult = preProcessing(filteringResult);
		JavaPairRDD<String, Double> comparisonPairsRDD = metaBlocking(preprocessingResult);
		
		return comparisonPairsRDD.mapToPair(new PairFunction<Tuple2<String,Double>,String,String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Double> t)
			throws Exception {
				String entitiesPair = t._1;
				String[] entities = entitiesPair.split(Metablocking.ENTITY_SEPARATOR);
				return new Tuple2<String,String>(entities[0],entities[1]);
			}
			
		});
	}
	
	public static JavaPairRDD<Tuple2<String, String>, Set<List<String>>> createSimpleBlocksFromComparisons(final JavaPairRDD<String, List<String>> resourcesRDD,
				 						 															    final JavaPairRDD<String, String> entityComparisonsRDD){
		
		JavaPairRDD<Tuple2<String, String>, Set<List<String>>> blocks = 
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
		.groupByKey()
		.mapValues(new Function<Iterable<List<String>>,Set<List<String>>>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Set<List<String>> call(Iterable<List<String>> iterable)
			throws Exception {
				Iterator<List<String>> it = iterable.iterator();
				HashSet<List<String>> result = new HashSet<List<String>>();
				while(it.hasNext()){
					result.add(it.next());
				}
				return result;
			}
			
		});
		
		return blocks;
	}
	
	
	private static JavaPairRDD<String, List<String>> blockFiltering(JavaPairRDD<String, String> tokenPairsRDD, final int N){
		
		JavaPairRDD<String, Iterable<String>> blocksRDD = tokenPairsRDD.groupByKey();
		
		return 
		blocksRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>,String,Tuple2<String,Long>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Tuple2<String,Long>>> call(Tuple2<String, Iterable<String>> block)
			throws Exception {
				
				String token = block._1;
				
				int blockSize = Iterables.size(block._2);
				long numOfComparisons = blockSize*(blockSize-1)/2;
				
				Iterator<String> entities = block._2.iterator();
				ArrayList<Tuple2<String, Tuple2<String,Long>>> result = new ArrayList<Tuple2<String, 
																					  Tuple2<String,Long>>>();
				
				while(entities.hasNext()){
					result.add(new Tuple2<String, Tuple2<String,Long>>(entities.next(),
																	   new Tuple2<String,Long>(token,numOfComparisons)));
				}
				
			
				return result;
			}
			
		})
		.groupByKey()
		.mapValues(new Function<Iterable<Tuple2<String,Long>>,List<String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public ArrayList<String> call(Iterable<Tuple2<String, Long>> value)
			throws Exception {
				
				
				Iterator<Tuple2<String, Long>> iterator = value.iterator();
				ArrayList<Tuple2<String, Long>> blockInfoPairs = new ArrayList<Tuple2<String, Long>>();
				ArrayList<String> result = new ArrayList<String>();
				
				while(iterator.hasNext()){
					blockInfoPairs.add(iterator.next());
				}
				
				Collections.sort(blockInfoPairs,
						new Comparator<Tuple2<String, Long>>() {
							@Override
							public int compare(
									Tuple2<String, Long> t1,
									Tuple2<String, Long> t2) {
								// TODO Auto-generated method stub
								return Long.compare(t1._2,
										t2._2);
							}
						});
				
				for(int i = 0; i < Math.min(N,blockInfoPairs.size()); i++){
					result.add(blockInfoPairs.get(i)._1);
				}
				return result;
			}
			
		});
		
	}
	
	private static JavaPairRDD<String, Iterable<List<String>>> preProcessing(JavaPairRDD<String, List<String>> entityInBlocksRDD){
		
		return 
		entityInBlocksRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,List<String>>,String,List<String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, List<String>>> call(Tuple2<String, List<String>> entityInBlocksPair)
			throws Exception {
			
				ArrayList<Tuple2<String,List<String>>> result = new ArrayList<Tuple2<String,List<String>>>();
				String entity = entityInBlocksPair._1;
				List<String> blocks = entityInBlocksPair._2;
				
				blocks.add(0, entity);
				
				for(int i = 0; i < blocks.size(); i++){
					result.add(new Tuple2<String,List<String>>(blocks.get(i),blocks));
				}
				return result;
			}
			
		}).groupByKey();
	}
	
	private static JavaPairRDD<String, Double> metaBlocking(JavaPairRDD<String, Iterable<List<String>>> blocksWithListOfEntitiesAndBlocksRDD){
		
		JavaPairRDD<String, Double> entityPairs = 
		blocksWithListOfEntitiesAndBlocksRDD
		.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<List<String>>>,String,Double>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Double>> call(Tuple2<String, Iterable<List<String>>> pair)
			throws Exception {
				HashMap<String,Set<String>> entityToBlocksMap = new HashMap<String,Set<String>>();
				ArrayList<String> entities = new ArrayList<String>();
				ArrayList<Tuple2<String, Double>> result = new ArrayList<Tuple2<String, Double>>();
				
				Iterator<List<String>> iterator = pair._2.iterator();
				
				
				while(iterator.hasNext()){
					List<String> blocksOfEntity = iterator.next();
					String entity = blocksOfEntity.remove(0);
					entityToBlocksMap.put(entity, Sets.newHashSet(blocksOfEntity));
					entities.add(entity);
				}
				
				String e_i;
				String e_j; 
				double Bi;
				double Bj;
				double Bij;
				double JS;
				for(int i = 0; i < entities.size(); i++){
					e_i = entities.get(i);
					Bi = entityToBlocksMap.get(e_i).size();
					for(int j = i+1; j < entities.size();j++){
						e_j = entities.get(j);
						Bj = entityToBlocksMap.get(e_j).size();
						Bij = Sets.intersection(entityToBlocksMap.get(e_i), entityToBlocksMap.get(e_j)).size();
						JS = Bij/(Bi+Bj-Bij);
						if(e_i.compareTo(e_j) < 0){
							result.add(new Tuple2<String,Double>(e_i+Metablocking.ENTITY_SEPARATOR+e_j,JS));
						}else{
							result.add(new Tuple2<String,Double>(e_j+Metablocking.ENTITY_SEPARATOR+e_i,JS));
						}
						
					}
				}
				
				return result;
			}
		}).persist(StorageLevel.DISK_ONLY()).setName("entityPairs");
		
		long totalPairs = entityPairs.count();
		
		Function2<Double, Tuple2<String, Double>, Double> seqOp = 
		new Function2<Double, Tuple2<String, Double>, Double>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double v1,Tuple2<String, Double> t)
			throws Exception {
				
				return v1+t._2;
			}
		};
		
		Function2<Double, Double, Double> combOp = 
		new Function2<Double, Double, Double>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double v1, Double v2)
					throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		};
		
		double totalJS = entityPairs.aggregate(0d, seqOp, combOp);
		
		final double meanWeight = (double)totalJS/(double)totalPairs;
		
		return entityPairs.filter(new Function<Tuple2<String,Double>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Double> entityPair)
					throws Exception {
				
				return entityPair._2 >= meanWeight;
			}
			
		}).distinct();
	}
	
	public static void main(String[] args){
		int totalJS = 1000000000;
		int totalPairs = 123456789;
		final double meanWeight = (double)totalJS/(double)totalPairs;
		System.out.println(meanWeight);
	}
}
