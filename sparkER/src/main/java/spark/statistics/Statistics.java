package spark.statistics;

import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.execution.planning.plan.NestedPlan;
import org.aksw.limes.core.execution.planning.planner.ExecutionPlannerFactory;
import org.aksw.limes.core.execution.planning.planner.IPlanner;
import org.aksw.limes.core.execution.rewriter.Rewriter;
import org.aksw.limes.core.execution.rewriter.RewriterFactory;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.BlocksCreator;
import spark.DatasetManager;
import spark.HDFSUtils;
import spark.IndexCreator;
import spark.SparkUtils;
import spark.blockProcessing.BlockPurging;
import spark.preprocessing.DataAggregatorByEntity;
import spark.preprocessing.DataFilter;
import spark.preprocessing.DataParser;
import spark.preprocessing.EntityFilterOld;


public class Statistics {

	
	
	public static Logger logger = LoggerFactory.getLogger(Statistics.class);
	static SparkConf sparkConf;
	static JavaSparkContext ctx;
	
	
	
	
	public static void main(String[] args) {
		
		
		String LIMES_CONFIG_XML = args[0];
		String LIMES_DTD = args[1];
		Boolean purging_enabled = Boolean.parseBoolean(args[2]);
		String STOPWORDS_FILE = args[3];
		String LINKS_FILE = args[4];
		String STATS_FILE = args[5];
		
		
		/*
		 * reading and validation of LIMES configuration files
		 */
		InputStream configFile = HDFSUtils.getHDFSFile(LIMES_CONFIG_XML);
		InputStream dtdFile = HDFSUtils.getHDFSFile(LIMES_DTD);

		XMLConfigurationReader reader = new XMLConfigurationReader();
		org.aksw.limes.core.io.config.Configuration config = reader.validateAndRead(configFile,dtdFile);



		if(config == null){
			System.exit(0);
		}
		
		
		
		HDFSUtils.deleteHDFSFile(config.getAcceptanceFile());
		HDFSUtils.deleteHDFSFile(config.getAcceptanceFile()+".ser");
		
		/*
		 * LIMES: creation of the execution plan
		 */
		Rewriter rw = RewriterFactory.getRewriter("Default");
		LinkSpecification ls = new LinkSpecification(config.getMetricExpression(), config.getVerificationThreshold());
		LinkSpecification rwLs = rw.rewrite(ls);
		IPlanner planner = ExecutionPlannerFactory.getPlanner(config.getExecutionPlan(), null, null);
		assert planner != null;
		NestedPlan plan = planner.plan(rwLs);

		/*
		 * serialization of the plan, configuration and KBInfo java objects 
		 * in order to be broadcasted via SPARK from the driver 
		 * to the executors 
		 */
		
		byte[] planBinary = SparkUtils.serialize(plan);
		byte[] configBinary = SparkUtils.serialize(config);
		byte[] skbBinary = SparkUtils.serialize(config.getSourceInfo());
		byte[] tkbBinary = SparkUtils.serialize(config.getTargetInfo());

		
		/*
		 * user defines if purging will be enabled during execution
		 */
		
		
		sparkConf = new SparkConf().setAppName("Controller");
		ctx = new JavaSparkContext(sparkConf);
		/*Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
    	hdfsConf.set("textinputformat.record.delimiter", "\n");
		 */
		
		final Broadcast<byte[]> planBinary_B = ctx.broadcast(planBinary);
		final Broadcast<byte[]> configBinary_B = ctx.broadcast(configBinary);

		
		Broadcast<byte[]> skb = ctx.broadcast(skbBinary);
		Broadcast<byte[]> tkb = ctx.broadcast(tkbBinary);


		Broadcast<HashMap<String, String>> prefixIndex_B = 
				ctx.broadcast(config.getPrefixes());
		
		
		
		/*
		 * reading of source and target data sets
		 */
		
		JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> records1 = 
				ctx.objectFile(config.getSourceInfo().getEndpoint());

		JavaRDD<Tuple2<String,Set<Tuple2<String,String>>>> records2 = 
				ctx.objectFile(config.getTargetInfo().getEndpoint());

		
		/*
		 * filtering of entities and properties according to 
		 * the LIMES .xml configuration file
		 */


		
		records1 = DataFilter.applyAllPropertiesFilter(records1, skb,prefixIndex_B);
		records2 = DataFilter.applyAllPropertiesFilter(records2, tkb,prefixIndex_B);
		
		
		
		// add datasetId to subject and shrink URIs
		JavaPairRDD<String, List<String>> entities1 = 
				DatasetManager.mapRecordsToEntities(records1,config.getSourceInfo().getId(),prefixIndex_B);
		
		JavaPairRDD<String, List<String>> entities2 = 
				DatasetManager.mapRecordsToEntities(records2,config.getTargetInfo().getId(),prefixIndex_B);
		
		
		
		
		
		

		
		
		JavaPairRDD<String, List<String>> entitiesRDD = 
				entities1.union(entities2)
				.persist(StorageLevel.MEMORY_ONLY_SER())
				.setName("entities");
		
		
		
		long totalEntities = entitiesRDD.count();
		
		
		/*
		 * creation of token pairs in the form (token, r_id)
		 */
		final Broadcast<List<String>> stopwords = ctx.broadcast(ctx.textFile(STOPWORDS_FILE ).collect());
		
		JavaPairRDD<String, String> tokenPairsRDD = 
				IndexCreator.getTokenPairs(entitiesRDD,skb,tkb,stopwords)
				.persist(StorageLevel.MEMORY_ONLY_SER())
				.setName("tokenIndex");
		

		
		/*
		 * creation of blockSizesRDD.
		 * It represents the size of each block
		 * (block_key, size)
		 */
		JavaPairRDD<String, Integer> blockSizesRDD = 
				BlockPurging.getBlockSizes(tokenPairsRDD)
				.persist(StorageLevel.MEMORY_ONLY_SER())
				.setName("blockSizesRDD");

		
		/*
		 * purging
		 */

		final int optimalSize = BlockPurging.getOptimalBlockSize(blockSizesRDD);

		


		/*
		 * blockSizes are purged 
		 */
		
		
		//filtering (token,N) , N < optimalSize
		blockSizesRDD = blockSizesRDD.filter(new Function<Tuple2<String,Integer>,Boolean>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String,Integer> block) 
			throws Exception {
				// TODO Auto-generated method stub
				return (block._2 > 1 && block._2 <= optimalSize);
			}
		});


		/*
		 * blockSizesRDD is locally collected and distributed as a HashSet
		 */
		HashSet<String> localPurgedBlockKeysSet = new HashSet<String>(blockSizesRDD.keys().collect());

		final Broadcast<HashSet<String>> broadcastedPurgedBlockKeys = 
				ctx.broadcast(localPurgedBlockKeysSet);


		/*
		 * if purging is enabled the tokenPairs RDD is purged
		 */
		if(purging_enabled){
			tokenPairsRDD 
			= tokenPairsRDD.filter(new Function<Tuple2<String,String>,Boolean>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, String> indexPair) throws Exception {
					String blockKey = indexPair._1;
					return broadcastedPurgedBlockKeys.getValue().contains(blockKey);
				}
			});
			
			
			
			/*
			tokenPairsRDD 
			= tokenPairsRDD.filter(new Function<Tuple2<String,String>,Boolean>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, String> indexPair) throws Exception {
					String blockKey = indexPair._1;
					return !stopwords.getValue().contains(blockKey);
				}
			});*/
		}

		BigInteger comparisons = getNumberOfComparisons(tokenPairsRDD,skb,tkb);
	
		
		
		JavaRDD<Tuple2<String, String>> links = ctx.objectFile(LINKS_FILE);
		
		
		ArrayList<String> result = new ArrayList<String>();
		
		result.add("total links = "+links.count());
	    result.add("total entities = "+totalEntities);
	    result.add("total comparisons = "+comparisons);
	    result.add("ratio = "+comparisons.longValue()/totalEntities*100+"%");
	    
	    ctx.parallelize(result).coalesce(1).saveAsTextFile(STATS_FILE);
	    
		ctx.close();
	}

	
	
	
	


	private static BigInteger getNumberOfComparisons(JavaPairRDD<String, String> tokenPairs,
			final Broadcast<byte[]> skb,
			final Broadcast<byte[]> tkb) {
	
		Function2<Set<String>, String, Set<String>> secFunc
		= new Function2<Set<String>, String, Set<String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Set<String> call(Set<String> set, String resourceId) throws Exception {
				set.add(resourceId);
				return set;
			}
		};
	
		Function2<Set<String>, Set<String>, Set<String>> combFunc
		= new Function2<Set<String>, Set<String>, Set<String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Set<String> call(Set<String> set1, Set<String> set2) throws Exception {
				set1.addAll(set2);
				return set1;
			}
		};
	
		JavaPairRDD<String, Set<String>> blocks = 
				tokenPairs.aggregateByKey(new HashSet<String>(), secFunc,combFunc);
	
		AccumulatorParam<BigInteger> accParam = new AccumulatorParam<BigInteger>(){
	
			private static final long serialVersionUID = 1L;
	
			@Override
			public BigInteger addInPlace(BigInteger arg0, BigInteger arg1) {
				return arg0.add(arg1);
			}
	
			@Override
			public BigInteger zero(BigInteger arg0) {
				return BigInteger.ZERO;
			}
	
			@Override
			public BigInteger addAccumulator(BigInteger arg0, BigInteger arg1) {
				return arg0.add(arg1);
			}
		};
		final Accumulator<BigInteger> numberOfComparisons = ctx.accumulator(BigInteger.ZERO,accParam);
	
		final KBInfo sourceKb = (KBInfo) SparkUtils.deserialize(skb.value());
		final KBInfo targetKb = (KBInfo) SparkUtils.deserialize(tkb.value());
	
		blocks.foreach(new VoidFunction<Tuple2<String,Set<String>>>(){
	
			private static final long serialVersionUID = 1L;
	
			@Override
			public void call(Tuple2<String, Set<String>> blockPair) throws Exception {
				// TODO Auto-generated method stub
				long cnt1 = 0;
				long cnt2 = 0;
				String datasetId;
				for(String resourceId : blockPair._2){
					datasetId = DatasetManager.getDatasetIdOfEntity(resourceId);
					if(datasetId.equals(sourceKb.getId())){
						cnt1++;
					}else if(datasetId.equals(targetKb.getId())){
						cnt2++;
					}
				}
				numberOfComparisons.add(BigInteger.valueOf(cnt1).multiply(BigInteger.valueOf(cnt2)));
			}
		});
		return numberOfComparisons.value();
	}

}