package spark;

import java.io.InputStream;
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
import org.aksw.limes.core.io.config.reader.xml.XMLConfigurationReader;
import org.aksw.limes.core.io.ls.LinkSpecification;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.blockProcessing.BlockPurging;
import spark.preprocessing.DataAggregatorByEntity;
import spark.preprocessing.DataFilter;
import spark.preprocessing.DataParser;
import spark.preprocessing.EntityFilterOld;



/**
 * The Controller class is the main class of the application. 
 * It coordinates the SPARK actions and transformations needed for the linking of the two data sets. 
 * @author John Kanakakis
 *
 */
public class Controller {

	
	public static Logger logger = LoggerFactory.getLogger(Controller.class);
	static SparkConf sparkConf;
	static JavaSparkContext ctx;
	
	
	
	
	public static void main(String[] args) {
		
		//xml configuration file
		String LIMES_CONFIG_XML = args[0];
		
		//limes_dtd file for validation of .xml file
		String LIMES_DTD = args[1];
		
		//purging
		Boolean purging_enabled = Boolean.parseBoolean(args[2]);
		
		//stopwords file
		String STOPWORDS_FILE = args[3];
		
		//filters only the entities with all properties of LIMES configuration 
		boolean filter_all = Boolean.parseBoolean(args[4]);
		
		
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
		
		
		//deleting previous links file
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

		
		sparkConf = new SparkConf().setAppName("Controller");
		ctx = new JavaSparkContext(sparkConf);
		

		//broadcasting of necessary java objects (plan and configuration) -> LIMES objects
		Broadcast<byte[]> planBinary_B = ctx.broadcast(planBinary);
		Broadcast<byte[]> configBinary_B = ctx.broadcast(configBinary);

		
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

		
		
		if(filter_all){
			records1 = DataFilter.applyAllPropertiesFilter(records1, skb,prefixIndex_B);
			records2 = DataFilter.applyAllPropertiesFilter(records2, tkb,prefixIndex_B);
		}
		
		
		
		
		
		// add datasetId to subject and shrink URIs
		JavaPairRDD<String, List<String>> entities1 = 
				DatasetManager.mapRecordsToEntities(records1,config.getSourceInfo().getId(),prefixIndex_B);
		
		JavaPairRDD<String, List<String>> entities2 = 
				DatasetManager.mapRecordsToEntities(records2,config.getTargetInfo().getId(),prefixIndex_B);
		
		
		
		
		
		

		//union of source and target entities to a single entitiesRDD
		
		JavaPairRDD<String, List<String>> entitiesRDD = 
				entities1.union(entities2)
				.persist(StorageLevel.MEMORY_ONLY_SER())
				.setName("entities");
		
		
		
		
		
		/*
		 * creation of token pairs in the form (token, r_id)
		 */
		
		//broadcasting of stopwords file
		final Broadcast<List<String>> stopwords = ctx.broadcast(ctx.textFile(STOPWORDS_FILE).collect());
		
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
		 * if purging is enabled the tokenPairs RDD is purged
		 */
		if(purging_enabled){
			
			/*
			 * optimal blocksize
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
			
			//localPurgedBlockKeysSet are the accepted tokens: the ones that have remained after the purging
			HashSet<String> localPurgedBlockKeysSet = new HashSet<String>(blockSizesRDD.keys().collect());

			final Broadcast<HashSet<String>> broadcastedPurgedBlockKeys = 
					ctx.broadcast(localPurgedBlockKeysSet);


			//filter tokenPairs with token in localPurgedBlockKeysSet
			
			tokenPairsRDD 
			= tokenPairsRDD.filter(new Function<Tuple2<String,String>,Boolean>(){
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, String> indexPair) throws Exception {
					String blockKey = indexPair._1;
					return broadcastedPurgedBlockKeys.getValue().contains(blockKey);
				}
			});
			
		}

		
		/**
		 *  tokenPairs RDD in the form of (block_key, entityID)
		 *  the resourceIndex RDD in the form of (entityID, (block_key, entityID) )
		 */
	
		/*
		 * resourceIndex RDD is created from the tokenPairs RDD
		 */
		JavaPairRDD<String, Tuple2<String, String>> resourceIndex = IndexCreator.createIndex(tokenPairsRDD);

		
		/**
		 * creates blocks from the join of the entities RDD and the resourceIndex RDD
	 	 * @param resourceIndex : (e_id,(token, e_id) )
		 * @param entities : (e_id, [info])
		 * @return blocks in the form of (token, { [e_id1|info1], [e_id2|info2], ..., [e_idN|infoN]})
		 */
		/*
		 * blocks RDD is created from the resourceIndex RDD and the resources RDD
		 */
		JavaPairRDD<String, Set<List<String>>> blocks = BlocksCreator.createBlocks(resourceIndex,entitiesRDD);

		
		/*
		 * At this point the LIMES is used to generate the links
		 */
		
		JavaPairRDD<String, String> links = null;
		links = Linker.run(blocks, planBinary_B, configBinary_B);
		links.persist(StorageLevel.MEMORY_ONLY_SER()).setName("linksRDD");

		links.saveAsTextFile(config.getAcceptanceFile());
		links.saveAsObjectFile(config.getAcceptanceFile()+".ser");
		
		ctx.close();
	}

	
	
	
	
}

