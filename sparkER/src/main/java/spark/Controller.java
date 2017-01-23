package spark;

import java.io.InputStream;
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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.blockProcessing.BlockPurging;
import spark.preprocessing.EntityFilterOld;



/**
 * The Controller class is the main class of the application. 
 * It coordinates the SPARK actions and transformations needed for the linking of the two data sets. 
 * @author John Kanakakis
 *
 */
public class Controller {

	private static String STATS_FILE;
	public static Logger logger = LoggerFactory.getLogger(Controller.class);
	static SparkConf sparkConf;
	static JavaSparkContext ctx;
	
	
	static boolean purging_enabled;
	
	
	public static void main(String[] args) {
		
		/*
		 * reading and validation of LIMES configuration files
		 */
		InputStream configFile = HDFSUtils.getHDFSFile(args[0]);
		InputStream dtdFile = HDFSUtils.getHDFSFile(args[1]);

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
		
		byte[] planBinary = HDFSUtils.serialize(plan);
		byte[] configBinary = HDFSUtils.serialize(config);
		byte[] skbBinary = HDFSUtils.serialize(config.getSourceInfo());
		byte[] tkbBinary = HDFSUtils.serialize(config.getTargetInfo());

		
		/*
		 * user defines if purging will be enabled during execution
		 */
		purging_enabled = Boolean.parseBoolean(args[2]);
		
		sparkConf = new SparkConf().setAppName("Controller");

		ctx = new JavaSparkContext(sparkConf);


		Broadcast<byte[]> planBinary_B = ctx.broadcast(planBinary);
		Broadcast<byte[]> configBinary_B = ctx.broadcast(configBinary);

		
		Broadcast<byte[]> skb = ctx.broadcast(skbBinary);
		Broadcast<byte[]> tkb = ctx.broadcast(tkbBinary);


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

		
		
		
		
		
		JavaPairRDD<String, List<String>> entities1 = null;
				//EntityFilterOld.run(records1,skb);
		JavaPairRDD<String, List<String>> entities2 = null;
				//EntityFilterOld.run(records2,tkb);
		
		

		JavaPairRDD<String, List<String>> entitiesRDD = 
				entities1.union(entities2)
				.persist(StorageLevel.MEMORY_ONLY_SER())
				.setName("entities");
		
		
		
		
		
		/*
		 * creation of token pairs in the form (token, r_id)
		 */
		JavaPairRDD<String, String> tokenPairsRDD = 
				IndexCreator.getTokenPairs(entitiesRDD,skb,tkb).persist(StorageLevel.MEMORY_ONLY_SER());
		

		
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
		}

	
		/*
		 * resourceIndex RDD is created from the tokenPairs RDD
		 */
		JavaPairRDD<String, Tuple2<String, String>> resourceIndex = IndexCreator.createIndex(tokenPairsRDD);

		

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

