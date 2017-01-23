package spark.preprocessing;



import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.limes.core.io.config.KBInfo;
//import org.apache.jena.rdf.model.Property;
//import org.apache.jena.vocabulary.RDF;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import spark.DatasetManager;
import spark.HDFSUtils;


/**
 * The EntityFilter filters the data according to the LIMES configuration
 * @author John Kanakakis
 *
 */
public class EntityFilterOld {
	/*public static Logger logger = LoggerFactory.getLogger(EntityFilterOld.class);
	public final static Property TYPE_PROPERTY = RDF.type;
	
	
	
    private static boolean hasCorrectProperties(Set<Tuple2<String, String>> entityProperties, HashSet<String> configProperties){
    	
    	HashSet<String> entityPropertiesNames = new HashSet<String>();
    	for(Tuple2<String, String> po : entityProperties)
    		entityPropertiesNames.add(po._1);
    	
    	
    	
    	if(entityPropertiesNames.containsAll(configProperties))
    		return true;
    	
    	return false;
    }
    
    private static List<String> filterEntityInfo(Set<Tuple2<String, String>> pos, HashSet<String> configProperties){
    	
    	ArrayList<String> infoTokeep = new ArrayList<String>();
    	for(Tuple2<String, String> po : pos){
    		if(configProperties.contains(po._1)){
    			infoTokeep.add(po._1);
    			infoTokeep.add(po._2);
    		}
    	}
    	if(infoTokeep.size() == 2)
    		return null;
    	return infoTokeep;
    }
    
   
	*//**
	 * @param records : the input data RDD
	 * @param kbB : 
	 * the broadcasted KBInfo object which holds the information of
	 * the entities (type class, properties etc) involved in the linking task 
	 * @return the filtered data in the form of (r_id, [info]) 
	 *//*
	public static JavaPairRDD<String, List<String>> run(
			JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> records, final Broadcast<byte[]> kbB) {
		// TODO Auto-generated method stub
		final KBInfo kb = (KBInfo)HDFSUtils.deserialize(kbB.getValue());
		
		final HashSet<String> configProperties = new HashSet<String>(kb.getProperties());
		configProperties.add(TYPE_PROPERTY.toString());
		
		
		JavaPairRDD<String, List<String>> result = 
		records.filter(new Function<Tuple2<String, Set<Tuple2<String, String>>>,Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Set<Tuple2<String, String>>> t) throws Exception {
				// TODO Auto-generated method stub
			
				boolean hasTypeClass = false;
				for(Tuple2<String,String> po : t._2){
					if(po._1.equals(TYPE_PROPERTY.toString()) && po._2.equals(kb.getClassRestriction())){
						hasTypeClass = true;
					}
				}
				if(hasTypeClass){
					if(hasCorrectProperties(t._2,configProperties)){
						return true;
					};
				}
				
				return false;
				
				//return true;
			}
			
		}).mapToPair(new PairFunction<Tuple2<String, Set<Tuple2<String, String>>>,String,List<String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(Tuple2<String, Set<Tuple2<String, String>>> entity)
					throws Exception {
				
				List<String> infoToKeep = filterEntityInfo(entity._2,configProperties);
				if(infoToKeep == null){
					logger.error("accepted false tuple "+entity);
					return null;
				}
				String r_id = DatasetManager.addDatasetIdToEntity(entity._1, kb.getId());
				
				return new Tuple2<String,List<String>>(r_id,infoToKeep);
			}
		});
		return result;
	}*/
}


