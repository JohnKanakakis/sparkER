package spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * The DatasetManager is used to add a data set stamp to each entity URI
 * so as to distinguish between source and target URIs.
 * 
 * @author John Kanakakis
 *
 */
public class DatasetManager implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final String DATASET_STAMP = "_";
	public static Logger logger = LoggerFactory.getLogger(DatasetManager.class);
	
	
	//inverts the prefixesMap (prefix,baseURI) to (baseURI,prefix)
	public static HashMap<String,String> invertPrefixIndex(HashMap<String, String> prefixes){
		
		//invert prefixes
		HashMap<String, String> invertedPrefixes = new HashMap<String, String>();
		
		for(Entry<String,String> entry: prefixes.entrySet()){
			invertedPrefixes.put(entry.getValue(), entry.getKey());
		}
		
		return invertedPrefixes;
	}
	
	public static String shrinkURI(String uri,HashMap<String, String> prefixes) {
			
		String baseURI;
		String name;
		int indexOfBaseURIDelimeter;
		if(uri.contains("#")){
			indexOfBaseURIDelimeter = uri.lastIndexOf("#");
		}else{
			indexOfBaseURIDelimeter = uri.lastIndexOf("/");
		}
		if(indexOfBaseURIDelimeter == -1){
			logger.warn("invalid URI "+uri);
			
			return null;
		}
		
		baseURI = uri.substring(0,indexOfBaseURIDelimeter+1);
		name = uri.substring(indexOfBaseURIDelimeter+1);
		
		
		String prefix = prefixes.get(baseURI);
		
		if(prefix == null){
			logger.warn("invalid baseURI "+baseURI);
			logger.warn(uri+" ---> "+baseURI + " + "+name);
			//System.exit(0);
			return null;
		}
		
		return prefix+":"+name;
	}
	
	
	public static String addDatasetIdToEntity(String entity, String datasetId){
		
		return entity+DATASET_STAMP+datasetId;
	}

	public static String getDatasetIdOfEntity(String entity){
		
		int pos = entity.lastIndexOf(DATASET_STAMP)+DATASET_STAMP.length();
		String datasetId = entity.substring(pos);
		return datasetId;
	}

	public static String removeDatasetIdFromEntity(String entity) {
		// TODO Auto-generated method stub
		int pos = entity.lastIndexOf(DATASET_STAMP);
		
		return entity.substring(0,pos);
	}



	public static JavaPairRDD<String, List<String>> mapRecordsToEntities(JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> records,
			                                                             final String datasetId, 
			                                                             Broadcast<HashMap<String, String>> prefixIndex_B) 
	{
		
			
			final HashMap<String, String> prefixIndex = prefixIndex_B.getValue();
			final HashMap<String, String> invertedPrefixIndex = invertPrefixIndex(prefixIndex);
			
			
			PairFunction<Tuple2<String, Set<Tuple2<String, String>>>, String, List<String>> addDatasetId = 
				
				
				new PairFunction<Tuple2<String, Set<Tuple2<String, String>>>, String, List<String>>(){
				        
						
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, List<String>> call(
								Tuple2<String, Set<Tuple2<String, String>>> triple)
								throws Exception {
							
							String subject = triple._1;
							subject = DatasetManager.addDatasetIdToEntity(subject, datasetId);
							
							ArrayList<String> poPairs = new ArrayList<String>();
							
							String predicate;
							String object;
							for(Tuple2<String, String> po : triple._2){
								predicate = po._1;
								object = po._2;
								predicate = shrinkURI(predicate,invertedPrefixIndex);
								
								if (predicate == null) continue;
								
								if(predicate.equals("rdf:type")){
						  			object = shrinkURI(object,invertedPrefixIndex);
						  		}
								if(object == null) continue;
								//logger.info(predicate + " "+object);
								poPairs.add(predicate);
								poPairs.add(object);
							}
							//System.out.println("po list size = "+poPairs.size());
							return new Tuple2<String, List<String>>(subject,poPairs);
						}
				
			};
		return  records.mapToPair(addDatasetId);
	}

	public static String getURIOfPredicate(String property,
			HashMap<String, String> prefixIndex) {
		int prefixDelimiterIndex = property.indexOf(":");
		String prefix = property.substring(0,prefixDelimiterIndex);
		String propertyName = property.substring(prefixDelimiterIndex+1);
		String baseURI = prefixIndex.get(prefix);
		String propertyURI = baseURI+propertyName;
		return propertyURI;
	}
}
