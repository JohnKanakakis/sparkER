package spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

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
	private static DatasetManager dm = null;
	
	public static DatasetManager get() {
		// TODO Auto-generated method stub
		if(dm == null){
			dm = new DatasetManager();
		}
		return dm;
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



	public static JavaPairRDD<String, List<String>> addDatasetId(
			JavaRDD<Tuple2<String, Set<Tuple2<String, String>>>> records,
			final String datasetId) {
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
							
							for(Tuple2<String, String> po : triple._2){
								poPairs.add(po._1);
								poPairs.add(po._2);
							}
							//System.out.println("po list size = "+poPairs.size());
							return new Tuple2<String, List<String>>(subject,poPairs);
						}
				
			};
		return  records.mapToPair(addDatasetId);
	}
}
